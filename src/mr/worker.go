package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func loadKv(filename string, kv []KeyValue, Nreduce int) error {
	i := make(map[int]*struct {
		F string
		M map[string]interface{}
	})

	for _, k := range kv {
		node := ihash(k.Key) / Nreduce
		data, ok := i[node]
		if !ok {
			fn := fmt.Sprintf("/tmp/rm-%d-%s", node, filename)
			os.Remove(fn)
			i[node] = &struct {
				F string
				M map[string]interface{}
			}{F: fn, M: make(map[string]interface{})}
			data = i[node]
		}
		data.M[k.Key] = k.Value
	}

	for _, v := range i {
		f, err := os.Create(v.F)
		if err != nil {
			return err // 返回错误
		}
		defer f.Close()
		err = json.NewEncoder(f).Encode(v.M)
		if err != nil {
			return err // 返回错误
		}
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := &Args{
		SelfName: strconv.Itoa(os.Getpid()) + strconv.Itoa(int(time.Now().Unix())),
	}
	for {
		// 读特定的文件进行map()拿到kv
		args.Filename = ""
		rpl := &Reply{}
		err := call("Coordinator.GetMapTask", args, rpl)
		if err {
			return
		} else {
			if rpl.TaskName == "" && !rpl.Need_wait {
				break
			} else {
				args.Filename = rpl.TaskName
				data, err := os.ReadFile(rpl.TaskName)
				if err != nil {
					args.data = err.Error()
					call("Coordinator.DoMapTaskErr", args, rpl)
					return
				} else {
					kv := mapf(args.Filename, string(data))
					err := loadKv(args.Filename, kv, rpl.Nreduce)
					if err != nil {
						args.data = err.Error()
						call("Coordinator.DoMapTaskErr", args, rpl)
						return
					}
					call("Coordinator.MapTaskDone", args, rpl)
				}
			}
		}
	}
	for {
		rpl := &Reply{}
		err := call("Coordinator.ReduceTaskGet", args, rpl)
		if err {
			log.Println("call Coordinator.ReduceTaskGet failed")
			return
		} else {
			if rpl.Need_wait && rpl.Nreduce == -1 {
				time.Sleep(time.Second)
				continue
			} else if rpl.Nreduce == -1 {
				return
			}
			Reduce_number := rpl.Nreduce
			filenames := rpl.Filenames
			args.Nreduce = rpl.Nreduce
			sum := make(map[string][]string)
			for _, i := range filenames {
				to_get_data(sum, i, Reduce_number)
			}
			err := load_reduce_data(sum, Reduce_number, reducef)
			if err != nil {
				log.Printf("load_reduce_data failed: %v", err)
				args.data = err.Error()
				ok := call("Coordinator.DoReduceTaskErr", args, rpl)
				if !ok {
					log.Println(`call("Coordinator.DoReduceTaskErr", args, rpl) false : `, err.Error())
				}
			} else {
				ok := call("Coordinator.ReduceTasksDone", args, rpl)
				if !ok {
					log.Println(`call("Coordinator.ReduceTasksDone", args, rpl) false : `, err.Error())
				}
			}
		}
	}

	// 上面做了整合

	// 调用reduce

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

func load_reduce_data(sum map[string][]string, reduce_num int, reducef func(string, []string) string) error {
	file, err := os.Open(fmt.Sprintf("mr-out-%d", reduce_num))
	if err != nil {
		log.Printf("mr-out-%d open failed: %v", reduce_num, err)
		return err
	}
	defer file.Close()
	kv := make([]KeyValue, 0)
	for k, v := range sum {
		kv = append(kv, KeyValue{Key: k, Value: reducef(k, v)})
	}
	sort.Sort(ByKey(kv))
	for _, pkv := range kv {
		_, err := file.WriteString(fmt.Sprintf("%v %v", pkv.Key, pkv.Value))
		if err != nil {
			log.Printf("mr-out-%d write failed: %v", reduce_num, err)
			return err
		}
	}
	return nil
}
func to_get_data(sum map[string][]string, i string, Reduce_number int) {
	file, err := os.Open(fmt.Sprintf("mr-%d-%s", Reduce_number, i))
	if err != nil {
		log.Printf("mr-%d-%s cant open\n", Reduce_number, i)
	}
	defer file.Close()

	cache := make(map[string]interface{})
	err = json.NewDecoder(file).Decode(&cache)
	if err != nil {
		log.Printf("json decode false : %v\n", err) // handle the error appropriately
	}
	for key, val := range cache {
		_, ok := sum[key]
		if !ok {
			sum[key] = make([]string, 0)
		}
		sum[key] = append(sum[key], val.(string))
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
