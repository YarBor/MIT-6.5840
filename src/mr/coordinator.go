package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type filename string
type Coordinator struct {
	// Your definitions here.
	Tasks map[filename]*struct {
		mode       int32
		C          chan struct{}
		WorkerName string
		lock       sync.Locker
	}
	Taskpipe    chan filename
	doneTaskNum int32

	reduce_size int32
	Nreduce     chan int
	ReduceTasks map[int]*struct {
		mode       int32
		C          chan struct{}
		WorkerName string
		lock       sync.Locker
	}
	DoneReduceNum int32
}

// type worker struct {
// 	name       string
// 	doing_task string
// }

func (c *Coordinator) GetMapTask(a *Args, r *Reply) error {
	select {
	case filename, ok := <-c.Taskpipe:
		if !ok { // close
			if atomic.LoadInt32(&c.doneTaskNum) == int32(len(c.Tasks)) {
				r.Need_wait = false
			} else {
				r.Need_wait = true
			}
		} else {
			r.TaskName = string(filename)
			r.Nreduce = int(c.reduce_size)
			// atomic.StoreInt32(&c.Tasks[filename].mode, 1)
			// atomic.StorePointer(&unsafe.Pointer(c.Tasks[filename].WorkerName),unsafe.Pointer(&a.SelfName))
			// c.Tasks[filename].WorkerName.Store(unsafe.Pointer(&a.SelfName))
			c.Tasks[filename].lock.Lock()
			defer c.Tasks[filename].lock.Unlock()
			c.Tasks[filename].mode = 1
			c.Tasks[filename].WorkerName = a.SelfName
			for { // 清空
				select {
				case <-c.Tasks[filename].C:
				default:
					goto done
				}
			}
		done:
			go func() {
				select {
				case <-time.NewTimer(time.Second * 10).C:
					c.Tasks[filename].lock.Lock()
					defer c.Tasks[filename].lock.Unlock()
					if c.Tasks[filename].mode == 1 && c.Tasks[filename].WorkerName == a.SelfName {
						c.Tasks[filename].mode = 0
						c.Tasks[filename].WorkerName = ""
						c.Taskpipe <- filename
					}
				case <-c.Tasks[filename].C:
				}
			}()
		}
	default:
		r.Need_wait = true
	}
	return nil
}
func (c *Coordinator) DoMapTaskErr(a *Args, r *Reply) error {
	c.Tasks[filename(a.Filename)].lock.Lock()
	defer c.Tasks[filename(a.Filename)].lock.Unlock()
	if !(c.Tasks[filename(a.Filename)].mode == 1 && c.Tasks[filename(a.Filename)].WorkerName == a.SelfName) {
		log.Println(a.Data)
		return nil
	}
	log.Println(a.Data)

	c.Tasks[filename(a.Filename)].C <- struct{}{}
	c.Tasks[filename(a.Filename)].mode = 0
	c.Tasks[filename(a.Filename)].WorkerName = ""
	c.Taskpipe <- filename(a.Filename)
	return nil
}
func (c *Coordinator) MapTaskDone(a *Args, r *Reply) error {
	c.Tasks[filename(a.Filename)].lock.Lock()
	defer c.Tasks[filename(a.Filename)].lock.Unlock()
	if !(c.Tasks[filename(a.Filename)].mode == 1 && c.Tasks[filename(a.Filename)].WorkerName == a.SelfName) {
		return nil
	}
	c.Tasks[filename(a.Filename)].mode = 2
	select {
	case c.Tasks[filename(a.Filename)].C <- struct{}{}:
	default:
		log.Println("c.Tasks[filename(a.Filename)].C <- struct{}{} FAILED  in mapTaskDone")
	}
	fmt.Printf("Close %s channel %s done\n", filename(a.Filename), filename(a.SelfName))
	close(c.Tasks[filename(a.Filename)].C)
	if atomic.AddInt32(&c.doneTaskNum, 1) == int32(len(c.Tasks)) {
		fmt.Printf("Close %s channel %s done\n", "MapTask", "MapTask")
		close(c.Taskpipe)
	}
	return nil
}
func (c *Coordinator) ReduceTaskGet(a *Args, r *Reply) error {
	select {
	case TaskNum, ok := <-c.Nreduce:
		if !ok { // close
			if atomic.LoadInt32(&c.DoneReduceNum) == c.reduce_size {
				r.Need_wait = false
			} else {
				r.Need_wait = true
			}
			r.Nreduce = -1
		} else {
			r.Nreduce = TaskNum
			for p, _ := range c.Tasks {
				r.Filenames = append(r.Filenames, string(p))
			}
			c.ReduceTasks[TaskNum].lock.Lock()
			defer c.ReduceTasks[TaskNum].lock.Unlock()
			c.ReduceTasks[TaskNum].mode = 1
			c.ReduceTasks[TaskNum].WorkerName = a.SelfName

			for { // 清空请求
				select {
				case <-c.ReduceTasks[TaskNum].C:
				default:
					goto done
				}
			}
		done:
			go func() {
				select {
				case <-time.NewTimer(time.Second * 10).C:
					c.ReduceTasks[TaskNum].lock.Lock()
					defer c.ReduceTasks[TaskNum].lock.Unlock()
					if c.ReduceTasks[TaskNum].mode == 1 && c.ReduceTasks[TaskNum].WorkerName == a.SelfName {
						c.ReduceTasks[TaskNum].mode = 0
						c.ReduceTasks[TaskNum].WorkerName = ""
						c.Nreduce <- TaskNum
					}
				case <-c.ReduceTasks[TaskNum].C:
				}
			}()
		}
	default:
		r.Nreduce = -1
		r.Need_wait = true
	}
	return nil
}
func (c *Coordinator) DoReduceTaskErr(a *Args, r *Reply) error {
	c.ReduceTasks[a.Nreduce].lock.Lock()
	defer c.ReduceTasks[a.Nreduce].lock.Unlock()
	if !(c.ReduceTasks[a.Nreduce].WorkerName == a.SelfName && c.ReduceTasks[a.Nreduce].mode == 1) {
		log.Println(a.Data)
		return nil
	}
	log.Println(a.Data)

	c.ReduceTasks[a.Nreduce].C <- struct{}{}
	c.ReduceTasks[a.Nreduce].mode = 0
	c.ReduceTasks[a.Nreduce].WorkerName = ""
	c.Taskpipe <- filename(a.Filename)
	return nil
}
func (c *Coordinator) ReduceTasksDone(a *Args, r *Reply) error {
	c.ReduceTasks[a.Nreduce].lock.Lock()
	defer c.ReduceTasks[a.Nreduce].lock.Unlock()
	if !(c.ReduceTasks[a.Nreduce].WorkerName == a.SelfName && c.ReduceTasks[a.Nreduce].mode == 1) {
		return nil
	}
	c.ReduceTasks[a.Nreduce].mode = 2
	c.ReduceTasks[a.Nreduce].C <- struct{}{}
	if atomic.AddInt32(&c.DoneReduceNum, 1) == c.reduce_size {

		fmt.Printf("Close Redece:%d channel [Redece:%d] done\n", a.Nreduce, a.Nreduce)
		close(c.Nreduce)
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.Taskpipe = make(chan filename, len(files))
	c.Tasks = make(map[filename]*struct {
		mode       int32
		C          chan struct{}
		WorkerName string
		lock       sync.Locker
	})
	for _, f := range files {
		c.Tasks[filename(f)] = &struct {
			mode       int32
			C          chan struct{}
			WorkerName string
			lock       sync.Locker
		}{mode: 0, C: make(chan struct{}, 1), lock: &sync.Mutex{}}
		c.Taskpipe <- filename(f)
	}
	c.Nreduce = make(chan int, nReduce)
	c.doneTaskNum = 0
	c.ReduceTasks = make(map[int]*struct {
		mode       int32
		C          chan struct{}
		WorkerName string
		lock       sync.Locker
	})
	for i := 0; i < nReduce; i++ {
		c.Nreduce <- i
		c.ReduceTasks[i] = &struct {
			mode       int32
			C          chan struct{}
			WorkerName string
			lock       sync.Locker
		}{mode: 0, C: make(chan struct{}, 1), lock: &sync.Mutex{}}
	}
	c.reduce_size = int32(nReduce)
	c.server()
	return &c
}
