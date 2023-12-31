package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func (kv *KVServer) checkFuncDone(FuncName ...interface{}) func() {
	t := time.Now().UnixMilli()
	i := make(chan bool, 1)
	i2 := make(chan bool, 1)
	go func() {
		kv.Dolog("\t", t, FuncName, " GO")
		i2 <- true
		for {
			select {
			case <-time.After(1 * time.Second):
				kv.Dolog("\t", t, "!!!!\t", FuncName, " MayLocked")
			case <-i:
				return
			}
		}
	}()
	<-i2
	close(i2)
	return func() {
		t2 := time.Now().UnixMilli()
		i <- true
		kv.Dolog("\t", t, FuncName, " return", t2-t, "ms")
	}
}
func (kv *KVServer) Dolog(i ...interface{}) {
	log.Printf("KVServer [%d]     %+v", kv.me, i)
	kv.DebugLoger.Printf("KVServer [%d]     %+v", kv.me, i)
	kv.rf.DebugLoger.Printf("KVServer [%d]     %+v", kv.me, i)
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type node struct {
	ArgsId int64
	SelfID int64
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataRWLock     sync.RWMutex
	Data           map[string]string
	RequestMapLock sync.Mutex
	RequestMap     map[node]chan bool

	DebugLoger *log.Logger
	CheckCache *cache

	snapshotIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "Server killed"
		return
	}
	if _, _, ok := kv.rf.Start(nil); ok && kv.rf.Ping() {
		defer kv.checkFuncDone("Get")()
		defer func(args *GetArgs, reply *GetReply) { kv.Dolog(*args, *reply) }(args, reply)
		commandMode := 0
		if commandMode = kv.check(args.SelfID, args.ArgsId); commandMode == commandNoDone {
			kv.Dolog("[Get] check return commandNoDone")
			reply.Err = ErrNeedWait
			return
		} else if commandMode == noRegiste && args.ArgsId != 0 {
			kv.Dolog("[Get] check return noRegiste")
			reply.Err = ErrNeedWait
			return
		}
		kv.DataRWLock.RLock()
		V, exist := kv.Data[args.Key]
		kv.DataRWLock.RUnlock()
		if exist {
			reply.Value = V
			kv.Dolog(*reply)
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "Server killed"
		return
	}

	if commandMode := kv.check(args.SelfID, args.ArgsId); commandMode == commandIsDone {
		kv.Dolog("get req PutAppend", *args, "But already loaded return ")
		return
	}

	var IsLoadChan chan bool
	n := node{ArgsId: args.ArgsId, SelfID: args.SelfID}

	if _, _, ok := kv.rf.Start(*args); ok || kv.rf.Ping() {
		defer kv.checkFuncDone("PutAppend  ")()
		defer func(args *PutAppendArgs, reply *PutAppendReply) { kv.Dolog(*args, *reply) }(args, reply)

		IsLoadChan = make(chan bool, 1)
		kv.RequestMapLock.Lock()
		kv.RequestMap[n] = IsLoadChan
		kv.RequestMapLock.Unlock()

	} else {
		kv.Dolog("Get PutAppend Request", *args, "No Leader")
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-time.NewTimer(50 * time.Millisecond).C:
		kv.RequestMapLock.Lock()
		IsLoadChan, ok := kv.RequestMap[n]
		if ok {
			delete(kv.RequestMap, n)
			close(IsLoadChan)
			reply.Err = ErrTimeout
			kv.Dolog("Timeout", *args)
		}
		kv.RequestMapLock.Unlock()
	case <-IsLoadChan:
		reply.Err = ""
		kv.Dolog("success", *args)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.Dolog(" DoKILL ")
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// for i := range kv.CheckCache.sets {
	// kv.CheckCache.sets[i].kill <- struct{}{}
	// }
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.RequestMap = make(map[node]chan bool)
	kv.Data = make(map[string]string)

	file2, err := os.OpenFile(fmt.Sprintf("/home/wang/raftLog/kvServer_%d_%d.R", os.Getpid(), kv.me), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("err.Error(): %v\n", err.Error()))
		log.Fatal(err)
	}
	kv.DebugLoger = log.New(file2, "", log.LstdFlags)
	kv.DebugLoger.SetFlags(log.Lmicroseconds)
	kv.Dolog("\n\n\n\n\nKvServer started")
	kv.CheckCache = makeCache()
	kv.snapshotDecode(kv.rf.GetSnapshot())
	go kv.loadData()
	return kv
}

func (kv *KVServer) loadData() {
	var submitCommand raft.ApplyMsg
	var ok bool
	for {
		if kv.killed() {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
			continue
		case submitCommand, ok = <-kv.applyCh:
		}
		if !ok {
			break
		}
		checkdone := kv.checkFuncDone("LoadData")
		if submitCommand.CommandValid {
			Command, ok := submitCommand.Command.(PutAppendArgs)
			if !ok {
				kv.Dolog(submitCommand, "Command Not PutAppendArgs{}")
				fmt.Printf("reflect.TypeOf(submitCommand.Command): %v\n", reflect.TypeOf(submitCommand.Command))
				panic("GetCommandFalse")
			} else {
				if commandMode := kv.check(Command.SelfID, Command.ArgsId); commandMode == commandIsDone {
					kv.Dolog("get command ", Command, " Has Done Before")
					checkdone()
					continue
				} else {
					kv.DataRWLock.Lock()
					kv.Dolog("to do command ", Command)
					switch Command.Op {
					case "Put":
						kv.Data[Command.Key] = Command.Value
					case "Append":
						if value, ok := kv.Data[Command.Key]; ok {
							kv.Data[Command.Key] = value + Command.Value
						} else {
							kv.Data[Command.Key] = Command.Value
						}
					}
					kv.DataRWLock.Unlock()
					kv.registe(&Command)
					kv.Dolog("Done command ", Command)
				}
			}
			kv.RequestMapLock.Lock()
			n := node{ArgsId: Command.ArgsId, SelfID: Command.SelfID}
			IsloadChan, ok := kv.RequestMap[n]
			if !ok {
			} else {
				delete(kv.RequestMap, n)
				IsloadChan <- true
				close(IsloadChan)
			}
			kv.RequestMapLock.Unlock()
			checkdone()
			if size := int(kv.rf.RaftSize()); kv.maxraftstate != -1 && size >= kv.maxraftstate/2 {
				kv.Dolog("do SnapShot", submitCommand, "size:", size)
				buffer := bytes.Buffer{}
				encoder := labgob.NewEncoder(&buffer)
				encoder.Encode(submitCommand.CommandIndex)
				kv.snapshotIndex = submitCommand.CommandIndex

				kv.DataRWLock.RLock()
				encoder.Encode(kv.Data)
				kv.DataRWLock.RUnlock()

				kv.CheckCache.rwlock.Lock()
				encoder.Encode(kv.CheckCache.data)
				kv.CheckCache.rwlock.Unlock()

				data := buffer.Bytes()

				kv.rf.Snapshot(submitCommand.CommandIndex, data)
			}
		} else if submitCommand.SnapshotValid {
			kv.snapshotDecode(&submitCommand)
			checkdone()
		} else {
			panic("wrong command")
		}
	}
}
func (kv *KVServer) snapshotDecode(submitCommand *raft.ApplyMsg) {
	if !submitCommand.SnapshotValid || submitCommand.Snapshot == nil {
		return
	}
	kv.Dolog("Save SnapShot", *submitCommand)
	buffer := bytes.NewBuffer(submitCommand.Snapshot)
	decoder := labgob.NewDecoder(buffer)
	Index := -1
	if err := decoder.Decode(&Index); err != nil {
		panic(err)
	} else {
		if Index <= kv.snapshotIndex {
			return
		}
		kv.snapshotIndex = Index
	}
	// data
	var newdata map[string]string
	if err := decoder.Decode(&newdata); err != nil {
		panic(err)
	}

	kv.DataRWLock.Lock()
	kv.Data = newdata
	kv.DataRWLock.Unlock()
	kv.Dolog(newdata)
	// check data
	var newCheckMap map[int64]*struct{ CommandId int64 }
	if err := decoder.Decode(&newCheckMap); err != nil {
		panic(err)
	}
	kv.CheckCache.rwlock.Lock()
	kv.CheckCache.data = newCheckMap
	kv.CheckCache.rwlock.Unlock()
}

/*
前提:
server集群 通过 Raft 进行同步/服务复制
提供一个简易的kv服务
Client 进行并发的rpc调用

问题
client 向 Server集群 发送一条
`append "wang" where key = "1"`

	因为种种原因

Servers 做出了预期的行为 但未及时进行回复 client
导致 client端超时 进行命令重发
Server 再一次 拿到 该命令
如何进行优雅的命令去重

目前的想法是生成命令的唯一标识符
在server端进行 命令同步/执行过程中 比对唯一标识 从而去重

	窗口设置?

但  资源回收?

	锁争用

	导致的时间开销?
*/
// type Set struct {
// 	cache map[struct {
// 		CommandID int64
// 		TimeStamp int64
// 	}]struct{}
// 	Lock sync.RWMutex
// }

//	func (s *Set) Check(args *PutAppendArgs) bool {
//		s.Lock.RLock()
//		defer s.Lock.RUnlock()
//		_, ok := s.cache[struct {
//			CommandID int64
//			TimeStamp int64
//		}{CommandID: args.ArgsId, TimeStamp: args.Time}]
//		return ok
//	}
//
//	func (s *Set) Registe(args *PutAppendArgs) {
//		s.Lock.Lock()
//		defer s.Lock.Unlock()
//		s.cache[struct {
//			CommandID int64
//			TimeStamp int64
//		}{CommandID: args.ArgsId, TimeStamp: args.Time}] = struct{}{}
//	}

var LogDDL = 20 * time.Second

type cache struct {
	data   map[int64]*struct{ CommandId int64 }
	rwlock sync.RWMutex
}

func makeCache() *cache {
	C := &cache{data: make(map[int64]*struct{ CommandId int64 })}
	return C
}

// IsDone
var commandIsDone = 1
var commandNoDone = 2
var noRegiste = 3

func (kv *KVServer) check(SelfId int64, ArgsId int64) int {
	kv.CheckCache.rwlock.RLock()
	i, ok := kv.CheckCache.data[SelfId]
	kv.CheckCache.rwlock.RUnlock()
	if ok {
		Id := atomic.LoadInt64(&i.CommandId)
		kv.Dolog("[", SelfId, "] check ", ArgsId, " history", Id)
		if Id >= ArgsId {
			return commandIsDone
		} else {
			return commandNoDone
		}
	}
	kv.Dolog("check ", ArgsId, " history", nil)
	return noRegiste
}
func (kv *KVServer) registe(input *PutAppendArgs) {
	kv.CheckCache.rwlock.RLock()
	i, ok := kv.CheckCache.data[input.SelfID]
	kv.CheckCache.rwlock.RUnlock()
	if !ok {
		kv.CheckCache.rwlock.Lock()
		kv.CheckCache.data[input.SelfID] = &struct{ CommandId int64 }{CommandId: input.ArgsId}
		kv.CheckCache.rwlock.Unlock()
		kv.Dolog("registe ", input.SelfID, "--", input.ArgsId)
		return
	} else {
		// atomic.CompareAndSwapInt64(&i.CommandId,atomic.LoadInt64(&i.CommandId),input.ArgsId)
		atomic.StoreInt64(&i.CommandId, input.ArgsId)
	}
}
