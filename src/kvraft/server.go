package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func (kv *KVServer) Dolog(i interface{}) {
	log.Printf("KVServer [%d]     %+v", kv.me, i)
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
	RequestMap     map[PutAppendArgs]chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "Server killed"
		return
	}
	kv.DataRWLock.RLocker()
	defer kv.DataRWLock.RUnlock()
	V, ok := kv.Data[args.Key]
	if ok {
		reply.Value = V
	} else {
		reply.Err = "K/V not found"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "Server killed"
		return
	}
	_, _, ok := kv.rf.Start(args)
	var IsLoadChan chan bool
	if !ok {
		reply.Err = "This server is not leader"
		return
	} else {
		IsLoadChan = make(chan bool, 1)
		kv.RequestMapLock.Lock()
		kv.RequestMap[*args] = IsLoadChan
		kv.RequestMapLock.Unlock()
	}
	select {
	case <-time.NewTimer(100 * time.Millisecond).C:
		kv.RequestMapLock.Lock()
		delete(kv.RequestMap, *args)
		close(IsLoadChan)
		kv.RequestMapLock.Unlock()
		reply.Err = "Timeout"
	case <-IsLoadChan:
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.RequestMap = make(map[PutAppendArgs]chan bool)
	kv.Data = make(map[string]string)
	go kv.loadData()
	return kv
}

func (kv *KVServer) loadData() {
	for {
		if kv.killed() {
			return
		}
		submitCommand, ok := <-kv.applyCh
		if ok {
			break
		}
		Command, ok := submitCommand.Command.(PutAppendArgs)
		if !ok {
			panic("GetCommandFalse")
		} else {
			kv.DataRWLock.Lock()
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
		}

		kv.RequestMapLock.Lock()
		IsloadChan, ok := kv.RequestMap[Command]
		if !ok {
		} else {
			delete(kv.RequestMap, Command)
			IsloadChan <- true
			close(IsloadChan)
		}
		kv.RequestMapLock.Unlock()
	}
}
