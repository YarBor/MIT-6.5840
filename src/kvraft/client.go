package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

var ID = int64(0)

func (c *Clerk) checkFuncDone(FuncName string) func() {
	t := time.Now().UnixNano()
	i := make(chan bool, 1)
	i2 := make(chan bool, 1)
	go func() {
		c.Logger.Print("\t", t, "\t", FuncName+" GO\n")
		i2 <- true
		for {
			select {
			case <-time.After(1 * time.Second):
				c.Logger.Print("\t", t, "\t", "!!!!\t", FuncName+" MayLocked\n")
			case <-i:
				close(i)
				return
			}
		}
	}()
	<-i2
	close(i2)
	return func() {
		i <- true
		c.Logger.Print("\t", t, "\t", FuncName+" return\n")
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID                int64
	usefulServerIndex int64
	Logger            *log.Logger
	CommandId         int64
	Mutex             sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = append(make([]*labrpc.ClientEnd, 0), servers...)
	// You'll have to add code here.
	ck.ID = atomic.AddInt64(&ID, 1)
	ck.CommandId = 0
	file2, err := os.OpenFile(fmt.Sprintf("/home/wang/raftLog/Client_%d.R", os.Getpid()), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("err.Error(): %v\n", err.Error()))
		log.Fatal(err)
	}
	ck.Logger = log.New(file2, "", log.LstdFlags)
	ck.Logger.SetFlags(log.Lmicroseconds)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	defer ck.checkFuncDone("Get")()
	args := GetArgs{Key: key, ArgsId: atomic.LoadInt64(&ck.CommandId), SelfID: ck.ID}
	Sindex := atomic.LoadInt64(&ck.usefulServerIndex)
	i := Sindex
	var rpl *GetReply
	for ; ; Sindex = (Sindex + 1) % int64(len(ck.servers)) {
		rpl = &GetReply{}
		ck.Logger.Printf("Client(%d)--S[%d] Call Get K[%s]", ck.ID, Sindex, key)
		if ok := ck.servers[Sindex].Call("KVServer.Get", &args, rpl); ok && rpl.Err == "" {
			break
		} else {
			if rpl.Err == ErrWrongLeader {
			} else if rpl.Err == ErrNeedWait {
				time.Sleep(25 * time.Millisecond)
				Sindex = Sindex + int64(len(ck.servers)-1) // for continue
			}
			ck.Logger.Printf("Client(%d)--S[%d] Call false %+v", ck.ID, Sindex, *rpl)
			// time.Sleep(25 * time.Millisecond)
		}
	}
	if i != Sindex {
		atomic.StoreInt64(&ck.usefulServerIndex, Sindex)
		ck.Logger.Printf("'Client(%d)s usefulServerIndex Been Rewritten to [%d]'\n", ck.ID, Sindex)
	}
	if len(rpl.Err) != 0 {
		ck.Logger.Printf("Client(%d)--S[%d] Get Key(%s) failed Because of error: %v\n", ck.ID, Sindex, key, rpl.Err)
		return ""
	} else {
		ck.Logger.Printf("Client(%d)--S[%d] Get Key(%s) - Value(%s)\n", ck.ID, Sindex, key, rpl.Value)
		return rpl.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.Mutex.Lock()
	defer ck.Mutex.Unlock()
	defer ck.checkFuncDone("PutAppend")()
	args := PutAppendArgs{Key: key, Value: value, Op: op, ArgsId: atomic.AddInt64(&ck.CommandId, 1), SelfID: ck.ID}
	Sindex := atomic.LoadInt64(&ck.usefulServerIndex)
	i := Sindex
	for ; ; Sindex = (Sindex + 1) % int64(len(ck.servers)) {
		rpl := PutAppendReply{}
		ck.Logger.Printf("Client(%d)--Server(%d) Call %s K[%s]", ck.ID, Sindex, op, key)
		if ok := ck.servers[Sindex].Call("KVServer.PutAppend", &args, &rpl); ok && len(rpl.Err) == 0 {
			ck.Logger.Printf("Client(%d)--Server[%d] %s-ed K(%s)-V(%s)\n", ck.ID, Sindex, op, key, value)
			break
		} else {
			if !ok {
				ck.Logger.Printf("Client(%d)--Server(%d) Call %s Call False ", ck.ID, Sindex, op)
			} else if rpl.Err == ErrWrongLeader{
			} else if rpl.Err == ErrNeedWait {
				ck.Logger.Printf("Client(%d)--Server[%d] %s[Err:%s] K(%s)-V(%s)\n", ck.ID, Sindex, op, rpl.Err, key, value)
				time.Sleep(25 * time.Millisecond)
				Sindex = Sindex + int64(len(ck.servers)-1) // for continue
				continue
			} else if rpl.Err == ErrTimeout {
				ck.Logger.Printf("Client(%d)--Server[%d] %s[Err:%s] K(%s)-V(%s)\n", ck.ID, Sindex, op, rpl.Err, key, value)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	if i != Sindex {
		atomic.StoreInt64(&ck.usefulServerIndex, Sindex)
		ck.Logger.Printf("'Client(%d)s usefulServerIndex Been Rewritten to [%d]'\n", ck.ID, Sindex)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
