package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID                int64
	usefulServerIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
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
	args := GetArgs{Key: key}
	rpl := GetReply{}
	for ok := true; ok; {
		index := nrand() % int64(len(ck.servers))
		ok = ck.servers[index].Call("KVServer.Get", &args, &rpl)
	}
	if len(rpl.Err) != 0 {
		log.Printf("Client Get Key(%s) failed Because of error: %v\n", key, rpl.Err)
		return ""
	} else {
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
	args := PutAppendArgs{Key: key, Value: value, Op: op, ArgsId: fmt.Sprintf("%d", nrand())}
	rpl := GetReply{}
	for ; ; ck.usefulServerIndex = (ck.usefulServerIndex + 1) % len(ck.servers) {
		ck.servers[ck.usefulServerIndex].Call("KVServer.PutAppend", &args, &rpl)
		if len(rpl.Err) == 0 {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
