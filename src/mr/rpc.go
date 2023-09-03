package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Args struct {
	SelfName string 
	Filename string
	Data string
	Nreduce int `json:"Nreduce"`
}
type Reply struct {
	TaskName string `json:"TaskName"`
	// TaskData string `json:"TaskData"`
	Need_wait bool `json:"NeedWait"`
	Nreduce int `json:"Nreduce"`
	Filenames []string `json:"Filenames"`
}
func (r *Args) Clear()  {
	r.Filename = ""
	r.Data = ""
	r.Nreduce = 0
}
func (r *Reply) Clear()  {
	r.TaskName = ""
	r.Need_wait = false
	r.Nreduce = 0
	r.Filenames = nil
}

type ExampleArgs struct {
	X int 
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
