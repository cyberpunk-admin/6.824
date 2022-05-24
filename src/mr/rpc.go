package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapArg struct {
	MapTask
	OK bool
}

type MapReply struct {
	MapTask
	OK bool
}

type ReduceReply struct {
	ReduceTask
	OK bool
}

type ReduceArg struct {
	ReduceTask
	OK bool
}

type MapTask struct {
	Filename string
	ID       int
	NReduce  int
}

type ReduceTask struct {
	ID      int
	NReduce int
	NMap    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
