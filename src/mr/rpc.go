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

type ExampleArgs struct { // 声明参数
	X int
}

type ExampleReply struct { // 回复rpc
	Y int
}

// is allover

type AlloverArgs struct {
}

type AlloverReply struct {
	done bool
}

// view the number of maps

type NumMapArgs struct {
}

type NumMapReply struct {
	num int
}

// map task assignment

type MapTaskArgs struct {
	mes string
}

type MapTaskReply struct {
	id   int
	file string
}

// map task is done(T/F)

type MapTaskDoneArgs struct {
	mes  string
	file string
}

type MapTaskDoneReply struct {
	isDone bool
}

//
// TODO --reduce
//

type ReduceArgs struct {
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
