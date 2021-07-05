package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type AcquireTaskArgs struct {
	Wid int
}

type MapTask struct {
	Filepath      string
	MapTaskNumber int
	NReducers     int
}

type ReduceTask struct {
	IntermediateFiles []string
	ReduceNumber int
}

type AcquireTaskReply struct {
	MapTask *MapTask
	ReduceTask *ReduceTask
	Done bool
}

type MapTaskDoneArgs struct {
	IntermediateFiles []string
	InputFile         string
	Wid               int
}

type MapTaskDoneReply struct {

}

type ReduceTaskDoneArgs struct {
	ReduceNumber int
	Wid          int
}

type ReduceTaskDoneReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
