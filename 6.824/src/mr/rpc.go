package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ProduceJobArgs struct {
	JobIdDone	int			// if not -1, mark this job done
}

type ProduceJobReplay struct {
	JobId			int			// job identifier, if equal -1 then means need wait
	Type 			JobType		// map or reduce
	FileName 		string		// mapper's input file name or reducer's output file name
	NMap			int
	NReduce			int
	StartTimestamp	time.Time	// start timestamp, used for create intermediate file, avoid file name conflict
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
