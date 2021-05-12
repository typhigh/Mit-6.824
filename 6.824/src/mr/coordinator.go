package mr

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
const (
	JobTimeoutDuration time.Duration = 10 * time.Second   // 10s
)
type Job struct {
	jobType		JobType
	step		JobStep
	fileName	string
	startTime	time.Time
}

func (j Job) IsTimeout() bool {
	return j.step != Done && time.Now().Sub(j.startTime) > JobTimeoutDuration
}

type Coordinator struct {
	// Your definitions here.
	lock 		sync.Mutex
	jobs   		[]Job 		// jobs for map/reduce
	nMap		int			// map count
	nReduce		int			// reduce count
	nMapDone	int32		// note : just inc
	nReduceDone	int32		// note : just inc
}

// Your code here -- RPC handlers for the worker to call.
type JobType int
const (
	Unknown JobType = iota
	Map
	Reduce
)

type JobStep int
const (
	NotStarted JobStep = iota
	Running
	Done
)

func (jobStep JobStep) String() string {
	return jobStepNames[jobStep]
}

var jobStepNames = []string {
	"NotStarted",
	"Running",
	"Done",
}

// ProduceJob produce job for worker, maybe map or reduce
func (coordinator *Coordinator) ProduceJob(args *ProduceJobArgs, reply *ProduceJobReplay) error {
	coordinator.lock.Lock()
	defer coordinator.lock.Unlock()

	// mark job done
	if args.JobIdDone != -1 && args.JobIdDone < len(coordinator.jobs) {
		coordinator.updateJobStep(args.JobIdDone, Done)
	}

	// schedule (produce) new job for this worker
	jobType := Map
	if atomic.LoadInt32(&coordinator.nMapDone) == int32(coordinator.nMap) {
		jobType = Reduce
	}
	jobId := coordinator.selectNotStartedJob(jobType)
	reply.JobId = jobId
	reply.Type = jobType
	reply.NReduce = coordinator.nReduce
	reply.NMap = coordinator.nMap
	if jobId != -1 {
		reply.FileName = coordinator.jobs[jobId].fileName
		coordinator.updateJobStep(jobId, Running)
		coordinator.jobs[jobId].startTime = time.Now()
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (coordinator *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (coordinator *Coordinator) server() {
	rpc.Register(coordinator)
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (coordinator *Coordinator) Done() bool {
	return atomic.LoadInt32(&coordinator.nReduceDone) == int32(coordinator.nReduce)
}

// need called with lock
func (coordinator *Coordinator) updateJobStep(jobId int, step JobStep) {
	job := coordinator.jobs[jobId]
	oldStep := job.step
	if oldStep == step {
		return
	}

	log.Printf("update job[%d] from old Step[%s] to new Step[%s]",
		jobId, oldStep.String(), step.String())
	coordinator.jobs[jobId].step = step
	// update done counter
	if step == Done {
		if job.jobType == Map {
			atomic.AddInt32(&coordinator.nMapDone, 1)
		} else {
			atomic.AddInt32(&coordinator.nReduceDone, 1)
		}
	}
}

func (coordinator *Coordinator) selectNotStartedJob(jobType JobType) int {
	// |--------------|---------------|
	//      nMap            nReduce
	begin := 0
	end := coordinator.nMap
	if jobType == Reduce {
		begin = coordinator.nMap
		end = coordinator.nMap + coordinator.nReduce
	}
	for i := begin; i < end; i++ {
		// not started or timeout
		if coordinator.jobs[i].step == NotStarted ||
			coordinator.jobs[i].IsTimeout() {
			return i
		}
	}
	return -1
}



//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	var jobs []Job
	nMap := len(files)
	for i := 0; i < nMap; i++ {
		jobs = append(jobs, Job{Map, NotStarted, files[i], time.Time{}})
	}
	for i := 0; i < nReduce; i++ {
		jobs = append(jobs, Job{Reduce, NotStarted, "mr-out-" + strconv.Itoa(i), time.Time{}})
	}
	coordinator := Coordinator {
		sync.Mutex{},
		jobs,
		nMap,
		nReduce,
		0,
		0,
	}
	coordinator.server()
	return &coordinator
}
