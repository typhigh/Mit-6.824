package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	WaitProduceJobInMicrosecond = 20
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	lastDoneJobId := -1
	for {
		args := ProduceJobArgs{lastDoneJobId}
		reply := ProduceJobReplay{}
		if !call("Coordinator.ProduceJob", &args, &reply) {
			// coordinator go wrong, just stop
			log.Fatal("worker can't call coordinator, coordinator may be fatal, exit")
			os.Exit(-1)
		}
		if reply.JobId == -1 {
			// sleep 10 ms
			time.Sleep(time.Microsecond * WaitProduceJobInMicrosecond)
			continue
		}
		// consume produced job
		ConsumeJob(reply.Type, reply.FileName, reply.JobId, reply.NMap, reply.NReduce, mapf, reducef)
		lastDoneJobId = reply.JobId
	}

}

func ConsumeJob(
	jobType JobType,
	fileName string,
	jobId int,
	nMap int,
	nReduce int,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if jobType == Map {
		ConsumeMapJob(fileName, mapf, nReduce, jobId)
	} else {
		ConsumeReduceJob(fileName, reducef, nMap, jobId - nMap)
	}
}

func ConsumeReduceJob(fileName string, reducef func(string, []string) string, nMap int, reduceId int) {
	var intermediate []KeyValue
	for mapId := 0; mapId < nMap; mapId++ {
		fileNme := "mr-" + strconv.Itoa(mapId)+ "-" + strconv.Itoa(reduceId)
		file, err := os.Open(fileNme)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		decode := json.NewDecoder(file)
		var kva []KeyValue
		decode.Decode(&kva)
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	// ofile, _ := os.Create(fileName)
	ofile, _:= ioutil.TempFile("", "mr-tmp-" + strconv.Itoa(reduceId))
	for i := 0; i < len(intermediate) ; {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), fileName)
	ofile.Close()
}

func ConsumeMapJob(fileName string, mapf func(string, string) []KeyValue, nReduce int, mapId int) {
	// read content from input file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	// write intermediate to some output files
	kva := mapf(fileName, string(content))
	intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		intermediate[reduceId] = append(intermediate[reduceId], kv)
	}
	outFiles := make([]*os.File, nReduce)
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		outName := "mr-tmp" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId)
		outFiles[reduceId], _ = ioutil.TempFile("", outName)
		// outFile, _ := os.Create(outName)
		encode := json.NewEncoder(outFiles[reduceId])
		encode.Encode(intermediate[reduceId])
	}
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		os.Rename(outFiles[reduceId].Name(), "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId))
		outFiles[reduceId].Close()
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
