package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		reply := AcquireTask()

		if reply.Done {
			log.Printf("Got a Done signal from Master Exiting")
			break
		}

		if reply.MapTask != nil {
			performMapTask(reply.MapTask, mapf)
		}

		if reply.ReduceTask != nil {
			performReduceTask(reply.ReduceTask, reducef)
		}
	}
	// Your worker implementation here.

}

func performReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	intermediate := collectKV(task.IntermediateFiles)
	sort.Sort(ByKey(intermediate))
	ofileName := fmt.Sprintf("mr-out-%v", task.ReduceNumber)
	tempFile, err := ioutil.TempFile(".", ofileName)
	if err != nil {
		log.Fatalf("cannot open %v", ofileName)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = os.Rename(tempFile.Name(), ofileName)
	RespondToReduceTask(task.ReduceNumber)
}

func RespondToReduceTask(reduceNumber int) {
	args := ReduceTaskDoneArgs{
		ReduceNumber: reduceNumber,
		Wid:          os.Getpid(),
	}

	reply := ReduceTaskDoneReply{}
	call("Master.ReduceTaskDone", &args, &reply)
}

func collectKV(files []string) []KeyValue {
	var intermediate []KeyValue

	for _, f := range files {
		dat, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Printf("FileName %v\n", f)
			fmt.Println("Read error: ", f, err.Error())
		}
		var input []KeyValue
		err = json.Unmarshal(dat, &input)
		if err != nil {
			fmt.Println("Unmarshal error: ", err.Error())
		}

		intermediate = append(intermediate, input...)
	}
	return intermediate
}

func performMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	kva := applyMap(task, mapf)
	sort.Sort(ByKey(kva))

	partitions := make([][]KeyValue, task.NReducers)

	for _, val := range kva {
		partition := ihash(val.Key) % task.NReducers
		partitions[partition] = append(partitions[partition], val)
	}

	intermediateFiles := make([]string, task.NReducers)
	for i := 0; i < task.NReducers; i++ {
		interMediateFile := fmt.Sprintf("mr-%d-%d", task.MapTaskNumber, i)
		ofile, _ := os.Create(interMediateFile)
		b, err := json.Marshal(partitions[i])
		if err != nil {
			fmt.Println("Marshal error: ", err)
		}
		ofile.Write(b)
		ofile.Close()
		intermediateFiles[i] = interMediateFile
	}
	RespondToMapTask(intermediateFiles, task.Filepath)
}

func applyMap(task *MapTask, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(task.Filepath)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", task.Filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filepath)
	}
	return mapf(task.Filepath, string(content))
}

func RespondToMapTask(intermediateFiles []string, inputFile string) {
	args := MapTaskDoneArgs{
		IntermediateFiles: intermediateFiles,
		InputFile:         inputFile,
		Wid:               os.Getpid(),
	}

	reply := MapTaskDoneReply{}
	call("Master.MapTaskDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func AcquireTask() AcquireTaskReply {

	// declare an argument structure.
	args := AcquireTaskArgs{Wid: os.Getpid()}

	// declare a reply structure.
	reply := AcquireTaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.AcquireTask", &args, &reply)
	return reply

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
