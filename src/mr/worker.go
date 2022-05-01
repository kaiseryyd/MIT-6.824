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


//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//test

	for {
		task := getTask()
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}

func writeToLocalFile(mapNumber int, reduceNumber int, intermediate *[]KeyValue) string{
	ofname := "mr-" + strconv.Itoa(mapNumber) + "-" + strconv.Itoa(reduceNumber)
	ofile, _ := os.Create(ofname)
	defer func(ofile *os.File) {
		err := ofile.Close()
		if err != nil {

		}
	}(ofile)
	for _, kv := range *intermediate {
		enc := json.NewEncoder(ofile)
		err := enc.Encode(&kv)
		if err!= nil {
			log.Fatalf("can not read %v" , ofile)
		}
	}
	return ofname
}
func readFromLocalFile(filePaths []string) []KeyValue {
	var intermediate []KeyValue
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		defer file.Close()
		if err != nil {
			log.Fatalf("open file error")
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	return intermediate
}
func mapper(task *Task, mapf func(string, string)[]KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("fail to read file: " + task.Input, err)
	}
	intermediates := mapf(task.Input, string(content))

	buffer := make([][]KeyValue, task.NReducer)
	//根据key hash拆分成nReduce份
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	task.Intermediates = mapOutput
	TaskCompleted(task)
}
func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _:= os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("fail to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.AssignTask", task, &reply)
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
