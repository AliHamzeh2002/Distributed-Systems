package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task, err := requestTask()
		if err != nil {
			fmt.Println("Error requesting task:", err)
		}

		switch task.Type {
		case Map:
			processMap(task, mapf)
		case Reduce:
			processReduce(task, reducef)
		default:
			fmt.Println("Unknown task type:", task.Type)
			continue
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func requestTask() (*TaskResponse, error) {
	taskRequest := TaskRequest{}
	taskResponse := TaskResponse{}

	ok := call("Coordinator.AssignTask", &taskRequest, &taskResponse)
	if !ok {
		return nil, fmt.Errorf("rpc call failed")
	}

	return &taskResponse, nil
}

func processMap(
	task *TaskResponse,
	mapf func(string, string) []KeyValue,
) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	intermediateFiles := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create file %v", filename)
		}
		intermediateFiles[i] = f
		encoders[i] = json.NewEncoder(f)
	}

	for _, kv := range kva {
		reducePartition := ihash(kv.Key) % task.NReduce
		err := encoders[reducePartition].Encode(&kv)
		if err != nil {
			log.Fatalf("error encoding JSON: %v", err)
		}
	}

	for _, f := range intermediateFiles {
		f.Close()
	}
}

func processReduce(
	task *TaskResponse,
	reducef func(string, []string) string,
) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		inputFile := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("cannot open file %v", inputFile)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	filename := fmt.Sprintf("mr-out-%d", task.TaskID)
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create file %v", filename)
	}
	defer f.Close()

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
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	f.Close()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
