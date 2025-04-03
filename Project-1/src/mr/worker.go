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
	"time"
)

// KeyValue structure used by map/reduce.
type KeyValue struct {
	Key   string
	Value string
}

// Sorting helper for reduce phase.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Hash function to assign keys to reducers.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Main entry point for the worker.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Println("Worker started")

	for {
		task, err := requestTask()
		if err != nil {
			fmt.Println("Error requesting task:", err)
			continue
		}

		switch task.Type {
		case Map:
			processMap(task, mapf)
			reportDone(Map, task.TaskID)

		case Reduce:
			processReduce(task, reducef)
			reportDone(Reduce, task.TaskID)

		case Wait:
			time.Sleep(time.Second)

		case Exit:
			return
		}
	}
}

// Send RPC to ask for a task.
func requestTask() (*TaskResponse, error) {
	args := TaskRequest{}
	reply := TaskResponse{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		return nil, fmt.Errorf("rpc call failed")
	}
	return &reply, nil
}

// Tell the coordinator a task is done.
func reportDone(taskType int, taskID int) {
	args := TaskReport{
		TaskType: taskType,
		TaskID:   taskID,
	}
	call("Coordinator.ReportTaskDone", &args, &struct{}{})
}

// Process a map task.
func processMap(task *TaskResponse, mapf func(string, string) []KeyValue) {
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
	tempFiles := make([]string, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-", task.TaskID, i))
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		intermediateFiles[i] = tmpFile
		tempFiles[i] = tmpFile.Name()
		encoders[i] = json.NewEncoder(tmpFile)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		err := encoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("error encoding JSON: %v", err)
		}
	}

	for i, f := range intermediateFiles {
		f.Close()
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		err := os.Rename(tempFiles[i], finalName)
		if err != nil {
			log.Fatalf("cannot rename temp file %v to %v: %v", tempFiles[i], finalName, err)
		}
	}
}

// Process a reduce task.
func processReduce(task *TaskResponse, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-", task.TaskID))
	if err != nil {
		log.Fatalf("cannot create temporary file for %v", task.TaskID)
	}
	defer tempFile.Close()

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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)
	err = os.Rename(tempFile.Name(), finalName)
	if err != nil {
		log.Fatalf("cannot rename temp file %v to %v: %v", tempFile.Name(), finalName, err)
	}
}


// Sends an RPC request.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
