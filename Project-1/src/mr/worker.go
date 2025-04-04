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
			log.Println("Error requesting task:", err)
			time.Sleep(time.Second)
			continue
		}

		switch task.Type {
		case Map:
			err := processMap(task, mapf)
			if err != nil {
				log.Println("Error in Map Task:", err)
				continue
			}
			reportDone(Map, task.TaskID)

		case Reduce:
			err := processReduce(task, reducef)
			if err != nil {
				log.Println("Error in Reduce Task:", err)
				continue
			}
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
func processMap(task *TaskResponse, mapf func(string, string) []KeyValue) error {
	content, err := readFileContent(task.Filename)
	if err != nil {
		return fmt.Errorf("error Reading File: %w", err)
	}

	kva := mapf(task.Filename, content)
	intermediateFiles, err := writeToIntermediateFiles(kva, task.TaskID, task.NReduce)
	if err != nil {
		return fmt.Errorf("error Writing to Intermediate Files: %w", err)
	}

	err = finalizeIntermediateFiles(intermediateFiles, task.TaskID)
	if err != nil {
		return fmt.Errorf("error finalizing Intermediate Files: %w", err)
	}

	return nil
}

// Read a file content for Map Task.
func readFileContent(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", fmt.Errorf("cannot open %v: %w", fileName, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read %v: %w", fileName, err)
	}
	return string(content), nil
}

// Write map results to temporary intermediate files
func writeToIntermediateFiles(kva []KeyValue, taskID int, nReduce int) ([]*os.File, error) {
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-", taskID, i))
		if err != nil {
			return nil, fmt.Errorf("cannot create temp file: %w", err)
		}
		intermediateFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		err := encoders[r].Encode(&kv)
		if err != nil {
			return nil, fmt.Errorf("error encoding JSON: %w", err)
		}
	}

	return intermediateFiles, nil
}

// Rename Temporary Intermediate files to their original names.
func finalizeIntermediateFiles(intermediateFiles []*os.File, taskID int) error {
	for i, f := range intermediateFiles {
		f.Close()
		finalName := fmt.Sprintf("mr-%d-%d", taskID, i)
		err := os.Rename(f.Name(), finalName)
		if err != nil {
			return fmt.Errorf("cannot rename temp file %v to %v: %w", f.Name(), finalName, err)
		}
	}
	return nil
}

// Process a reduce task.
func processReduce(task *TaskResponse, reducef func(string, []string) string) error {
	intermediate, err := readIntermediateValues(task.TaskID, task.NMap)
	if err != nil {
		return fmt.Errorf("error reading Intermediate values: %w", err)
	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-", task.TaskID))
	if err != nil {
		return fmt.Errorf("cannot create temporary file for %v: %w", task.TaskID, err)
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
		return fmt.Errorf("cannot rename temp file %v to %v: %w", tempFile.Name(), finalName, err)
	}

	return nil
}

// Read Key Values from Intermediate files
func readIntermediateValues(taskID int, nMap int) ([]KeyValue, error) {
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskID)
		file, err := os.Open(filename)
		if err != nil {
			return intermediate, fmt.Errorf("cannot open file %v: %w", filename, err)
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
	return intermediate, nil
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
