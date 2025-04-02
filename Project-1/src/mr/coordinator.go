package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu          sync.Mutex
	mapCount    int // Counter to track how many map tasks have been assigned
	reduceCount int // Counter to track how many reduce tasks have been assigned
	nReduce     int // Total number of reduce tasks
	nMap        int // Total number of map tasks (files)
	files       []string
}

// AssignTask provides tasks based on the request sequence.
func (c *Coordinator) AssignTask(req *TaskRequest, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapCount < len(c.files) {
		// Assign Map tasks for the first two requests
		res.TaskID = c.mapCount
		res.Type = Map
		res.Filename = c.files[c.mapCount] // Assign file1.txt for task 0, file2.txt for task 1
		res.NReduce = c.nReduce
		res.NMap = c.nMap
		c.mapCount++
		return nil
	} else if c.reduceCount < c.nReduce {
		// Assign Reduce tasks for the next 10 requests
		res.TaskID = c.reduceCount
		res.Type = Reduce
		res.NReduce = c.nReduce
		res.NMap = c.nMap
		c.reduceCount++
		return nil
	}

	// No tasks available (i.e., completed all tasks)
	fmt.Println("All tasks completed")
	return nil
}

// Done checks if all tasks are completed.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapCount >= 2 && c.reduceCount >= c.nReduce
}

// Create and initialize a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		files:   files,
	}
	c.server()
	return &c
}

// Start an RPC server to listen for worker requests.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
