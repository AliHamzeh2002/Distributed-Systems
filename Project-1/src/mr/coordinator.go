package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus struct {
	isDone    bool
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.

	mu sync.Mutex

	files    []string
	nMap     int
	nReduce  int
	phase    string

	mapTasks    map[int]*TaskStatus
	reduceTasks map[int]*TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(_ *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case "map":
		for id, task := range c.mapTasks {
			if !task.isDone && time.Since(task.startTime) > 10*time.Second {
				task.startTime = time.Now()
				reply.Type = Map
				reply.TaskID = id
				reply.Filename = c.files[id]
				reply.NReduce = c.nReduce
				return nil
			}
		}
		if c.allDone(c.mapTasks) {
			c.phase = "reduce"
		}
		reply.Type = Wait
		return nil

	case "reduce":
		for id, task := range c.reduceTasks {
			if !task.isDone && time.Since(task.startTime) > 10*time.Second {
				task.startTime = time.Now()
				reply.Type = Reduce
				reply.TaskID = id
				reply.NMap = c.nMap
				return nil
			}
		}
		if c.allDone(c.reduceTasks) {
			c.phase = "done"
		}
		reply.Type = Wait
		return nil

	default:
		reply.Type = Exit
		return nil
	}
}

func (c *Coordinator) ReportTaskDone(args *TaskReport, _ *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == "map" && args.TaskType == Map {
		if task, ok := c.mapTasks[args.TaskID]; ok {
			task.isDone = true
		}
	}
	if c.phase == "reduce" && args.TaskType == Reduce {
		if task, ok := c.reduceTasks[args.TaskID]; ok {
			task.isDone = true
		}
	}
	return nil
}

func (c *Coordinator) allDone(tasks map[int]*TaskStatus) bool {
	for _, t := range tasks {
		if !t.isDone {
			return false
		}
	}
	return true
}

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

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.phase == "done"

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = "map"
	c.mapTasks = make(map[int]*TaskStatus)
	c.reduceTasks = make(map[int]*TaskStatus)

	for i := range files {
		c.mapTasks[i] = &TaskStatus{}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskStatus{}
	}

	c.server()
	return &c
}
