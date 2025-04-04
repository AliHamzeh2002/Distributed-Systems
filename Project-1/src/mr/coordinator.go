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

type Phase = int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	mu sync.Mutex

	files   []string
	nMap    int
	nReduce int
	phase   Phase

	mapTasks    map[int]*TaskStatus
	reduceTasks map[int]*TaskStatus
}

func (c *Coordinator) AssignTask(_ *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		for id, task := range c.mapTasks {
			if !task.isDone && time.Since(task.startTime) > 10*time.Second {
				task.startTime = time.Now()
				reply.Type = Map
				reply.TaskID = id
				reply.Filename = c.files[id]
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}

		reply.Type = Wait
		return nil

	case ReducePhase:
		for id, task := range c.reduceTasks {
			if !task.isDone && time.Since(task.startTime) > 10*time.Second {
				task.startTime = time.Now()
				reply.Type = Reduce
				reply.TaskID = id
				reply.NMap = c.nMap
				return nil
			}
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

	if c.phase == MapPhase && args.TaskType == Map {
		if task, ok := c.mapTasks[args.TaskID]; ok {
			task.isDone = true
		}
		if c.allDone(c.mapTasks) {
			c.phase = ReducePhase
		}
		return nil
	}

	if c.phase == ReducePhase && args.TaskType == Reduce {
		if task, ok := c.reduceTasks[args.TaskID]; ok {
			task.isDone = true
		}
		if c.allDone(c.reduceTasks) {
			c.phase = DonePhase
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

	c.mu.Lock()
	defer c.mu.Unlock()
	ret := (c.phase == DonePhase)

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = Map
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
