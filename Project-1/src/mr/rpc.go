package mr

import (
	"os"
	"strconv"
)

type TaskRequest struct {
}

// Type of tasks returned to workers.
type RequestType = int

const (
	Map RequestType = iota
	Reduce
	Wait
	Exit
)

// Sent from Coordinator to Worker in response to TaskRequest
type TaskResponse struct {
	Type     RequestType
	TaskID   int
	Filename string // Only for map tasks
	NReduce  int
	NMap 	 int
}

// Sent from Worker to Coordinator after completing a task
type TaskReport struct {
	TaskType RequestType
	TaskID   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
