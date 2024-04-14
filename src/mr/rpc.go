package mr

import (
	"os"
	"strconv"
)

type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	FileSlice  []string // map一个文件对应一个文件，reduce是对应多个temp中间值文件
}

type TaskArgs struct{}

type TaskType int
type Phase int
type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)
const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)
const (
	Waiting State = iota
	Running
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
