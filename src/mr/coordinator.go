package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mu sync.Mutex
)

type Coordinator struct {
	Files             []string
	ReducerNum        int
	TaskIdGlobal      int
	Phase             Phase
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	TaskInfoMap       map[int]*TaskInfo
}

type TaskInfo struct {
	State   State
	StartTs time.Time
	TaskPtr *Task
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.Phase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				fmt.Println("[PollTask] MapTaskId: ", reply.TaskId)
				c.adjustTaskState(reply.TaskId)
			} else {
				reply.TaskType = WaitTask
				if c.isPhaseDone() {
					c.toNextPhase()
				}
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				fmt.Println("[PollTask] ReduceTaskId: ", reply.TaskId)
				c.adjustTaskState(reply.TaskId)
			} else {
				reply.TaskType = WaitTask
				if c.isPhaseDone() {
					c.toNextPhase()
				}
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		fmt.Println("[PollTask] Unreached!")
	}
	return nil
}

func (c *Coordinator) FinishTask(args *Task, reply *TaskArgs) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.Phase {
	case MapPhase:
		{
			if c.TaskInfoMap[args.TaskId].State == Running {
				c.TaskInfoMap[args.TaskId].State = Done
				fmt.Println("[FinshTask] MapTaskid:", args.TaskId)
			} else {
				fmt.Println("[FinshTask] MapTaskid:", args.TaskId, " CurState: ",
					c.TaskInfoMap[args.TaskId].State, ", Expect: Running")
			}
		}
	case ReducePhase:
		{
			if c.TaskInfoMap[args.TaskId].State == Running {
				c.TaskInfoMap[args.TaskId].State = Done
				fmt.Println("[FinshTask] ReduceTaskid:", args.TaskId)
			} else {
				fmt.Println("[FinshTask] ReduceTaskid:", args.TaskId, " CurState: ",
					c.TaskInfoMap[args.TaskId].State, ", Expect: Running")
			}
		}
	default:
		fmt.Println("[FinishTask] unreached!")
	}
	return nil
}

// ================= Phase Thransfer  ====================
func (c *Coordinator) isPhaseDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDownNum = 0
	)
	for _, taskinfo := range c.TaskInfoMap {
		if taskinfo.TaskPtr.TaskType == MapTask {
			if taskinfo.State == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if taskinfo.TaskPtr.TaskType == ReduceTask {
			if taskinfo.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDownNum++
			}
		}
	}
	if c.Phase == MapPhase && mapDoneNum > 0 && mapUnDoneNum == 0 {
		return true
	}
	if c.Phase == ReducePhase && reduceDoneNum > 0 && reduceUnDownNum == 0 {
		return true
	}
	return false
}

func (c *Coordinator) toNextPhase() {
	if c.Phase == MapPhase {
		c.makeReduceTasks()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = AllDone
	}
	fmt.Println("[Thransfer Phase] cur: ", c.Phase)
}

// ===========  Called by main/mrcoordinator.go ================

// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:             files,
		ReducerNum:        nReduce,
		TaskIdGlobal:      0,
		Phase:             MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskInfoMap:       make(map[int]*TaskInfo, len(files)+nReduce),
	}
	c.makeMapTasks()
	c.server()
	go c.CrashDetector()
	return &c
}

func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == AllDone {
		fmt.Println("======== All done ========")
		return true
	}
	return false
}

// ================= Util functions ====================

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) makeMapTasks() {
	for _, file := range c.Files {
		task := Task{
			TaskType:   MapTask,
			TaskId:     c.TaskIdGlobal,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{file},
		}
		c.TaskIdGlobal++
		taskInfo := TaskInfo{
			State:   Waiting,
			TaskPtr: &task,
		}
		tf, _ := c.TaskInfoMap[taskInfo.TaskPtr.TaskId]
		if tf != nil {
			fmt.Println("taskinfo repeated!")
		} else {
			c.TaskInfoMap[taskInfo.TaskPtr.TaskId] = &taskInfo
		}
		fmt.Println("make a map task :", &task)
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    c.TaskIdGlobal,
			FileSlice: selectReduceName(i),
		}
		c.TaskIdGlobal++

		taskInfo := TaskInfo{
			State:   Waiting,
			TaskPtr: &task,
		}
		tf, _ := c.TaskInfoMap[taskInfo.TaskPtr.TaskId]
		if tf != nil {
			fmt.Println("[ERROR] taskinfo repeated!")
		} else {
			c.TaskInfoMap[taskInfo.TaskPtr.TaskId] = &taskInfo
		}
		fmt.Println("[makeReduceTask] ", &task)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) adjustTaskState(taskid int) {
	tf, ok := c.TaskInfoMap[taskid]
	if !ok || tf.State != Waiting {
		fmt.Println("[ERROR] TaskId ", taskid, " is already running")
	}
	tf.State = Running
	tf.StartTs = time.Now()
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			break
		}
		for _, taskinfo := range c.TaskInfoMap {
			if taskinfo.State == Running && time.Since(taskinfo.StartTs) > 9*time.Second {
				fmt.Println("[Detect] crushed ", taskinfo)
				if taskinfo.TaskPtr.TaskType == MapTask {
					c.MapTaskChannel <- taskinfo.TaskPtr
				} else if taskinfo.TaskPtr.TaskType == ReduceTask {
					c.ReduceTaskChannel <- taskinfo.TaskPtr
				} else {
					fmt.Println("[Detect] unreached !!!")
				}
				taskinfo.State = Waiting
			}
		}
		mu.Unlock()
	}
}
