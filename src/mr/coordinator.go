package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)
type CoordinatorTask struct {
	TaskStatus 		CoordinatorTaskStatus
	StartTime		time.Time
	TaskReference	*Task
}
type Coordinator struct {
	// Your definitions here.
	TaskQueue			chan *Task
	TaskMeta			map[int]*CoordinatorTask
	CoordinatorPhase	State
	NReduce				int
	InputFiles			[]string
	Intermediates		[][]string
	lock    			*sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()

	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue

		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}
func (c *Coordinator) createMapTask() {
	for i, InputFile := range c.InputFiles {
		task := & Task{
			Input: InputFile,
			TaskState: Map,
			TaskNumber: i,
			NReducer: c.NReduce,
		}
		c.TaskQueue <- task
		c.TaskMeta[i] = &CoordinatorTask{
			TaskReference: task,
		}
	}
}
func (c *Coordinator) createReduceTask() {
	for i := 0; i < c.NReduce; i++ {
		task := & Task{
			TaskState: Reduce,
			TaskNumber: i,
		}
		c.TaskQueue <- task
		c.TaskMeta[i] = &CoordinatorTask{
			TaskReference: task,
		}
	}
}

func (c *Coordinator) processTaskResult(task *Task) {
	c.lock.L.Lock()
	defer c.lock.L.Lock()

	switch task.TaskState {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		c.TaskMeta[task.TaskNumber].TaskStatus = Completed
		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			c.CoordinatorPhase = Exit
		}
	}
}
func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	ret := c.CoordinatorPhase == Exit
	return ret
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		c.lock.L.Lock()
		if c.CoordinatorPhase == Exit {
			c.lock.L.Unlock()
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProgress && time.Now().Sub(coordinatorTask.StartTime) > 10 * time.Second {
				c.TaskQueue <- coordinatorTask.TaskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		c.lock.L.Unlock()
	}
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue: 			make(chan *Task, max(nReduce, len(files))),
		TaskMeta: 			make(map[int]*CoordinatorTask),
		CoordinatorPhase: 	Map,
		NReduce: 			nReduce,
		InputFiles: 		files,
		Intermediates: 		make([][]string, nReduce),
	}

	// Your code here.
	c.createMapTask()

	c.server()
	return &c
}
