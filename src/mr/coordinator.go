package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskWait TaskStatus = iota
	TaskRunning
	TaskComplete
)

type TaskStatus int

type Coordinator struct {
	mu    sync.RWMutex
	cond  sync.Cond
	files []string

	mapTasks      []TaskStatus
	mapStart      []time.Time
	mapTaskclosed bool

	nReduce     int
	reduceTask  []TaskStatus
	reduceStart []time.Time
	closed      bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskForMapTask(args *MapReply, reply *MapReply) error {
	id := c.getMapTask()
	if id == -2 {
		reply.ID = id
		fmt.Println("[Map] all map finished")
		return nil
	}
	if id == -1 {
		fmt.Println("[Map] wait task not exit")
		reply.ID = id
		return nil
	}
	reply.Filename = c.files[id]
	reply.ID = id
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) MapTaskFinish(args *MapArg, reply *MapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("[Map] a map file complete", args.Filename, args.ID)
	c.mapTasks[args.ID] = TaskComplete
	fmt.Println(c.mapTasks)
	for _, st := range c.mapTasks {
		if st != TaskComplete {
			return nil
		}
	}
	c.mapTaskclosed = true
	fmt.Println("[Map] all map complete")
	return nil
}

func (c *Coordinator) AskReduceTask(args *MapReply, reply *ReduceReply) error {
	id := c.getReduceTask()
	if id == -1 {
		fmt.Println("[Reduce] wait task not exit")
		reply.ID = id
		return nil
	}
	reply.ID = id
	reply.OK = true
	reply.NMap = len(c.files)

	return nil
}
func (c *Coordinator) ReduceTaskFinish(args *ReduceArg, reply *ReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTask[args.ID] = TaskComplete
	fmt.Println("[Map] a reduce file complete", args.ID)
	reply.OK = true
	for _, st := range c.reduceTask {
		if st != TaskComplete {
			return nil
		}
	}
	c.closed = true
	fmt.Println("[Reduce] all reduce complete")
	return nil
}

func (c *Coordinator) getMapTask() int {
	if c.mapTaskclosed || c.closed {
		return -2
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, status := range c.mapTasks {
		if status == TaskWait {
			c.mapTasks[i] = TaskRunning
			c.mapStart[i] = time.Now()
			fmt.Println("generate map task id ", i)
			return i
		}
	}
	c.checkMap()
	fmt.Println(c.mapTasks)
	return -1
}

func (c *Coordinator) getReduceTask() int {
	if c.closed {
		return -2
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, status := range c.mapTasks {
		if status == TaskWait {
			c.reduceTask[i] = TaskRunning
			c.reduceStart[i] = time.Now()
			return i
		}
	}
	c.checkReduce()
	return -1
}

func (c *Coordinator) checkMap() {
	if !c.mapTaskclosed {
		for i, t := range c.mapStart {
			if c.mapTasks[i] == TaskRunning {
				if time.Now().Add(-10 * time.Second).After(t) {
					c.mapTasks[i] = TaskWait
				}
			}
		}
	}
}

func (c *Coordinator) checkReduce() {
	if !c.closed {
		for i, t := range c.reduceStart {
			if c.reduceTask[i] == TaskRunning {
				if time.Now().Add(-10 * time.Second).After(t) {
					c.reduceTask[i] = TaskWait
				}
			}
		}
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskStatus, len(files)),
		mapStart:    make([]time.Time, len(files)),
		reduceTask:  make([]TaskStatus, nReduce),
		reduceStart: make([]time.Time, nReduce),
	}

	for i := range files {
		c.mapTasks[i] = TaskWait
	}
	for i := range c.reduceTask {
		c.reduceTask[i] = TaskWait
	}

	c.server()
	return &c
}
