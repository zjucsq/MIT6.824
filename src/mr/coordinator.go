package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTodo       map[int]bool
	ReduceTodo    map[int]bool
	MapRunning    map[int]bool
	ReduceRunning map[int]bool
	InputFiles    []string
	MapNum        int
	ReduceNum     int
	Mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.MapNum
	for i := 0; i < c.MapNum; i++ {
		c.MapTodo[i] = true
	}
	for i := 0; i < c.ReduceNum; i++ {
		c.ReduceTodo[i] = true
	}
	c.InputFiles = files

	c.server()
	return &c
}

func (c *Coordinator) AcquireJob(args *AcJobArgs, reply *AcJobReply) error {
	reply.MapNum = c.MapNum
	reply.ReduceNum = c.ReduceNum

	c.Mu.Lock()
	defer c.Mu.Unlock()

	if len(c.MapTodo) > 0 {
		for i := range c.MapTodo {
			reply.AcJob = Job{JobType: MapJob, JobId: i, File: c.InputFiles[i]}
			delete(c.MapTodo, i)
			c.MapRunning[i] = true
			go func(jobId int) {
				time.Sleep(10 * time.Second)
				c.Mu.Lock()
				defer c.Mu.Unlock()
				_, ok := c.MapRunning[jobId]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("map Task", jobId, "time out!")
					delete(c.MapRunning, jobId)
					c.MapTodo[jobId] = true
				}
			}(i)
			return nil
		}
	} else if len(c.MapRunning) > 0 {
		reply.AcJob = Job{JobType: WaitJob}
		log.Println("waiting map job done!")
		return nil
	} else if len(c.ReduceTodo) > 0 {
		for i := range c.ReduceTodo {
			reply.AcJob = Job{JobType: ReduceJob, JobId: i}
			delete(c.ReduceTodo, i)
			c.ReduceRunning[i] = true
			go func(jobId int) {
				time.Sleep(10 * time.Second)
				c.Mu.Lock()
				defer c.Mu.Unlock()
				_, ok := c.ReduceRunning[jobId]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("reduce task", jobId, "time out!")
					delete(c.ReduceRunning, jobId)
					c.ReduceTodo[jobId] = true
				}
			}(i)
			return nil
		}
	} else if len(c.ReduceRunning) > 0 {
		reply.AcJob = Job{JobType: WaitJob}
		log.Println("waiting reduce job done!")
		return nil
	} else {
		// all queue is empty, so the job is done!
		reply.AcJob = Job{JobType: WaitJob}
		log.Println("job is all done!")
		return nil
	}
}
