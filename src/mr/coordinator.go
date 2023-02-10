package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	// map
	muMap        sync.Mutex
	MapTaskChan  chan string
	MapTaskNum   int
	MapTaskID    chan int
	MapTaskState map[string]bool

	// reduce
	muReduce        sync.Mutex
	ReduceTaskChan  chan int
	ReduceTaskState map[int]bool

	// checkout allover
	muOver    sync.Mutex
	isAllover bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) isAlloverDone(args *AlloverArgs, reply *AlloverReply) error {
	c.muOver.Lock()
	reply.Done = c.isAllover
	c.muOver.Unlock()
	return nil
}

func (c *Coordinator) MapNum(args *NumMapArgs, reply *NumMapReply) error {
	// TODO
	return nil
}

func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	// master wait worker for 10s
	// TODO
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	if args.Mes == "single" { // worker call master a mapTask is done
		// TODO
	} else if args.Mes == "all" { // worker checks whether mapTask has been completed allover
		// TODO
	}
	return nil
}

// TODO reduceTask

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	fmt.Println("【now into server...】")
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":9999")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//fmt.Println(l, e)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	// TODO checks reduceTask is done

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("【now into init master....】")
	c := Coordinator{}
	// Your code here.
	// init master
	c.MapTaskNum = len(files)
	c.MapTaskChan = make(chan string, len(files))
	c.MapTaskID = make(chan int, len(files))
	c.MapTaskState = make(map[string]bool)
	c.ReduceTaskChan = make(chan int, nReduce)
	c.ReduceTaskState = make(map[int]bool)
	for i, fn := range files {
		c.MapTaskChan <- fn
		c.MapTaskID <- i
		c.MapTaskState[fn] = false
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskChan <- i
		c.ReduceTaskState[i] = false
	}
	c.server()
	return &c
}
