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

type Coordinator struct {
	// Your definitions here.
	// map
	MuMap        sync.Mutex
	MapTaskChan  chan string
	MapTaskNum   int
	MapTaskID    chan int
	MapTaskState map[string]bool

	// reduce
	MuReduce        sync.Mutex
	ReduceTaskChan  chan int
	ReduceTaskState map[int]bool

	// checkout allover
	MuOver    sync.Mutex
	IsAllover bool
}

// Your code here -- RPC handlers for the worker to call.

/*// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}*/

func (c *Coordinator) IsAlloverDone(args *AlloverArgs, reply *AlloverReply) error {
	c.MuOver.Lock()
	reply.Done = c.IsAllover
	c.MuOver.Unlock()
	return nil
}

func (c *Coordinator) MapNum(args *NumMapArgs, reply *NumMapReply) error {
	reply.Num = c.MapTaskNum
	return nil
}

func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	select {
	case reply.File = <-c.MapTaskChan:
		reply.Id = <-c.MapTaskID
		// master wait worker for 10s
		go func(file string, id int) {
			time.Sleep(time.Second * 10)
			c.MuMap.Lock()
			if !c.MapTaskState[file] { // register map to middle file
				c.MapTaskChan <- file
				c.MapTaskID <- id
			}
			c.MuMap.Unlock()
		}(reply.File, reply.Id)
	default:
		reply.Id = 0
		reply.File = ""
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	if args.Mes == "single" { // worker call master a mapTask is done
		c.MuMap.Lock()
		c.MapTaskState[args.File] = true
		c.MuMap.Unlock()
	} else if args.Mes == "all" { // worker checks whether mapTask has been completed allover
		reply.IsDone = true
		c.MuMap.Lock()
		for _, v := range c.MapTaskState {
			if !v { // there have incomplete map tasks
				reply.IsDone = false
				break
			}
		}
		c.MuMap.Unlock()
	}
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	select {
	case reply.ID = <-c.ReduceTaskChan:
		go func(id int) {
			time.Sleep(time.Second * 10)
			c.MuReduce.Lock()
			if !c.ReduceTaskState[id] {
				c.ReduceTaskChan <- id
			}
			c.MuReduce.Unlock()
		}(reply.ID)
	default:
		reply.ID = -1
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	c.MuReduce.Lock()
	c.ReduceTaskState[args.ID] = true
	c.MuReduce.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	//fmt.Println("【now into server...】")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":9999")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//fmt.Println(l, e)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	// checks reduceTask is done
	c.MuReduce.Lock()
	for _, v := range c.ReduceTaskState {
		if !v {
			ret = false
			break
		}
	}
	c.MuReduce.Unlock()
	c.MuOver.Lock()
	c.IsAllover = ret
	c.MuOver.Unlock()
	return c.IsAllover
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println("【now into init master....】")
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
