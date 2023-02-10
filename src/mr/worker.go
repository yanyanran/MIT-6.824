package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// get number of map task
func getMapNum() int {
	args := NumMapArgs{}
	reply := NumMapReply{}
	call("Coordinator.MapNum", &args, &reply)
	return reply.Num
}

// worker get new task form master
func getMapTask() (string, int) {
	args := MapTaskArgs{}
	reply := MapTaskReply{}
	call("Coordinator.MapTask", &args, &reply)
	return reply.File, reply.Id
}

// notify the master when mapTask is done
// mes: single/all
func mapTaskDone(mes, file string) bool {
	args := MapTaskDoneArgs{}
	args.Mes = mes
	args.File = file
	reply := MapTaskDoneReply{}
	call("Coordinator.MapTaskDone", &args, &reply)
	return reply.IsDone
}

// write file after mapf done
func writeToMiddleFile(taskID int, middleKV []KeyValue) {
	f := make([]*os.File, 10)
	for i := 0; i < 10; i++ {
		fname := fmt.Sprintf("mr-%d-%d", taskID, i)
		f[i], _ = os.Create(fname)
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				fmt.Errorf("closing file is error")
			}
		}(f[i])
	}
	// write kv pair to JSON file (read back during the reduce task
	for _, kv := range middleKV {
		reduceID := ihash(kv.Key) % 10
		enc := json.NewEncoder(f[reduceID])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Errorf("encoding error")
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// TODO part-map
	mapTaskNum := getMapNum() // map file numbers
	for {
		mapFile, mapID := getMapTask()
		if mapFile != "" { // more
			f, err := os.Open(mapFile)
			if err != nil {
				fmt.Errorf("cannot open this file named %s", mapFile)
			}
			mes, err := io.ReadAll(f)
			if err != nil {
				fmt.Errorf("cannot read file named %s", mapFile)
			}
			f.Close()
			kv := mapf(mapFile, string(mes))
			writeToMiddleFile(mapID, kv)
			mapTaskDone("single", mapFile) // call to master -> this mapTask has been done
		} else {                        // mapFile == "" -> none
			if mapTaskDone("all", "") { // when all over done, break for{}
				break
			}
			time.Sleep(time.Second)
		}
	}

	// TODO part-reduce
	for {
		reduceID := getReduceTask()
		if reduceID != -1 {
			kva := make([]KeyValue, 0)
			for j := 0; j < mapTaskNum; j++ {
				fileName := fmt.Sprintf("mr-%d-%d", j, reduceID) // 1fileName v 1reduceTask
				f, err := os.Open(fileName)
				if err != nil {
					fmt.Errorf("cannot open file named: %v", fileName)
				}
				// read back files
				dec := json.NewDecoder(f)
				f.Close()
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv) // append result
				}
			}
			writeToEndFile(reduceID, kva, reducef)
			reduceTaskDone(reduceID) // call to master
		} else {             // when reduceID is -1, it means none reduceTask
			if IsAllover() { // judge all reduceTask is over
				break
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func IsAllover() bool {
	args := AlloverArgs{}
	reply := AlloverReply{}
	call("Coordinator.isAlloverDone", &args, &reply)
	return reply.Done
}

func getReduceTask() int {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Coordinator.ReduceTask", &args, &reply)
	return reply.ID
}

// mid: Intermediate datasets
func writeToEndFile(reduceID int, mid []KeyValue, reducef func(string, []string) string) {
	sort.Sort(ByKey(mid))
	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, _ := os.Create(oname) // output file
	defer ofile.Close()
	// call reduce at different middle key
	i := 0
	for i < len(mid) {
		j := i + 1
		for j < len(mid) && mid[j].Key == mid[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, mid[k].Value)
		}
		output := reducef(mid[i].Key, values) // call reduce func
		fmt.Fprintf(ofile, "%v %v\n", mid[i].Key, output)
		i = j
	}
}

func reduceTaskDone(id int) {
	args := ReduceTaskDoneArgs{ID: id}
	reply := ReduceTaskDoneReply{}
	call("Coordinator.ReduceTaskDone", &args, &reply)
}

/*// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.声明参数结构
	args := ExampleArgs{}

	// fill in the argument(s).填写参数
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9999")
	/*	sockname := coordinatorSock()
		c, err := rpc.DialHTTP("unix", sockname)*/
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
