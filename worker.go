package gomapreducr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type mapperFunc func(string, string) []KeyValue
type reducerFunc func(string, []string) string

type worker struct {
	id      string
	mapFunc mapperFunc
	redFunc reducerFunc
}

type workerTask struct {
	taskType       int
	filename       string
	index          int
	nReduceBuckets int
	taskNum        int
}

func getId() string {

	x := rand.NewSource(time.Now().UnixNano())
	y := rand.New(x)

	return strconv.Itoa(y.Intn(time.Now().Nanosecond()))
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := worker{
		id:      getId(),
		mapFunc: mapf,
		redFunc: reducef,
	}

	for {
		status, task := w.CallGetTask()

		fmt.Printf("Got status: %d, task: %+v\n", status, task)

		if status == Assigned {
			taskErr := w.performTask(task)
			taskSucceeded := true
			if taskErr != nil {
				fmt.Println(taskErr.Error())
				taskSucceeded = false
			}
			w.CallCompleteTask(task, taskSucceeded)
		} else if status == NoIdle {
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
		time.Sleep(time.Second)
	}
}

func (w *worker) performTask(task *workerTask) error {
	switch task.taskType {
	case Map:
		filename := task.filename
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		file.Close()
		kva := w.mapFunc(filename, string(content))

		// kva contains all keyvalue pair for a mapper
		// sort the kva
		sort.Sort(ByKey(kva))
		// mapper will write the kva to its reducer file; based on nReduce
		reducerPartitions := make([][]KeyValue, task.nReduceBuckets)
		for _, kv := range kva {
			partitionIdx := ihash(kv.Key) % task.nReduceBuckets
			reducerPartitions[partitionIdx] = append(reducerPartitions[partitionIdx], kv)
		}

		for i := 0; i < task.nReduceBuckets; i++ {

			// mr-X-Y
			intermediateFile := fmt.Sprintf("mr-%d-%d", task.taskNum, i)
			f, cErr := os.Create(intermediateFile)
			if cErr != nil {
				return cErr
			}
			enc := json.NewEncoder(f)
			for _, kv := range reducerPartitions[i] {
				enc.Encode(kv)
			}
			f.Close()
		}
	case Reduce:
		reducerIndex := task.index
		// create temporary file
		f, err := os.CreateTemp("", "mr")
		if err != nil {
			return err
		}
		// read all intermediate files belonging to reducer to a list[kv]
		pattern := fmt.Sprintf("mr-*-%d", reducerIndex)
		files, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		kva := []KeyValue{}

		for _, fp := range files {
			f, err := os.Open(fp)
			if err != nil {
				return err
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			f.Close()
		}
		// sort the intermediate list
		sort.Sort(ByKey(kva))
		// call Reduce on each distinct key in intermediate[], and print to temp file
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := w.redFunc(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)

			i = j
		}
		// atomically rename file
		err = os.Rename(f.Name(), fmt.Sprintf("mr-out-%d", reducerIndex))
		if err != nil {
			return err
		}
		f.Close()
	}
	return nil
}

func (w *worker) CallGetTask() (int, *workerTask) {
	args := GetTaskArgs{}
	args.WorkerID = w.id
	reply := GetTaskReply{}

	var wTask *workerTask
	statusCode := Exit

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		statusCode = reply.ReplyStatus
		wTask = &workerTask{
			taskType:       reply.TaskType,
			filename:       reply.Filename,
			nReduceBuckets: reply.NumReduceBuckets,
			taskNum:        reply.TaskNum,
			index:          reply.ReduceIndex,
		}
	}
	return statusCode, wTask
}

func (w *worker) CallCompleteTask(task *workerTask, succeeded bool) {
	args := CompleteTaskArgs{}
	args.Filename = task.filename
	args.Index = task.index
	args.TaskSucceeded = succeeded
	args.TaskType = task.taskType
	reply := CompleteTaskReply{}

	call("Coordinator.CompleteTask", &args, &reply)

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
