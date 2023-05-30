package gomapreducr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type State int

type TaskType string

type Opt func(*Coordinator)

const (
	Idle State = iota
	InProgress
	Completed

	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
)

type Task struct {
	id       int
	state    State
	index    string
	workerID string
}

type StatsConfig struct {
	enabled  bool
	duration time.Duration
}

type Coordinator struct {
	mapTasks           map[string]*Task
	reduceTasks        map[int]*Task
	pendingMapTasks    int
	pendingReduceTasks int

	nMapTasks int
	nReduce   int

	sync.RWMutex

	closeCh    chan struct{}
	statsCfg   *StatsConfig
	taskNumber int
}

func (c *Coordinator) stats() {
	c.RLock()
	fmt.Printf("Total map tasks: %d\n", c.nMapTasks)
	fmt.Printf("Total reduce tasks: %d\n", c.nReduce)

	fmt.Printf("Total uncompleted map tasks: %d\n", c.pendingMapTasks)
	fmt.Printf("Total uncompleted reduce tasks: %d\n", c.pendingReduceTasks)

	fmt.Printf("Map task states\n")
	fmt.Printf("---------------------------------------------------\n")
	for filename, t := range c.mapTasks {
		fmt.Printf("filename: %s | state: %v | workerId: %s\n", filename, t.state, t.workerID)
	}
	fmt.Printf("Reduce task states\n")
	fmt.Printf("---------------------------------------------------\n")
	for index, t := range c.reduceTasks {
		fmt.Printf("index: %d | state: %v | workerId: %s\n", index, t.state, t.workerID)
	}
	c.RUnlock()
	fmt.Println()
}

func WithStats(interval time.Duration) Opt {
	return func(coordinator *Coordinator) {
		cfg := StatsConfig{
			enabled:  true,
			duration: interval,
		}

		coordinator.statsCfg = &cfg
	}
}
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock()
	defer c.Unlock()
	if c.pendingMapTasks > 0 {
		// get task in Idle state
		task := c.getUnassignedTask(MapTask)
		if task == nil {
			// this means that there is no idle task, but some are still in progress i.e not yet marked as completed
			reply.ReplyStatus = NoIdle
			return nil
		}
		c.taskNumber++

		reply.TaskType = Map
		reply.Filename = task.index
		reply.NumReduceBuckets = c.nReduce
		reply.ReplyStatus = Assigned
		reply.TaskNum = c.taskNumber

		task.workerID = args.WorkerID
		task.state = InProgress

		// track a task
		go c.trackAssignedTask(task)

	} else if c.pendingReduceTasks > 0 {
		task := c.getUnassignedTask(ReduceTask)
		if task == nil {
			// this means that there is no idle task, but some are still in progress i.e not yet marked as completed
			reply.ReplyStatus = NoIdle
			return nil
		}
		c.taskNumber++

		reply.TaskType = Reduce
		reply.ReduceIndex, _ = strconv.Atoi(task.index)
		reply.ReplyStatus = Assigned
		reply.TaskNum = c.taskNumber

		task.workerID = args.WorkerID
		task.state = InProgress

		// track a task
		go c.trackAssignedTask(task)
	} else {
		reply.ReplyStatus = Exit
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.Lock()
	defer c.Unlock()
	switch args.TaskType {
	case Map:
		task := c.mapTasks[args.Filename]
		if args.TaskSucceeded {
			// mark task as Completed
			task.state = Completed
			// reduce pendingMapTask
			c.pendingMapTasks--
		} else {
			// move task back to idle state to vbe picked up
			task.state = Idle
		}
	case Reduce:
		task := c.reduceTasks[args.Index]
		if args.TaskSucceeded {
			// mark task as Completed
			task.state = Completed
			// reduce pendingMapTask
			c.pendingReduceTasks--
		} else {
			// move task back to idle state to vbe picked up
			task.state = Idle
		}
	}
	return nil
}

func (c *Coordinator) trackAssignedTask(task *Task) {
	c.RLock()
	if task.state != InProgress {
		return
	}
	c.RUnlock()

	time.Sleep(10 * time.Second)

	c.Lock()
	if task.state == InProgress {
		task.state = Idle
	}
	c.Unlock()
}
func (c *Coordinator) getUnassignedTask(taskType TaskType) *Task {
	switch taskType {
	case MapTask:
		for _, task := range c.mapTasks {
			if task.state == Idle {
				return task
			}
		}
	case ReduceTask:
		for _, task := range c.reduceTasks {
			if task.state == Idle {
				return task
			}
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname) //remove any existing socket
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.RLock()
	defer c.RUnlock()
	if c.pendingMapTasks == 0 && c.pendingReduceTasks == 0 {
		ret = true
	}
	return ret
}

func MakeCoordinator(files []string, nReduce int, opts ...Opt) *Coordinator {
	c := Coordinator{
		mapTasks:    make(map[string]*Task),
		reduceTasks: make(map[int]*Task),
	}

	for _, opt := range opts {
		opt(&c)
	}

	// Your code here.
	taskId := 0
	for _, file := range files {
		taskId++
		task := Task{
			id:    taskId,
			state: Idle,
			index: file,
		}
		c.mapTasks[file] = &task
	}
	c.pendingMapTasks = len(files)
	c.nMapTasks = len(files)
	c.nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		taskId++
		task := Task{
			id:    taskId,
			state: Idle,
			index: strconv.Itoa(i),
		}
		c.reduceTasks[i] = &task
	}
	c.pendingReduceTasks = nReduce

	c.server()
	if c.statsCfg != nil && c.statsCfg.enabled {
		c.closeCh = make(chan struct{})
		ticker := time.NewTicker(c.statsCfg.duration)
		go func() {
			for {
				select {
				case <-c.closeCh:
					return
				case <-ticker.C:
					c.stats()
				}
			}
		}()
	}

	return &c
}

func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/gomapreducr-%d.sock", os.Getuid())
}
