package gomapreducr

const (
	// TaskTypes
	Map    = 0
	Reduce = 1

	// Status
	Exit     = -1
	Assigned = 0
	NoIdle   = 1
)

type GetTaskArgs struct {
	WorkerID string
}

type GetTaskReply struct {
	ReplyStatus      int
	TaskType         int
	Filename         string
	ReduceIndex      int
	NumReduceBuckets int
	TaskNum          int
}

type CompleteTaskArgs struct {
	Filename      string
	Index         int
	TaskType      int
	TaskSucceeded bool
}

type CompleteTaskReply struct {
}
