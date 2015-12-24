package storage

import (
	"errors"
	"net/url"
	"time"
)

const (
	TaskVacuum  = "VACUUM"
	TaskReplica = "REPLICA"
	TaskBalance = "BALANCE"
)

var (
	ErrTaskNotFinish = errors.New("TaskNotFinish")
	ErrTaskNotFound  = errors.New("TaskNotFound")
	ErrTaskInvalid   = errors.New("TaskInvalid")
	ErrTaskExists    = errors.New("TaskExists")
)

type TaskWorker interface {
	Run() error
	Commit() error
	Clean() error
	Info() url.Values
}

type Task struct {
	startTime time.Time
	worker    TaskWorker
	ch        chan bool
	result    error
}

type TaskManager struct {
	TaskList map[string]*Task
}

func NewTask(worker TaskWorker) *Task {
	t := &Task{
		worker:    worker,
		startTime: time.Now(),
		result:    ErrTaskNotFinish,
		ch:        make(chan bool, 1),
	}
	go func(t *Task) {
		t.result = t.worker.Run()
		t.ch <- true
	}(t)
	return t
}

func (t *Task) QueryResult(waitDuration time.Duration) error {
	if t.result == ErrTaskNotFinish && waitDuration > 0 {
		select {
		case <-time.After(waitDuration):
		case <-t.ch:
		}
	}
	return t.result
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		TaskList: make(map[string]*Task),
	}
}

func (tm *TaskManager) NewTask(s *Store, args url.Values) (tid string, e error) {
	tt := args.Get("task")
	vid := args.Get("volumme")
	tid = tt + "-" + vid
	if _, ok := tm.TaskList[tid]; ok {
		return tid, ErrTaskExists
	}
	var tw TaskWorker
	switch tt {
	case TaskVacuum:
		tw, e = NewVacuumTask(s, args)
	case TaskReplica:
		tw, e = NewReplicaTask(s, args)
	case TaskBalance:
	}
	if e != nil {
		return
	}
	if tw == nil {
		return "", ErrTaskInvalid
	}
	tm.TaskList[tid] = NewTask(tw)
	return tid, nil
}

func (tm *TaskManager) QueryResult(tid string, waitDuration time.Duration) (e error) {
	t, ok := tm.TaskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	return t.QueryResult(waitDuration)
}

func (tm *TaskManager) Commit(tid string) (e error) {
	t, ok := tm.TaskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	if t.QueryResult(time.Second*30) == ErrTaskNotFinish {
		return ErrTaskNotFinish
	}
	delete(tm.TaskList, tid)
	return t.worker.Commit()
}

func (tm *TaskManager) Clean(tid string) (e error) {
	t, ok := tm.TaskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	if t.QueryResult(time.Second*30) == ErrTaskNotFinish {
		return ErrTaskNotFinish
	}
	delete(tm.TaskList, tid)
	return t.worker.Clean()
}

func (tm *TaskManager) ElapsedDuration(tid string) (time.Duration, error) {
	t, ok := tm.TaskList[tid]
	if !ok {
		return 0, ErrTaskNotFound
	}
	return time.Since(t.startTime), nil
}
