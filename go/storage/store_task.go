package storage

import (
	"errors"
	"net/url"
	"time"

	"github.com/chrislusf/seaweedfs/go/glog"
)

type TaskType string

const (
	TaskVacuum  TaskType = "VACUUM"
	TaskReplica TaskType = "REPLICA"
	TaskBalance TaskType = "BALANCE"
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
	Id              string
	startTime       time.Time
	worker          TaskWorker
	ch              chan bool
	result          error
	cleanWhenFinish bool
}

type TaskManager struct {
	TaskList map[string]*Task
}

func NewTask(worker TaskWorker, id string) *Task {
	t := &Task{
		Id:        id,
		worker:    worker,
		startTime: time.Now(),
		result:    ErrTaskNotFinish,
		ch:        make(chan bool, 1),
	}
	go func(t *Task) {
		t.result = t.worker.Run()
		if t.cleanWhenFinish {
			glog.V(0).Infof("clean task (%s) when finish.", t.Id)
			t.worker.Clean()
		}
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
	switch TaskType(tt) {
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
	tm.TaskList[tid] = NewTask(tw, tid)
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

func (tm *TaskManager) Clean(tid string) error {
	t, ok := tm.TaskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	delete(tm.TaskList, tid)
	if t.QueryResult(time.Second*30) == ErrTaskNotFinish {
		t.cleanWhenFinish = true
		glog.V(0).Infof("task (%s) is not finish, clean it later.", tid)
	} else {
		t.worker.Clean()
	}
	return nil
}

func (tm *TaskManager) ElapsedDuration(tid string) (time.Duration, error) {
	t, ok := tm.TaskList[tid]
	if !ok {
		return 0, ErrTaskNotFound
	}
	return time.Since(t.startTime), nil
}
