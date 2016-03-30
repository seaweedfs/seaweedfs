package storage

import (
	"errors"
	"net/url"
	"time"

	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

const (
	TaskVacuum    = "vacuum"
	TaskReplicate = "replicate"
	TaskBalance   = "balance"
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

type task struct {
	Id              string
	startTime       time.Time
	worker          TaskWorker
	ch              chan bool
	result          error
	cleanWhenFinish bool
}

type TaskManager struct {
	taskList map[string]*task
	lock     sync.RWMutex
}

func newTask(worker TaskWorker, id string) *task {
	t := &task{
		Id:        id,
		worker:    worker,
		startTime: time.Now(),
		result:    ErrTaskNotFinish,
		ch:        make(chan bool, 1),
	}
	go func(t *task) {
		t.result = t.worker.Run()
		if t.cleanWhenFinish {
			glog.V(0).Infof("clean task (%s) when finish.", t.Id)
			t.worker.Clean()
		}
		t.ch <- true

	}(t)
	return t
}

func (t *task) queryResult(waitDuration time.Duration) error {
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
		taskList: make(map[string]*task),
	}
}

func (tm *TaskManager) NewTask(s *Store, args url.Values) (tid string, e error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	tt := args.Get("task")
	vid := args.Get("volume")
	tid = tt + "-" + vid
	if _, ok := tm.taskList[tid]; ok {
		return tid, ErrTaskExists
	}

	var tw TaskWorker
	switch tt {
	case TaskVacuum:
		tw, e = NewVacuumTask(s, args)
	case TaskReplicate:
		tw, e = NewReplicaTask(s, args)
	case TaskBalance:
	}
	if e != nil {
		return
	}
	if tw == nil {
		return "", ErrTaskInvalid
	}
	tm.taskList[tid] = newTask(tw, tid)
	return tid, nil
}

func (tm *TaskManager) QueryResult(tid string, waitDuration time.Duration) (e error) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	t, ok := tm.taskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	return t.queryResult(waitDuration)
}

func (tm *TaskManager) Commit(tid string) (e error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	t, ok := tm.taskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	if t.queryResult(time.Second*30) == ErrTaskNotFinish {
		return ErrTaskNotFinish
	}
	delete(tm.taskList, tid)
	return t.worker.Commit()
}

func (tm *TaskManager) Clean(tid string) error {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	t, ok := tm.taskList[tid]
	if !ok {
		return ErrTaskNotFound
	}
	delete(tm.taskList, tid)
	if t.queryResult(time.Second*30) == ErrTaskNotFinish {
		t.cleanWhenFinish = true
		glog.V(0).Infof("task (%s) is not finish, clean it later.", tid)
	} else {
		t.worker.Clean()
	}
	return nil
}

func (tm *TaskManager) ElapsedDuration(tid string) (time.Duration, error) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	t, ok := tm.taskList[tid]
	if !ok {
		return 0, ErrTaskNotFound
	}
	return time.Since(t.startTime), nil
}
