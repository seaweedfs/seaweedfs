package storage

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/chrislusf/seaweedfs/go/util"
)

type TaskParams map[string]string

var (
	ErrTaskTimeout = errors.New("TaskTimeout")
)

type TaskCli struct {
	TID      string
	DataNode string
}

func NewTaskCli(dataNode string, task string, params TaskParams) (*TaskCli, error) {
	args := url.Values{}
	args.Set("task", task)
	for k, v := range params {
		args.Set(k, v)
	}
	m, e := util.RemoteApiCall(dataNode, "/admin/task/new", args)
	if e != nil {
		return nil, e
	}
	tid := m["tid"].(string)
	if tid == "" {
		return nil, fmt.Errorf("Empty %s task", task)
	}
	return &TaskCli{
		TID:      tid,
		DataNode: dataNode,
	}, nil
}

func (c *TaskCli) WaitAndQueryResult(timeout time.Duration) error {
	startTime := time.Now()
	args := url.Values{}
	args.Set("task", c.TID)
	for time.Since(startTime) < timeout {
		_, e := util.RemoteApiCall(c.DataNode, "/admin/task/query", args)
		if e.Error() == ErrTaskNotFinish.Error() {
			continue
		}
		return e
	}
	return ErrTaskTimeout
}

func (c *TaskCli) Commit() error {
	args := url.Values{}
	args.Set("task", c.TID)
	_, e := util.RemoteApiCall(c.DataNode, "/admin/task/commit", args)
	return e
}

func (c *TaskCli) Clean() error {
	args := url.Values{}
	args.Set("task", c.TID)
	_, e := util.RemoteApiCall(c.DataNode, "/admin/task/clean", args)
	return e
}
