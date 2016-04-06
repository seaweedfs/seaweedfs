package storage

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type TaskParams map[string]string

var (
	ErrTaskTimeout = errors.New("TaskTimeout")
)

type TaskCli struct {
	TID      string
	DataNode string
}

func NewTaskCli(dataNode string, taskType string, params TaskParams) (*TaskCli, error) {
	args := url.Values{}
	args.Set("task", taskType)
	for k, v := range params {
		args.Set(k, v)
	}
	m, e := util.RemoteApiCall(dataNode, "/admin/task/new", args)
	if e != nil {
		return nil, e
	}
	tid := m["tid"].(string)
	if tid == "" {
		return nil, fmt.Errorf("Empty %s task", taskType)
	}
	return &TaskCli{
		TID:      tid,
		DataNode: dataNode,
	}, nil
}

func (c *TaskCli) WaitAndQueryResult(timeout time.Duration) error {
	startTime := time.Now()
	args := url.Values{}
	args.Set("tid", c.TID)
	args.Set("timeout", time.Minute.String())
	tryTimes := 0
	for time.Since(startTime) < timeout {
		_, e := util.RemoteApiCall(c.DataNode, "/admin/task/query", args)
		if e == nil {
			//task have finished and have no error
			return nil
		}
		if util.IsRemoteApiError(e) {
			if e.Error() == ErrTaskNotFinish.Error() {
				tryTimes = 0
				continue
			}
			return e
		} else {
			tryTimes++
			if tryTimes >= 10 {
				return e
			}
			glog.V(0).Infof("query task (%s) error %v, wait 1 minute then retry %d times", c.TID, e, tryTimes)
			time.Sleep(time.Minute)
		}

	}
	return ErrTaskTimeout
}

func (c *TaskCli) Commit() error {
	args := url.Values{}
	args.Set("tid", c.TID)
	_, e := util.RemoteApiCall(c.DataNode, "/admin/task/commit", args)
	return e
}

func (c *TaskCli) Clean() error {
	args := url.Values{}
	args.Set("tid", c.TID)
	_, e := util.RemoteApiCall(c.DataNode, "/admin/task/clean", args)
	return e
}
