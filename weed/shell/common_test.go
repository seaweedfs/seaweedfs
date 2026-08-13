package shell

import (
	_ "embed"
	"errors"
	"testing"
	"time"
)

//go:embed volume.list.txt
var topoData string

//go:embed volume.list2.txt
var topoData2 string

//go:embed volume.ecshards.txt
var topoDataEc string

var (
	testTopology1  = parseOutput(topoData)
	testTopology2  = parseOutput(topoData2)
	testTopologyEc = parseOutput(topoDataEc)
)

func TestExecuteParallelTaskGroups(t *testing.T) {
	firstTaskStarted := make(chan struct{})
	firstTaskRelease := make(chan struct{})
	secondTaskStarted := make(chan struct{})
	secondTaskInFirstGroupStarted := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- executeParallelTaskGroups(2, [][]ErrorWaitGroupTask{
			{
				func() error {
					close(firstTaskStarted)
					<-firstTaskRelease
					return nil
				},
				func() error {
					close(secondTaskInFirstGroupStarted)
					return nil
				},
			},
			{
				func() error {
					close(secondTaskStarted)
					return nil
				},
			},
		})
	}()

	select {
	case <-firstTaskStarted:
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}
	select {
	case <-secondTaskStarted:
	case <-time.After(time.Second):
		t.Fatal("independent task group did not start in parallel")
	}
	select {
	case <-secondTaskInFirstGroupStarted:
		t.Fatal("tasks in the same group ran in parallel")
	default:
	}

	close(firstTaskRelease)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("execute parallel task groups: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("parallel task groups did not finish")
	}
}

func TestExecuteParallelTaskGroupsStopsOnlyFailedGroup(t *testing.T) {
	expectedErr := errors.New("move failed")
	failedGroupContinued := false
	otherGroupRan := false

	err := executeParallelTaskGroups(1, [][]ErrorWaitGroupTask{
		{
			func() error { return expectedErr },
			func() error {
				failedGroupContinued = true
				return nil
			},
		},
		{
			func() error {
				otherGroupRan = true
				return nil
			},
		},
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected task error, got %v", err)
	}
	if failedGroupContinued {
		t.Fatal("task after a failed task in the same group ran")
	}
	if !otherGroupRan {
		t.Fatal("independent task group did not run")
	}
}
