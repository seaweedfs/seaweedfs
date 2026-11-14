package s3_lifecycle

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
	"github.com/stretchr/testify/require"
)

type progressSender struct {
	mu       sync.Mutex
	progress []*plugin_pb.JobProgressUpdate
	failures int
	attempts int
}

func (s *progressSender) SendProgress(p *plugin_pb.JobProgressUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failures > 0 {
		s.failures--
		return errors.New("stream closed")
	}
	s.progress = append(s.progress, p)
	return nil
}

func (s *progressSender) SendCompleted(*plugin_pb.JobCompleted) error { return nil }

func (s *progressSender) sent() []*plugin_pb.JobProgressUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*plugin_pb.JobProgressUpdate(nil), s.progress...)
}

func (s *progressSender) tries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

type fakeRun struct {
	entries, events, done, rpcErrors atomic.Int64
}

func (f *fakeRun) summary() dailyrun.RunSummary {
	return dailyrun.RunSummary{
		Phase:          dailyrun.PhaseWalk,
		Bucket:         "b1",
		EntriesVisited: f.entries.Load(),
		EventsScanned:  f.events.Load(),
		Outcomes:       map[string]int64{"DONE": f.done.Load(), "RPC_ERROR": f.rpcErrors.Load()},
	}
}

func progressRequest() *plugin_pb.ExecuteJobRequest {
	return &plugin_pb.ExecuteJobRequest{Job: &plugin_pb.JobSpec{JobId: "job-1"}}
}

const testInterval = 5 * time.Millisecond

func TestProgressReporter_IdleTicksSendNothing(t *testing.T) {
	sender := &progressSender{}
	run := &fakeRun{}
	stop := startProgressReporter(progressRequest(), sender, run.summary, testInterval)
	time.Sleep(20 * testInterval)
	stop()
	require.Empty(t, sender.sent())
}

func TestProgressReporter_ReportsEachKindOfWork(t *testing.T) {
	sender := &progressSender{}
	run := &fakeRun{}
	stop := startProgressReporter(progressRequest(), sender, run.summary, testInterval)
	defer stop()

	bumps := []*atomic.Int64{&run.entries, &run.events, &run.done, &run.rpcErrors}
	for i, counter := range bumps {
		counter.Add(1)
		require.Eventually(t, func() bool { return len(sender.sent()) == i+1 }, time.Second, testInterval)
	}
	time.Sleep(10 * testInterval)
	updates := sender.sent()
	require.Len(t, updates, len(bumps), "unchanged work must not refresh the job")

	last := updates[len(updates)-1]
	require.Equal(t, "job-1", last.JobId)
	require.Equal(t, jobType, last.JobType)
	require.Equal(t, plugin_pb.JobState_JOB_STATE_RUNNING, last.State)
	require.Equal(t, "running", last.Stage)
	require.Contains(t, last.Message, "last_phase=walk last_bucket=b1")
	require.Contains(t, last.Message, "entries=1")
	require.Contains(t, last.Message, "events=1")
	require.Contains(t, last.Message, "rpc_done=1")
	require.Contains(t, last.Message, "rpc_errors=1")
}

func TestProgressReporter_RetriesAFailedSendWithoutNewWork(t *testing.T) {
	sender := &progressSender{failures: 2}
	run := &fakeRun{}
	stop := startProgressReporter(progressRequest(), sender, run.summary, testInterval)
	defer stop()

	run.entries.Add(3)
	require.Eventually(t, func() bool { return len(sender.sent()) == 1 }, time.Second, testInterval)
	require.Equal(t, 3, sender.tries())
	time.Sleep(10 * testInterval)
	require.Equal(t, 3, sender.tries(), "a delivered report must not be resent")
	require.Contains(t, sender.sent()[0].Message, "entries=3")
}

func TestProgressReporter_NoProgressAfterStop(t *testing.T) {
	sender := &progressSender{}
	run := &fakeRun{}
	stop := startProgressReporter(progressRequest(), sender, run.summary, testInterval)
	run.done.Add(1)
	require.Eventually(t, func() bool { return len(sender.sent()) == 1 }, time.Second, testInterval)

	stop()
	tries := sender.tries()
	run.done.Add(1)
	run.entries.Add(1)
	time.Sleep(10 * testInterval)
	require.Equal(t, tries, sender.tries())
}
