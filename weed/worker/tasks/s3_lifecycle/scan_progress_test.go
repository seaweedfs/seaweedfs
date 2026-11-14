package s3_lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestEntries(count int) []*bootstrap.Entry {
	entries := make([]*bootstrap.Entry, count)
	for i := 0; i < count; i++ {
		entries[i] = &bootstrap.Entry{
			Path:     fmt.Sprintf("obj-%04d", i),
			ModTime:  time.Now().Add(-10 * 24 * time.Hour),
			Size:     1024,
			IsLatest: true,
		}
	}
	return entries
}

func TestScanProgressReporter_BatchThresholdDispatchesProgress(t *testing.T) {
	sender := &recordingExecSender{}
	req := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "job-batch-test",
			JobType: jobType,
		},
	}

	reporter := newScanProgressReporter(sender, req, 10, 1*time.Hour)
	entries := makeTestEntries(25)
	rawList := bootstrap.EntryCallback(entries)

	wrapped := reporter.wrapListFunc(rawList)
	err := wrapped(context.Background(), "my-bucket", "", func(e *bootstrap.Entry) error {
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, int64(25), reporter.TotalScanned())

	// Batch size is 10, 25 entries processed.
	// Progress updates at entry 10, entry 20, and bucket completion (since bucketScanned >= batchSize).
	require.NotEmpty(t, sender.progress)
	assert.GreaterOrEqual(t, len(sender.progress), 2)

	first := sender.progress[0]
	assert.Equal(t, "job-batch-test", first.JobId)
	assert.Equal(t, jobType, first.JobType)
	assert.Equal(t, plugin_pb.JobState_JOB_STATE_RUNNING, first.State)
	assert.Equal(t, "scanning", first.Stage)
	assert.Contains(t, first.Message, "my-bucket")
	assert.Contains(t, first.Message, "10 entries scanned")

	second := sender.progress[1]
	assert.Equal(t, "job-batch-test", second.JobId)
	assert.Equal(t, "scanning", second.Stage)
	assert.Contains(t, second.Message, "20 entries scanned")
}

func TestScanProgressReporter_TimeIntervalDispatchesProgress(t *testing.T) {
	sender := &recordingExecSender{}
	req := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "job-interval-test",
			JobType: jobType,
		},
	}

	mockTime := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	reporter := newScanProgressReporter(sender, req, 1000, 10*time.Second)
	reporter.now = func() time.Time { return mockTime }
	reporter.lastReport = mockTime

	entries := makeTestEntries(5)
	rawList := bootstrap.EntryCallback(entries)
	wrapped := reporter.wrapListFunc(rawList)

	callCount := 0
	err := wrapped(context.Background(), "time-bucket", "", func(e *bootstrap.Entry) error {
		callCount++
		if callCount == 3 {
			// Advance time past the 10-second interval before entry 3 is counted
			mockTime = mockTime.Add(15 * time.Second)
		}
		return nil
	})
	require.NoError(t, err)

	// Even though batchSize (1000) was not reached, the 10-second interval elapsed,
	// so progress was dispatched.
	require.NotEmpty(t, sender.progress)
	foundIntervalUpdate := false
	for _, p := range sender.progress {
		if p.Stage == "scanning" && p.JobId == "job-interval-test" {
			foundIntervalUpdate = true
			break
		}
	}
	assert.True(t, foundIntervalUpdate, "expected at least one progress update due to elapsed time interval")
}

func TestScanProgressReporter_AccumulatesAcrossBuckets(t *testing.T) {
	sender := &recordingExecSender{}
	req := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "job-multi-bucket",
			JobType: jobType,
		},
	}

	// Batch size of 5
	reporter := newScanProgressReporter(sender, req, 5, 1*time.Hour)

	bucket1Entries := makeTestEntries(3)
	bucket2Entries := makeTestEntries(4)

	wrapped1 := reporter.wrapListFunc(bootstrap.EntryCallback(bucket1Entries))
	wrapped2 := reporter.wrapListFunc(bootstrap.EntryCallback(bucket2Entries))

	require.NoError(t, wrapped1(context.Background(), "bucket-1", "", func(e *bootstrap.Entry) error { return nil }))
	assert.Equal(t, int64(3), reporter.TotalScanned())

	require.NoError(t, wrapped2(context.Background(), "bucket-2", "", func(e *bootstrap.Entry) error { return nil }))
	assert.Equal(t, int64(7), reporter.TotalScanned())

	// Entry 2 in bucket-2 hit totalScanned=5, which triggered a progress update
	require.NotEmpty(t, sender.progress)
	foundTotal5 := false
	for _, p := range sender.progress {
		if p.Stage == "scanning" && p.Message == "scanning bucket bucket-2: 2 entries scanned (5 total)" {
			foundTotal5 = true
			break
		}
	}
	assert.True(t, foundTotal5, "expected progress update for totalScanned=5 in bucket-2")
}

func TestScanProgressReporter_PropagatesCallbackError(t *testing.T) {
	sender := &recordingExecSender{}
	reporter := newScanProgressReporter(sender, nil, 10, 1*time.Hour)

	expectedErr := errors.New("callback abort")
	entries := makeTestEntries(5)
	wrapped := reporter.wrapListFunc(bootstrap.EntryCallback(entries))

	err := wrapped(context.Background(), "err-bucket", "", func(e *bootstrap.Entry) error {
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
}

func TestScanProgressReporter_NilGuards(t *testing.T) {
	var nilReporter *scanProgressReporter
	rawList := bootstrap.EntryCallback(makeTestEntries(1))
	assert.NotNil(t, nilReporter.wrapListFunc(rawList))

	reporter := newScanProgressReporter(nil, nil, 0, 0)
	assert.Nil(t, reporter.wrapListFunc(nil))
	assert.Equal(t, int64(defaultScanProgressBatchSize), reporter.batchSize)
	assert.Equal(t, defaultScanProgressInterval, reporter.minInterval)
}

type testRecordingDispatcher struct {
	deleted []*bootstrap.Entry
}

func (d *testRecordingDispatcher) Delete(_ context.Context, _ *engine.CompiledAction, entry *bootstrap.Entry) error {
	d.deleted = append(d.deleted, entry)
	return nil
}

func TestScanProgressReporter_WalkBucketsIntegration(t *testing.T) {
	sender := &recordingExecSender{}
	req := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "job-walk-test",
			JobType: jobType,
		},
	}

	reporter := newScanProgressReporter(sender, req, 5, 1*time.Hour)
	entries := makeTestEntries(15)

	rawList := bootstrap.EntryCallback(entries)
	wrappedList := reporter.wrapListFunc(rawList)

	eng := engine.New()
	eng.Compile(nil, engine.CompileOptions{})
	snap := eng.Snapshot()

	dispatcher := &testRecordingDispatcher{}
	shards := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	err := dailyrun.WalkBuckets(context.Background(), snap, shards, []string{"integrated-bucket"}, wrappedList, dispatcher)
	require.NoError(t, err)

	assert.Equal(t, int64(15), reporter.TotalScanned())
	require.NotEmpty(t, sender.progress)

	for _, p := range sender.progress {
		assert.Equal(t, "job-walk-test", p.JobId)
		assert.Equal(t, jobType, p.JobType)
		assert.Equal(t, plugin_pb.JobState_JOB_STATE_RUNNING, p.State)
		assert.Equal(t, "scanning", p.Stage)
	}
}
