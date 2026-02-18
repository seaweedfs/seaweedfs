package pluginworker

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestDummyStressDescriptorDefaults(t *testing.T) {
	t.Parallel()

	handler := NewDummyStressHandler(nil)
	descriptor := handler.Descriptor()
	if descriptor == nil {
		t.Fatalf("expected non-nil descriptor")
	}
	if descriptor.JobType != dummyStressJobType {
		t.Fatalf("unexpected job type: %s", descriptor.JobType)
	}
	if descriptor.AdminRuntimeDefaults == nil {
		t.Fatalf("expected admin runtime defaults")
	}
	if descriptor.AdminRuntimeDefaults.DetectionIntervalSeconds != 10 {
		t.Fatalf("unexpected detection interval default: %d", descriptor.AdminRuntimeDefaults.DetectionIntervalSeconds)
	}
}

func TestCollectDummyVolumeIDs(t *testing.T) {
	t.Parallel()

	response := buildDummyVolumeListResponseForTest()

	all := collectDummyVolumeIDs(response, "")
	assertUint32SliceEqual(t, all, []uint32{1, 2, 100})

	alphaOnly := collectDummyVolumeIDs(response, "alpha")
	assertUint32SliceEqual(t, alphaOnly, []uint32{1, 100})

	betaOnly := collectDummyVolumeIDs(response, "beta")
	assertUint32SliceEqual(t, betaOnly, []uint32{2})
}

func TestCollectDummyVolumeSnapshot(t *testing.T) {
	t.Parallel()

	response := buildDummyVolumeListResponseForTest()

	regular := collectDummyVolumeSnapshot(response, 1)
	if !regular.FoundRegular {
		t.Fatalf("expected regular volume snapshot to be found")
	}
	if regular.ReplicaCount != 2 {
		t.Fatalf("unexpected replica count: %d", regular.ReplicaCount)
	}
	if regular.ECShardCount != 2 {
		t.Fatalf("unexpected ec shard count for volume 1: %d", regular.ECShardCount)
	}

	ecOnly := collectDummyVolumeSnapshot(response, 100)
	if ecOnly.FoundRegular {
		t.Fatalf("did not expect regular volume for EC-only id")
	}
	if ecOnly.ECShardCount != 7 {
		t.Fatalf("unexpected ec shard count for volume 100: %d", ecOnly.ECShardCount)
	}
}

func TestDummyStressDetectAndExecute(t *testing.T) {
	t.Parallel()

	response := buildDummyVolumeListResponseForTest()
	handler := NewDummyStressHandler(nil)
	handler.fetchVolumeList = func(context.Context, []string) (*master_pb.VolumeListResponse, error) {
		return response, nil
	}
	handler.sleepWithContext = func(_ context.Context, duration time.Duration) error {
		if duration != dummyStressExecutionDelay {
			t.Fatalf("unexpected sleep duration: %v", duration)
		}
		return nil
	}

	detectSender := &dummyDetectCapture{}
	detectErr := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: dummyStressJobType,
		ClusterContext: &plugin_pb.ClusterContext{
			MasterGrpcAddresses: []string{"master-a:19333"},
		},
		MaxResults: 2,
	}, detectSender)
	if detectErr != nil {
		t.Fatalf("Detect error: %v", detectErr)
	}
	if detectSender.complete == nil || !detectSender.complete.Success {
		t.Fatalf("expected successful detection completion")
	}
	if len(detectSender.proposals) != 2 {
		t.Fatalf("expected 2 proposals, got %d", len(detectSender.proposals))
	}
	if firstVolumeID := readInt64Config(detectSender.proposals[0].Parameters, "volume_id", 0); firstVolumeID != 1 {
		t.Fatalf("unexpected first proposal volume id: %d", firstVolumeID)
	}
	if secondVolumeID := readInt64Config(detectSender.proposals[1].Parameters, "volume_id", 0); secondVolumeID != 2 {
		t.Fatalf("unexpected second proposal volume id: %d", secondVolumeID)
	}
	if len(detectSender.activities) == 0 {
		t.Fatalf("expected detector activities")
	}

	execSender := &dummyExecCapture{}
	execErr := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "dummy-job-1",
			JobType: dummyStressJobType,
			Parameters: map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1},
				},
			},
		},
		ClusterContext: &plugin_pb.ClusterContext{
			MasterGrpcAddresses: []string{"master-a:19333"},
		},
	}, execSender)
	if execErr != nil {
		t.Fatalf("Execute error: %v", execErr)
	}
	if execSender.completed == nil || !execSender.completed.Success {
		t.Fatalf("expected successful execution completion")
	}
	if len(execSender.progress) < 2 {
		t.Fatalf("expected progress updates, got %d", len(execSender.progress))
	}
	if volumeID := readInt64Config(execSender.completed.GetResult().GetOutputValues(), "volume_id", 0); volumeID != 1 {
		t.Fatalf("unexpected completion volume_id: %d", volumeID)
	}
	if shardCount := readInt64Config(execSender.completed.GetResult().GetOutputValues(), "ec_shard_count", 0); shardCount <= 0 {
		t.Fatalf("expected ec_shard_count > 0, got %d", shardCount)
	}
}

func TestDummyStressDetectStressLargeTopology(t *testing.T) {
	t.Parallel()

	const volumeCount = 5000
	const maxResults = 1200

	response := buildLargeDummyVolumeListResponse(volumeCount)
	handler := NewDummyStressHandler(nil)
	handler.fetchVolumeList = func(context.Context, []string) (*master_pb.VolumeListResponse, error) {
		return response, nil
	}

	detectSender := &dummyDetectCapture{}
	detectErr := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:    dummyStressJobType,
		MaxResults: maxResults,
		ClusterContext: &plugin_pb.ClusterContext{
			MasterGrpcAddresses: []string{"master-a:19333"},
		},
	}, detectSender)
	if detectErr != nil {
		t.Fatalf("Detect error: %v", detectErr)
	}
	if detectSender.complete == nil || !detectSender.complete.Success {
		t.Fatalf("expected successful detection completion")
	}
	if len(detectSender.proposals) != maxResults {
		t.Fatalf("expected %d proposals, got %d", maxResults, len(detectSender.proposals))
	}
	if detectSender.complete.TotalProposals != maxResults {
		t.Fatalf("unexpected total proposals in completion: %d", detectSender.complete.TotalProposals)
	}
	if len(detectSender.activities) == 0 {
		t.Fatalf("expected detector activities")
	}
}

type dummyDetectCapture struct {
	proposals  []*plugin_pb.JobProposal
	complete   *plugin_pb.DetectionComplete
	activities []*plugin_pb.ActivityEvent
}

func (c *dummyDetectCapture) SendProposals(response *plugin_pb.DetectionProposals) error {
	if response != nil {
		c.proposals = append(c.proposals, response.Proposals...)
	}
	return nil
}

func (c *dummyDetectCapture) SendComplete(response *plugin_pb.DetectionComplete) error {
	c.complete = response
	return nil
}

func (c *dummyDetectCapture) SendActivity(event *plugin_pb.ActivityEvent) error {
	c.activities = append(c.activities, event)
	return nil
}

type dummyExecCapture struct {
	progress  []*plugin_pb.JobProgressUpdate
	completed *plugin_pb.JobCompleted
}

func (c *dummyExecCapture) SendProgress(update *plugin_pb.JobProgressUpdate) error {
	c.progress = append(c.progress, update)
	return nil
}

func (c *dummyExecCapture) SendCompleted(done *plugin_pb.JobCompleted) error {
	c.completed = done
	return nil
}

func buildDummyVolumeListResponseForTest() *master_pb.VolumeListResponse {
	return &master_pb.VolumeListResponse{
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc1",
					RackInfos: []*master_pb.RackInfo{
						{
							Id: "rack1",
							DataNodeInfos: []*master_pb.DataNodeInfo{
								{
									Id: "dn1",
									DiskInfos: map[string]*master_pb.DiskInfo{
										"disk-a": {
											VolumeInfos: []*master_pb.VolumeInformationMessage{
												{Id: 1, Collection: "alpha", Size: 1000, DeletedByteCount: 30},
												{Id: 2, Collection: "beta", Size: 2000, DeletedByteCount: 100},
											},
											EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
												{Id: 1, Collection: "alpha", EcIndexBits: 3},
												{Id: 100, Collection: "alpha", EcIndexBits: 7},
											},
										},
									},
								},
								{
									Id: "dn2",
									DiskInfos: map[string]*master_pb.DiskInfo{
										"disk-b": {
											VolumeInfos: []*master_pb.VolumeInformationMessage{
												{Id: 1, Collection: "alpha", Size: 1000, DeletedByteCount: 40},
											},
											EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
												{Id: 100, Collection: "alpha", EcIndexBits: 0, ShardSizes: []int64{10, 10, 10, 10}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildLargeDummyVolumeListResponse(volumeCount int) *master_pb.VolumeListResponse {
	volumeInfos := make([]*master_pb.VolumeInformationMessage, 0, volumeCount)
	for i := 1; i <= volumeCount; i++ {
		volumeInfos = append(volumeInfos, &master_pb.VolumeInformationMessage{
			Id:               uint32(i),
			Collection:       "stress",
			Size:             uint64(1024 * i),
			DeletedByteCount: uint64(i % 13),
		})
	}

	return &master_pb.VolumeListResponse{
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc-stress",
					RackInfos: []*master_pb.RackInfo{
						{
							Id: "rack-stress",
							DataNodeInfos: []*master_pb.DataNodeInfo{
								{
									Id: "dn-stress",
									DiskInfos: map[string]*master_pb.DiskInfo{
										"disk-stress": {
											VolumeInfos: volumeInfos,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func assertUint32SliceEqual(t *testing.T, actual []uint32, expected []uint32) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("unexpected slice length: got=%d want=%d values=%v", len(actual), len(expected), actual)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("unexpected value at index %d: got=%d want=%d actual=%v expected=%v", i, actual[i], expected[i], actual, expected)
		}
	}
}
