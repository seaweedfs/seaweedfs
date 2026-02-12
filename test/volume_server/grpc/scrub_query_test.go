package volume_server_grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestScrubVolumeIndexAndUnsupportedMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(61)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubVolume index mode failed: %v", err)
	}
	if indexResp.GetTotalVolumes() != 1 {
		t.Fatalf("ScrubVolume expected total_volumes=1, got %d", indexResp.GetTotalVolumes())
	}

	_, err = grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode(99),
	})
	if err == nil {
		t.Fatalf("ScrubVolume should fail for unsupported mode")
	}
	if !strings.Contains(err.Error(), "unsupported volume scrub mode") {
		t.Fatalf("ScrubVolume unsupported mode error mismatch: %v", err)
	}
}

func TestScrubEcVolumeMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{98765},
		Mode:      volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err == nil {
		t.Fatalf("ScrubEcVolume should fail for missing EC volume")
	}
	if !strings.Contains(err.Error(), "EC volume id") {
		t.Fatalf("ScrubEcVolume missing-volume error mismatch: %v", err)
	}
}

func TestQueryInvalidAndMissingFileIDPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	invalidStream, err := grpcClient.Query(ctx, &volume_server_pb.QueryRequest{
		FromFileIds: []string{"bad-fid"},
		Selections:  []string{"name"},
		Filter:      &volume_server_pb.QueryRequest_Filter{},
		InputSerialization: &volume_server_pb.QueryRequest_InputSerialization{
			JsonInput: &volume_server_pb.QueryRequest_InputSerialization_JSONInput{},
		},
	})
	if err == nil {
		_, err = invalidStream.Recv()
	}
	if err == nil {
		t.Fatalf("Query should fail for invalid file id")
	}

	missingFid := framework.NewFileID(98766, 1, 1)
	missingStream, err := grpcClient.Query(ctx, &volume_server_pb.QueryRequest{
		FromFileIds: []string{missingFid},
		Selections:  []string{"name"},
		Filter:      &volume_server_pb.QueryRequest_Filter{},
		InputSerialization: &volume_server_pb.QueryRequest_InputSerialization{
			JsonInput: &volume_server_pb.QueryRequest_InputSerialization_JSONInput{},
		},
	})
	if err == nil {
		_, err = missingStream.Recv()
	}
	if err == nil {
		t.Fatalf("Query should fail for missing file id volume")
	}
}
