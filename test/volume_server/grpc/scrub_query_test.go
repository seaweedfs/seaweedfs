package volume_server_grpc_test

import (
	"context"
	"io"
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

func TestScrubEcVolumeAutoSelectNoEcVolumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		Mode: volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume auto-select failed: %v", err)
	}
	if resp.GetTotalVolumes() != 0 {
		t.Fatalf("ScrubEcVolume auto-select expected total_volumes=0 without EC data, got %d", resp.GetTotalVolumes())
	}
	if len(resp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("ScrubEcVolume auto-select expected no broken volumes, got %v", resp.GetBrokenVolumeIds())
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

func TestScrubVolumeAutoSelectAndNotImplementedModes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeIDA = uint32(62)
	const volumeIDB = uint32(63)
	framework.AllocateVolume(t, grpcClient, volumeIDA, "")
	framework.AllocateVolume(t, grpcClient, volumeIDB, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	autoResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		Mode: volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubVolume auto-select failed: %v", err)
	}
	if autoResp.GetTotalVolumes() < 2 {
		t.Fatalf("ScrubVolume auto-select expected at least 2 volumes, got %d", autoResp.GetTotalVolumes())
	}

	localResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeIDA},
		Mode:      volume_server_pb.VolumeScrubMode_LOCAL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume local mode failed: %v", err)
	}
	if localResp.GetTotalVolumes() != 1 {
		t.Fatalf("ScrubVolume local mode expected total_volumes=1, got %d", localResp.GetTotalVolumes())
	}
	if len(localResp.GetBrokenVolumeIds()) != 1 || localResp.GetBrokenVolumeIds()[0] != volumeIDA {
		t.Fatalf("ScrubVolume local mode expected broken volume %d, got %v", volumeIDA, localResp.GetBrokenVolumeIds())
	}
	if len(localResp.GetDetails()) == 0 || !strings.Contains(strings.Join(localResp.GetDetails(), " "), "not implemented") {
		t.Fatalf("ScrubVolume local mode expected not-implemented details, got %v", localResp.GetDetails())
	}

	fullResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeIDA},
		Mode:      volume_server_pb.VolumeScrubMode_FULL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume full mode failed: %v", err)
	}
	if fullResp.GetTotalVolumes() != 1 {
		t.Fatalf("ScrubVolume full mode expected total_volumes=1, got %d", fullResp.GetTotalVolumes())
	}
	if len(fullResp.GetDetails()) == 0 || !strings.Contains(strings.Join(fullResp.GetDetails(), " "), "not implemented") {
		t.Fatalf("ScrubVolume full mode expected not-implemented details, got %v", fullResp.GetDetails())
	}
}

func TestQueryJsonSuccessAndCsvNoOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(64)
	const needleID = uint64(777001)
	const cookie = uint32(0xAABBCCDD)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	jsonLines := []byte("{\"score\":3}\n{\"score\":12}\n{\"score\":18}\n")
	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, cookie)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, jsonLines)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != 201 {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queryStream, err := grpcClient.Query(ctx, &volume_server_pb.QueryRequest{
		FromFileIds: []string{fid},
		Selections:  []string{"score"},
		Filter: &volume_server_pb.QueryRequest_Filter{
			Field:   "score",
			Operand: ">",
			Value:   "10",
		},
		InputSerialization: &volume_server_pb.QueryRequest_InputSerialization{
			JsonInput: &volume_server_pb.QueryRequest_InputSerialization_JSONInput{Type: "LINES"},
		},
	})
	if err != nil {
		t.Fatalf("Query json start failed: %v", err)
	}

	firstStripe, err := queryStream.Recv()
	if err != nil {
		t.Fatalf("Query json recv failed: %v", err)
	}
	records := string(firstStripe.GetRecords())
	if !strings.Contains(records, "score:12") || !strings.Contains(records, "score:18") {
		t.Fatalf("Query json records missing expected filtered scores: %q", records)
	}
	if strings.Contains(records, "score:3") {
		t.Fatalf("Query json records should not include filtered-out score: %q", records)
	}
	_, err = queryStream.Recv()
	if err != io.EOF {
		t.Fatalf("Query json expected EOF after first stripe, got: %v", err)
	}

	csvStream, err := grpcClient.Query(ctx, &volume_server_pb.QueryRequest{
		FromFileIds: []string{fid},
		Selections:  []string{"score"},
		Filter:      &volume_server_pb.QueryRequest_Filter{},
		InputSerialization: &volume_server_pb.QueryRequest_InputSerialization{
			CsvInput: &volume_server_pb.QueryRequest_InputSerialization_CSVInput{},
		},
	})
	if err != nil {
		t.Fatalf("Query csv start failed: %v", err)
	}
	_, err = csvStream.Recv()
	if err != io.EOF {
		t.Fatalf("Query csv expected EOF with no rows, got: %v", err)
	}
}
