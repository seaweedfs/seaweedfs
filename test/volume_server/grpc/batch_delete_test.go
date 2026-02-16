package volume_server_grpc_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestBatchDeleteInvalidFidAndMaintenanceMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{"bad-fid"}})
	if err != nil {
		t.Fatalf("BatchDelete invalid fid should return response, got error: %v", err)
	}
	if len(resp.GetResults()) != 1 {
		t.Fatalf("expected one batch delete result, got %d", len(resp.GetResults()))
	}
	if got := resp.GetResults()[0].GetStatus(); got != 400 {
		t.Fatalf("invalid fid expected status 400, got %d", got)
	}

	stateResp, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{Maintenance: true, Version: stateResp.GetState().GetVersion()},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	_, err = client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{"1,1234567890ab"}})
	if err == nil {
		t.Fatalf("BatchDelete should fail when maintenance mode is enabled")
	}
	if !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("expected maintenance mode error, got: %v", err)
	}
}

func TestBatchDeleteCookieMismatchAndSkipCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(31)
	const needleID = uint64(900001)
	const correctCookie = uint32(0x1122AABB)
	const wrongCookie = uint32(0x1122AABC)
	framework.AllocateVolume(t, client, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, correctCookie)
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), fid, []byte("batch-delete-cookie-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	wrongCookieFid := framework.NewFileID(volumeID, needleID, wrongCookie)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mismatchResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{wrongCookieFid},
		SkipCookieCheck: false,
	})
	if err != nil {
		t.Fatalf("BatchDelete with cookie check failed: %v", err)
	}
	if len(mismatchResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete cookie mismatch expected 1 result, got %d", len(mismatchResp.GetResults()))
	}
	if mismatchResp.GetResults()[0].GetStatus() != http.StatusBadRequest {
		t.Fatalf("BatchDelete cookie mismatch expected status 400, got %d", mismatchResp.GetResults()[0].GetStatus())
	}

	skipCheckResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{wrongCookieFid},
		SkipCookieCheck: true,
	})
	if err != nil {
		t.Fatalf("BatchDelete skip cookie check failed: %v", err)
	}
	if len(skipCheckResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete skip check expected 1 result, got %d", len(skipCheckResp.GetResults()))
	}
	if skipCheckResp.GetResults()[0].GetStatus() != http.StatusAccepted {
		t.Fatalf("BatchDelete skip check expected status 202, got %d", skipCheckResp.GetResults()[0].GetStatus())
	}

	readAfterDelete := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, readAfterDelete)
	if readAfterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("read after skip-check batch delete expected 404, got %d", readAfterDelete.StatusCode)
	}
}

func TestBatchDeleteMixedStatusesAndMismatchStopsProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(32)
	framework.AllocateVolume(t, client, volumeID, "")

	const needleA = uint64(910001)
	const needleB = uint64(910002)
	const needleC = uint64(910003)
	const cookieA = uint32(0x11111111)
	const cookieB = uint32(0x22222222)
	const cookieC = uint32(0x33333333)

	httpClient := framework.NewHTTPClient()
	fidA := framework.NewFileID(volumeID, needleA, cookieA)
	fidB := framework.NewFileID(volumeID, needleB, cookieB)
	fidC := framework.NewFileID(volumeID, needleC, cookieC)

	for _, tc := range []struct {
		fid  string
		body string
	}{
		{fid: fidA, body: "batch-delete-mixed-a"},
		{fid: fidB, body: "batch-delete-mixed-b"},
		{fid: fidC, body: "batch-delete-mixed-c"},
	} {
		uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), tc.fid, []byte(tc.body))
		_ = framework.ReadAllAndClose(t, uploadResp)
		if uploadResp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s expected 201, got %d", tc.fid, uploadResp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	missingFid := framework.NewFileID(volumeID, 919999, 0x44444444)
	mixedResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds: []string{"bad-fid", fidA, missingFid},
	})
	if err != nil {
		t.Fatalf("BatchDelete mixed status request failed: %v", err)
	}
	if len(mixedResp.GetResults()) != 3 {
		t.Fatalf("BatchDelete mixed status expected 3 results, got %d", len(mixedResp.GetResults()))
	}
	if mixedResp.GetResults()[0].GetStatus() != http.StatusBadRequest {
		t.Fatalf("BatchDelete mixed result[0] expected 400, got %d", mixedResp.GetResults()[0].GetStatus())
	}
	if mixedResp.GetResults()[1].GetStatus() != http.StatusAccepted {
		t.Fatalf("BatchDelete mixed result[1] expected 202, got %d", mixedResp.GetResults()[1].GetStatus())
	}
	if mixedResp.GetResults()[2].GetStatus() != http.StatusNotFound {
		t.Fatalf("BatchDelete mixed result[2] expected 404, got %d", mixedResp.GetResults()[2].GetStatus())
	}

	readDeletedA := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fidA)
	_ = framework.ReadAllAndClose(t, readDeletedA)
	if readDeletedA.StatusCode != http.StatusNotFound {
		t.Fatalf("fidA should be deleted after batch delete, got status %d", readDeletedA.StatusCode)
	}

	wrongCookieB := framework.NewFileID(volumeID, needleB, cookieB+1)
	stopResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds: []string{wrongCookieB, fidC},
	})
	if err != nil {
		t.Fatalf("BatchDelete mismatch-stop request failed: %v", err)
	}
	if len(stopResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete mismatch-stop expected 1 result due early break, got %d", len(stopResp.GetResults()))
	}
	if stopResp.GetResults()[0].GetStatus() != http.StatusBadRequest {
		t.Fatalf("BatchDelete mismatch-stop expected 400, got %d", stopResp.GetResults()[0].GetStatus())
	}

	readB := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fidB)
	_ = framework.ReadAllAndClose(t, readB)
	if readB.StatusCode != http.StatusOK {
		t.Fatalf("fidB should remain after cookie mismatch path, got %d", readB.StatusCode)
	}

	readC := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fidC)
	_ = framework.ReadAllAndClose(t, readC)
	if readC.StatusCode != http.StatusOK {
		t.Fatalf("fidC should remain when batch processing stops on mismatch, got %d", readC.StatusCode)
	}
}

func TestBatchDeleteRejectsChunkManifestNeedles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(33)
	framework.AllocateVolume(t, client, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 920001, 0x5555AAAA)
	req, err := http.NewRequest(http.MethodPost, cluster.VolumeAdminURL()+"/"+fid+"?cm=true", bytes.NewReader([]byte("manifest-placeholder-payload")))
	if err != nil {
		t.Fatalf("create chunk manifest upload request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	uploadResp := framework.DoRequest(t, httpClient, req)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("chunk manifest upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{fid}})
	if err != nil {
		t.Fatalf("BatchDelete chunk manifest should return response, got grpc error: %v", err)
	}
	if len(resp.GetResults()) != 1 {
		t.Fatalf("BatchDelete chunk manifest expected one result, got %d", len(resp.GetResults()))
	}
	if resp.GetResults()[0].GetStatus() != http.StatusNotAcceptable {
		t.Fatalf("BatchDelete chunk manifest expected status 406, got %d", resp.GetResults()[0].GetStatus())
	}
	if !strings.Contains(resp.GetResults()[0].GetError(), "ChunkManifest") {
		t.Fatalf("BatchDelete chunk manifest expected error mentioning ChunkManifest, got %q", resp.GetResults()[0].GetError())
	}

	readResp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("chunk manifest should not be deleted by BatchDelete reject path, got %d", readResp.StatusCode)
	}
}

func TestBatchDeleteEcNeedleSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	if stopProxy := maybeStartGrpcOffsetProxy(t, cluster.VolumeAdminAddress(), cluster.VolumeGRPCAddress()); stopProxy != nil {
		t.Cleanup(stopProxy)
	}

	const volumeID = uint32(34)
	const needleID = uint64(930001)
	const cookie = uint32(0x6677BBCC)
	framework.AllocateVolume(t, client, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, cookie)
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), fid, []byte("batch-delete-ec-success"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsGenerate failed: %v", err)
	}

	_, err = client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: "",
		ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount failed: %v", err)
	}

	deleteResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{fid}})
	if err != nil {
		t.Fatalf("BatchDelete EC needle should return response, got grpc error: %v", err)
	}
	if len(deleteResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete EC needle expected one result, got %d", len(deleteResp.GetResults()))
	}
	if deleteResp.GetResults()[0].GetStatus() != http.StatusAccepted {
		t.Fatalf("BatchDelete EC needle expected status 202, got %d error=%q", deleteResp.GetResults()[0].GetStatus(), deleteResp.GetResults()[0].GetError())
	}
	if deleteResp.GetResults()[0].GetSize() == 0 {
		t.Fatalf("BatchDelete EC needle expected non-zero deleted size")
	}

	deletedStream, err := client.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: volumeID,
		ShardId:  0,
		FileKey:  needleID,
		Offset:   0,
		Size:     1,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardRead deleted-check start failed: %v", err)
	}
	deletedMsg, err := deletedStream.Recv()
	if err != nil {
		t.Fatalf("VolumeEcShardRead deleted-check recv failed: %v", err)
	}
	if !deletedMsg.GetIsDeleted() {
		t.Fatalf("VolumeEcShardRead expected IsDeleted=true after EC batch delete")
	}
}

func maybeStartGrpcOffsetProxy(t testing.TB, adminAddr, actualGrpcAddr string) func() {
	t.Helper()

	host, portText, err := net.SplitHostPort(adminAddr)
	if err != nil {
		t.Fatalf("split admin address %q: %v", adminAddr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse admin port %q: %v", portText, err)
	}

	expectedGrpcAddr := net.JoinHostPort(host, strconv.Itoa(port+10000))
	if expectedGrpcAddr == actualGrpcAddr {
		return nil
	}

	listener, err := net.Listen("tcp", expectedGrpcAddr)
	if err != nil {
		t.Fatalf("listen grpc-offset proxy %s -> %s: %v", expectedGrpcAddr, actualGrpcAddr, err)
	}

	stopped := make(chan struct{})
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				select {
				case <-stopped:
					return
				default:
					continue
				}
			}
			go proxyBidirectional(conn, actualGrpcAddr)
		}
	}()

	return func() {
		close(stopped)
		_ = listener.Close()
	}
}

func proxyBidirectional(inbound net.Conn, targetAddr string) {
	outbound, err := net.Dial("tcp", targetAddr)
	if err != nil {
		_ = inbound.Close()
		return
	}

	go func() {
		_, _ = io.Copy(outbound, inbound)
		_ = outbound.Close()
	}()
	_, _ = io.Copy(inbound, outbound)
	_ = inbound.Close()
}
