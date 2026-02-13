package volume_server_http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/operation"
)

func TestChunkManifestExpansionAndBypass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(102)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()

	chunkFID := framework.NewFileID(volumeID, 772005, 0x5E6F7081)
	chunkPayload := []byte("chunk-manifest-expanded-content")
	chunkUploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), chunkFID, chunkPayload)
	_ = framework.ReadAllAndClose(t, chunkUploadResp)
	if chunkUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("chunk upload expected 201, got %d", chunkUploadResp.StatusCode)
	}

	manifest := &operation.ChunkManifest{
		Name: "manifest.bin",
		Mime: "application/octet-stream",
		Size: int64(len(chunkPayload)),
		Chunks: []*operation.ChunkInfo{
			{
				Fid:    chunkFID,
				Offset: 0,
				Size:   int64(len(chunkPayload)),
			},
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal chunk manifest: %v", err)
	}

	manifestFID := framework.NewFileID(volumeID, 772006, 0x6F708192)
	manifestUploadReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+manifestFID+"?cm=true", bytes.NewReader(manifestBytes))
	if err != nil {
		t.Fatalf("create manifest upload request: %v", err)
	}
	manifestUploadReq.Header.Set("Content-Type", "application/json")
	manifestUploadResp := framework.DoRequest(t, client, manifestUploadReq)
	_ = framework.ReadAllAndClose(t, manifestUploadResp)
	if manifestUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("manifest upload expected 201, got %d", manifestUploadResp.StatusCode)
	}

	expandedReadResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), manifestFID)
	expandedReadBody := framework.ReadAllAndClose(t, expandedReadResp)
	if expandedReadResp.StatusCode != http.StatusOK {
		t.Fatalf("manifest expanded read expected 200, got %d", expandedReadResp.StatusCode)
	}
	if string(expandedReadBody) != string(chunkPayload) {
		t.Fatalf("manifest expanded read mismatch: got %q want %q", string(expandedReadBody), string(chunkPayload))
	}
	if expandedReadResp.Header.Get("X-File-Store") != "chunked" {
		t.Fatalf("manifest expanded read expected X-File-Store=chunked, got %q", expandedReadResp.Header.Get("X-File-Store"))
	}

	bypassReadResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+manifestFID+"?cm=false"))
	bypassReadBody := framework.ReadAllAndClose(t, bypassReadResp)
	if bypassReadResp.StatusCode != http.StatusOK {
		t.Fatalf("manifest bypass read expected 200, got %d", bypassReadResp.StatusCode)
	}
	if bypassReadResp.Header.Get("X-File-Store") != "" {
		t.Fatalf("manifest bypass read expected empty X-File-Store header, got %q", bypassReadResp.Header.Get("X-File-Store"))
	}

	var gotManifest operation.ChunkManifest
	if err = json.Unmarshal(bypassReadBody, &gotManifest); err != nil {
		t.Fatalf("manifest bypass read expected JSON payload, got decode error: %v body=%q", err, string(bypassReadBody))
	}
	if len(gotManifest.Chunks) != 1 || gotManifest.Chunks[0].Fid != chunkFID {
		t.Fatalf("manifest bypass read payload mismatch: %+v", gotManifest)
	}
}

func TestChunkManifestDeleteRemovesChildChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(104)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()

	chunkFID := framework.NewFileID(volumeID, 772008, 0x8192A3B4)
	chunkPayload := []byte("chunk-manifest-delete-content")
	chunkUploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), chunkFID, chunkPayload)
	_ = framework.ReadAllAndClose(t, chunkUploadResp)
	if chunkUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("chunk upload expected 201, got %d", chunkUploadResp.StatusCode)
	}

	manifest := &operation.ChunkManifest{
		Name: "manifest-delete.bin",
		Mime: "application/octet-stream",
		Size: int64(len(chunkPayload)),
		Chunks: []*operation.ChunkInfo{
			{
				Fid:    chunkFID,
				Offset: 0,
				Size:   int64(len(chunkPayload)),
			},
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal chunk manifest: %v", err)
	}

	manifestFID := framework.NewFileID(volumeID, 772009, 0x92A3B4C5)
	manifestUploadReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+manifestFID+"?cm=true", bytes.NewReader(manifestBytes))
	if err != nil {
		t.Fatalf("create manifest upload request: %v", err)
	}
	manifestUploadReq.Header.Set("Content-Type", "application/json")
	manifestUploadResp := framework.DoRequest(t, client, manifestUploadReq)
	_ = framework.ReadAllAndClose(t, manifestUploadResp)
	if manifestUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("manifest upload expected 201, got %d", manifestUploadResp.StatusCode)
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+manifestFID))
	deleteBody := framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("manifest delete expected 202, got %d", deleteResp.StatusCode)
	}
	var deleteResult map[string]int64
	if err = json.Unmarshal(deleteBody, &deleteResult); err != nil {
		t.Fatalf("decode manifest delete response: %v body=%q", err, string(deleteBody))
	}
	if deleteResult["size"] != int64(len(chunkPayload)) {
		t.Fatalf("manifest delete expected size=%d, got %d", len(chunkPayload), deleteResult["size"])
	}

	manifestReadAfterDelete := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), manifestFID)
	_ = framework.ReadAllAndClose(t, manifestReadAfterDelete)
	if manifestReadAfterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("manifest read after delete expected 404, got %d", manifestReadAfterDelete.StatusCode)
	}

	chunkReadAfterDelete := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), chunkFID)
	_ = framework.ReadAllAndClose(t, chunkReadAfterDelete)
	if chunkReadAfterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("chunk read after manifest delete expected 404, got %d", chunkReadAfterDelete.StatusCode)
	}
}

func TestChunkManifestDeleteFailsWhenChildDeletionFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(105)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	manifest := &operation.ChunkManifest{
		Name: "manifest-delete-failure.bin",
		Mime: "application/octet-stream",
		Size: 1,
		Chunks: []*operation.ChunkInfo{
			{
				Fid:    "not-a-valid-fid",
				Offset: 0,
				Size:   1,
			},
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal chunk manifest: %v", err)
	}

	manifestFID := framework.NewFileID(volumeID, 772010, 0xA3B4C5D6)
	manifestUploadReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+manifestFID+"?cm=true", bytes.NewReader(manifestBytes))
	if err != nil {
		t.Fatalf("create manifest upload request: %v", err)
	}
	manifestUploadReq.Header.Set("Content-Type", "application/json")
	manifestUploadResp := framework.DoRequest(t, client, manifestUploadReq)
	_ = framework.ReadAllAndClose(t, manifestUploadResp)
	if manifestUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("manifest upload expected 201, got %d", manifestUploadResp.StatusCode)
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+manifestFID))
	deleteBody := framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("manifest delete with invalid child fid expected 500, got %d body=%q", deleteResp.StatusCode, string(deleteBody))
	}

	manifestBypassRead := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+manifestFID+"?cm=false"))
	manifestBypassBody := framework.ReadAllAndClose(t, manifestBypassRead)
	if manifestBypassRead.StatusCode != http.StatusOK {
		t.Fatalf("manifest bypass read after failed delete expected 200, got %d", manifestBypassRead.StatusCode)
	}
	var gotManifest operation.ChunkManifest
	if err = json.Unmarshal(manifestBypassBody, &gotManifest); err != nil {
		t.Fatalf("manifest bypass read expected JSON payload, got decode error: %v body=%q", err, string(manifestBypassBody))
	}
	if len(gotManifest.Chunks) != 1 || gotManifest.Chunks[0].Fid != "not-a-valid-fid" {
		t.Fatalf("manifest payload mismatch after failed delete: %+v", gotManifest)
	}
}
