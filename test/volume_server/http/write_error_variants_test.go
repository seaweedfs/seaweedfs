package volume_server_http_test

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestWriteInvalidVidAndFidReturnBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	invalidVidReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/invalid,12345678", []byte("x"))
	invalidVidResp := framework.DoRequest(t, client, invalidVidReq)
	_ = framework.ReadAllAndClose(t, invalidVidResp)
	if invalidVidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("write with invalid vid expected 400, got %d", invalidVidResp.StatusCode)
	}

	invalidFidReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/1,bad", []byte("x"))
	invalidFidResp := framework.DoRequest(t, client, invalidFidReq)
	_ = framework.ReadAllAndClose(t, invalidFidResp)
	if invalidFidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("write with invalid fid expected 400, got %d", invalidFidResp.StatusCode)
	}
}

func TestWriteMalformedMultipartAndMD5Mismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(98)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 772001, 0x1A2B3C4D)

	malformedMultipartReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+fid, strings.NewReader("not-a-valid-multipart-body"))
	if err != nil {
		t.Fatalf("create malformed multipart request: %v", err)
	}
	malformedMultipartReq.Header.Set("Content-Type", "multipart/form-data")
	malformedMultipartResp := framework.DoRequest(t, client, malformedMultipartReq)
	malformedMultipartBody := framework.ReadAllAndClose(t, malformedMultipartResp)
	if malformedMultipartResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed multipart write expected 400, got %d", malformedMultipartResp.StatusCode)
	}
	if !strings.Contains(strings.ToLower(string(malformedMultipartBody)), "boundary") {
		t.Fatalf("malformed multipart response should mention boundary parse failure, got %q", string(malformedMultipartBody))
	}

	md5MismatchReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, []byte("content-md5-mismatch-body"))
	md5MismatchReq.Header.Set("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")
	md5MismatchResp := framework.DoRequest(t, client, md5MismatchReq)
	md5MismatchBody := framework.ReadAllAndClose(t, md5MismatchResp)
	if md5MismatchResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("content-md5 mismatch write expected 400, got %d", md5MismatchResp.StatusCode)
	}
	if !strings.Contains(string(md5MismatchBody), "Content-MD5") {
		t.Fatalf("content-md5 mismatch response should mention Content-MD5, got %q", string(md5MismatchBody))
	}
}

func TestWriteReadOnlyReportsConflictAndSafeRefusal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, volume := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()
	const volumeID = uint32(99)
	framework.AllocateVolume(t, volume, volumeID, "")
	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 773001, 0x1A2B3C4D)
	payload := []byte("existing bytes must survive a refused overwrite")
	response := framework.DoRequest(t, client, newUploadRequest(t, cluster.VolumeAdminURL()+"/"+fid, payload))
	body := framework.ReadAllAndClose(t, response)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("initial upload: %d %s", response.StatusCode, body)
	}
	if _, err := volume.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	dat := filepath.Join(cluster.BaseDir(), "volume", "99.dat")
	before, err := os.Stat(dat)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"", "?fsync=true", "?type=replicate"} {
		t.Run(query, func(t *testing.T) {
			response := framework.DoRequest(t, client, newUploadRequest(t, cluster.VolumeAdminURL()+"/"+fid+query, []byte("refused replacement")))
			body := framework.ReadAllAndClose(t, response)
			if response.StatusCode != http.StatusConflict {
				t.Fatalf("read-only upload: %d %s", response.StatusCode, body)
			}
			want := ""
			if query == "" {
				want = "refused-before-append"
			}
			if got := response.Header.Get("X-Seaweed-Write-Outcome"); got != want {
				t.Fatalf("write outcome: %q, want %q", got, want)
			}
		})
	}
	after, err := os.Stat(dat)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("refused write grew .dat from %d to %d", before.Size(), after.Size())
	}
	response = framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/"+fid))
	body = framework.ReadAllAndClose(t, response)
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("existing data changed: %d %q", response.StatusCode, body)
	}
	if _, err := volume.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	response = framework.DoRequest(t, client, newUploadRequest(t, cluster.VolumeAdminURL()+"/"+fid, []byte("writable again")))
	body = framework.ReadAllAndClose(t, response)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("unsealed upload: %d %s", response.StatusCode, body)
	}
	if got := response.Header.Get("X-Seaweed-Write-Outcome"); got != "" {
		t.Fatalf("successful write marked refused: %q", got)
	}
}
