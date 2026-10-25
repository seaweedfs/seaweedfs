package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

const overflowingNeedleDeltaFid = "1,ffffffffffffffff00000000_1"

func TestDeleteHandlerRejectsOverflowingNeedleDelta(t *testing.T) {
	req := httptest.NewRequest(http.MethodDelete, "/"+overflowingNeedleDeltaFid, nil)
	resp := httptest.NewRecorder()

	(&VolumeServer{}).DeleteHandler(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("DeleteHandler returned status %d, want %d", resp.Code, http.StatusBadRequest)
	}
}

func TestBatchDeleteRejectsOverflowingNeedleDelta(t *testing.T) {
	resp, err := (&VolumeServer{}).BatchDelete(context.Background(), &volume_server_pb.BatchDeleteRequest{
		FileIds: []string{overflowingNeedleDeltaFid},
	})
	if err != nil {
		t.Fatalf("BatchDelete returned error: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Status != http.StatusBadRequest {
		t.Fatalf("BatchDelete returned results %+v, want one bad request", resp.Results)
	}
}

func TestQueryRejectsOverflowingNeedleDelta(t *testing.T) {
	err := (&VolumeServer{}).Query(&volume_server_pb.QueryRequest{
		FromFileIds: []string{overflowingNeedleDeltaFid},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "overflows needle id") {
		t.Fatalf("Query returned error %v, want needle id overflow", err)
	}
}
