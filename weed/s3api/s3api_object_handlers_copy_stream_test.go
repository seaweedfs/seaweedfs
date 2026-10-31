package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestStreamCopyChunkRangeFsync(t *testing.T) {
	util_http.InitGlobalHttpClient()
	payload := bytes.Repeat([]byte("copy data"), 512)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer source.Close()

	for _, tc := range []struct {
		name      string
		fsync     bool
		wantQuery string
		status    int
		wantErr   bool
	}{
		{"without sync", false, "", http.StatusCreated, false},
		{"durable copy", true, "true", http.StatusCreated, false},
		{"durable write fails", true, "true", http.StatusInternalServerError, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("fsync"); got != tc.wantQuery {
					t.Errorf("upload fsync=%q, want %q", got, tc.wantQuery)
				}
				reader, err := r.MultipartReader()
				if err != nil {
					t.Error(err)
					http.Error(w, "invalid upload", http.StatusBadRequest)
					return
				}
				part, err := reader.NextPart()
				if err != nil {
					t.Error(err)
					http.Error(w, "missing data", http.StatusBadRequest)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil || !bytes.Equal(data, payload) {
					t.Errorf("copied payload differs: %v", err)
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprintf(w, `{"size":%d}`, len(payload))
			}))
			defer destination.Close()
			assign := &filer_pb.AssignVolumeResponse{
				FileId:   "1,test",
				Location: &filer_pb.Location{Url: strings.TrimPrefix(destination.URL, "http://")},
				Fsync:    tc.fsync,
			}
			err := (&S3ApiServer{}).streamCopyChunkRange(context.Background(), source.URL,
				assign.FileId, 0, int64(len(payload)), true, assign, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("stream copy error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}
