package gcs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestGCSRemoteStorageClientImplementsInterface(t *testing.T) {
	var _ remote_storage.RemoteStorageClient = (*gcsRemoteStorageClient)(nil)
}

func TestGCSErrRemoteObjectNotFoundIsAccessible(t *testing.T) {
	require.Error(t, remote_storage.ErrRemoteObjectNotFound)
	require.Equal(t, "remote object not found", remote_storage.ErrRemoteObjectNotFound.Error())
}

// TestMakeWithHTTPClientAllowedTypes covers the restriction a caller applies to
// credentials it does not control: a federated document never reaches the SDK,
// while an unrestricted caller keeps loading whatever the operator configured.
func TestMakeWithHTTPClientAllowedTypes(t *testing.T) {
	federated := `{"type":"external_account","audience":"a","subject_token_type":"t","token_url":"http://127.0.0.1:9/v1/token","credential_source":{"url":"http://169.254.169.254/"}}`
	conf := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: federated}

	_, err := MakeWithHTTPClient(conf, nil, StaticKeyCredentialTypes...)
	require.ErrorContains(t, err, `"external_account" is not accepted`)

	_, err = MakeWithHTTPClient(conf, nil)
	require.NoError(t, err)
}

func TestParseInlineCredentials(t *testing.T) {
	credType, tokenURL, err := ParseInlineCredentials(`{"type":"service_account"}`)
	require.NoError(t, err)
	require.Equal(t, "service_account", credType)
	require.Equal(t, defaultTokenURL, tokenURL)

	_, tokenURL, err = ParseInlineCredentials(`{"type":"service_account","token_uri":"https://example.com/t"}`)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/t", tokenURL)

	_, _, err = ParseInlineCredentials(`not json`)
	require.Error(t, err)
}

// fakeGCS answers object reads with readStatus and the JSON object listing
// with listStatus/listBody, counting listings.
type fakeGCS struct {
	readStatus int
	listStatus int
	listBody   string
	listCalls  atomic.Int32
}

func (f *fakeGCS) client(t *testing.T) *gcsRemoteStorageClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/storage/v1/b/") && strings.HasSuffix(r.URL.Path, "/o") {
			f.listCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(f.listStatus)
			_, _ = io.WriteString(w, f.listBody)
			return
		}
		w.WriteHeader(f.readStatus)
	}))
	t.Cleanup(srv.Close)
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return &gcsRemoteStorageClient{conf: &remote_pb.RemoteConf{Name: "test"}, client: client}
}

func TestGCSReadNotFoundClassification(t *testing.T) {
	loc := &remote_pb.RemoteStorageLocation{Name: "test", Bucket: "bucket", Path: "/dir/obj.bin"}
	tests := []struct {
		name      string
		gcs       *fakeGCS
		notFound  bool
		wantLists int32
	}{
		{"missing object", &fakeGCS{readStatus: http.StatusNotFound, listStatus: http.StatusOK, listBody: `{"items":[{"name":"dir/obj.bin.bak"}]}`}, true, 2},
		{"missing bucket", &fakeGCS{readStatus: http.StatusNotFound, listStatus: http.StatusNotFound, listBody: `{"error":{"code":404}}`}, false, 2},
		{"bucket listing denied", &fakeGCS{readStatus: http.StatusNotFound, listStatus: http.StatusForbidden, listBody: `{"error":{"code":403}}`}, false, 2},
		{"object listed after the read missed it", &fakeGCS{readStatus: http.StatusNotFound, listStatus: http.StatusOK, listBody: `{"items":[{"name":"dir/obj.bin"}]}`}, false, 2},
		{"read denied", &fakeGCS{readStatus: http.StatusForbidden}, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.gcs.client(t)

			_, err := client.ReadFile(loc, 0, 10)
			require.Equal(t, tt.notFound, err == remote_storage.ErrRemoteObjectNotFound, "ReadFile: %v", err)
			_, err = client.ReadFileAsStream(context.Background(), loc, 0, 10)
			require.Equal(t, tt.notFound, err == remote_storage.ErrRemoteObjectNotFound, "ReadFileAsStream: %v", err)
			require.Equal(t, tt.wantLists, tt.gcs.listCalls.Load())
		})
	}
}
