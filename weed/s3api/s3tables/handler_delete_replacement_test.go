package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// pausedLookupClient holds the first lookup of dir/name until release is
// closed, after closing reached. A delete reads the table bucket between the
// read that authorizes it and the read that claims the record, so holding that
// lookup lands another request exactly there.
type pausedLookupClient struct {
	filer_pb.SeaweedFilerClient
	dir     string
	name    string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPausedLookupClient(client filer_pb.SeaweedFilerClient, dir, name string) *pausedLookupClient {
	return &pausedLookupClient{
		SeaweedFilerClient: client,
		dir:                dir,
		name:               name,
		reached:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (c *pausedLookupClient) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Directory == c.dir && req.Name == c.name {
		c.once.Do(func() {
			close(c.reached)
			<-c.release
		})
	}
	return c.SeaweedFilerClient.LookupDirectoryEntry(ctx, req, opts...)
}

func createViewRequest(t *testing.T, name string) *CreateViewRequest {
	return &CreateViewRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             name,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/" + name + "/metadata/v1.metadata.json",
	}
}

func deleteViewRequest(t *testing.T, name, token string) *DeleteViewRequest {
	return &DeleteViewRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           name,
		VersionToken:   token,
	}
}

func deleteTableRequest(t *testing.T, name string) *DeleteTableRequest {
	return &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           name,
	}
}

func getViewRequest(t *testing.T, name string) *GetViewRequest {
	return &GetViewRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           name,
	}
}

func getTableRequest(t *testing.T, name string) *GetTableRequest {
	return &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           name,
	}
}

// versionTokenOf returns the token the record under name carries.
func versionTokenOf(t *testing.T, fs *s3tablestest.MemFiler, name string) string {
	t.Helper()
	entry := fs.Get(GetNamespacePath(renameTestBucket, "ns"), name)
	require.NotNil(t, entry)
	var metadata tableMetadataInternal
	require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
	return metadata.VersionToken
}

// requireOK runs an operation that has to succeed.
func requireOK(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, operation string, input interface{}) {
	t.Helper()
	status, resp := runHandler(t, m, fs.Client, operation, input)
	require.Equal(t, http.StatusOK, status, "%s: %+v", operation, resp)
}

// A delete authorized on one record must not remove a record that took its name
// afterwards. Another delete removes the view and a create reuses the name while
// the first delete is held between its two reads.
func TestDeleteViewSparesAReplacement(t *testing.T) {
	const name = "v"
	cases := []struct {
		name        string
		replace     func(t *testing.T, m *Manager, fs *s3tablestest.MemFiler)
		withToken   bool
		wantStatus  int
		wantType    string
		wantMessage string
		read        string
		readInput   func(t *testing.T) interface{}
	}{
		{
			name: "table without token",
			replace: func(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
				requireOK(t, m, fs, "CreateTable", createTableRequest(t, "ns", name))
			},
			wantStatus: http.StatusNotFound,
			wantType:   ErrCodeNoSuchView,
			read:       "GetTable",
			readInput:  func(t *testing.T) interface{} { return getTableRequest(t, name) },
		},
		{
			name: "view with the old token",
			replace: func(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
				requireOK(t, m, fs, "CreateView", createViewRequest(t, name))
			},
			withToken:   true,
			wantStatus:  http.StatusConflict,
			wantType:    ErrCodeConflict,
			wantMessage: "version token mismatch",
			read:        "GetView",
			readInput:   func(t *testing.T) interface{} { return getViewRequest(t, name) },
		},
		{
			name: "view without token",
			replace: func(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
				requireOK(t, m, fs, "CreateView", createViewRequest(t, name))
			},
			wantStatus:  http.StatusConflict,
			wantType:    ErrCodeConflict,
			wantMessage: "changed during delete",
			read:        "GetView",
			readInput:   func(t *testing.T) interface{} { return getViewRequest(t, name) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, m := startRenameManager(t)
			requireOK(t, m, fs, "CreateView", createViewRequest(t, name))
			oldToken := versionTokenOf(t, fs, name)
			token := ""
			if tc.withToken {
				token = oldToken
			}

			client := newPausedLookupClient(fs.Client, TablesPath, renameTestBucket)
			deleted := startHandler(t, m, client, "DeleteView", deleteViewRequest(t, name, token))
			waitForPause(t, client.reached, deleted)

			requireOK(t, m, fs, "DeleteView", deleteViewRequest(t, name, ""))
			tc.replace(t, m, fs)
			close(client.release)

			got := <-deleted
			assert.Equal(t, tc.wantStatus, got.status, "%+v", got.resp)
			assert.Equal(t, tc.wantType, got.resp.Type)
			assert.Contains(t, got.resp.Message, tc.wantMessage)

			status, resp := runHandler(t, m, fs.Client, tc.read, tc.readInput(t))
			assert.Equal(t, http.StatusOK, status, "the replacement must survive: %+v", resp)
			survivor := fs.Get(GetNamespacePath(renameTestBucket, "ns"), name)
			require.NotNil(t, survivor)
			assert.NotContains(t, survivor.Extended, ExtendedKeyDeleting, "a refused delete must not leave its claim")
		})
	}
}

// The same gap in DeleteTable: a view that takes the table's name while the
// delete is held between its reads is not the record the delete authorized.
func TestDeleteTableSparesAReplacementView(t *testing.T) {
	cases := []struct {
		name string
		// seed makes the table to delete and returns its name and the location
		// the replacement view points at, so a delete that wrongly took the view
		// would purge from there.
		seed func(fs *s3tablestest.MemFiler) (name, location string)
	}{
		{
			name: "colocated",
			seed: func(fs *s3tablestest.MemFiler) (string, string) {
				return "t", "s3://" + renameTestBucket + "/ns/t/metadata/v3.metadata.json"
			},
		},
		{
			name: "decoupled",
			seed: func(fs *s3tablestest.MemFiler) (string, string) {
				location := "s3://" + renameTestBucket + "/ns/dt-x/metadata/v1.metadata.json"
				meta, _ := json.Marshal(tableMetadataInternal{
					Name:             "dt",
					Namespace:        "ns",
					Format:           "ICEBERG",
					OwnerAccountID:   DefaultAccountID,
					MetadataLocation: location,
				})
				fs.Put(GetNamespacePath(renameTestBucket, "ns"), "dt", map[string][]byte{
					ExtendedKeyMetadata:  meta,
					ExtendedKeyEntryType: []byte(EntryTypeTable),
				})
				fs.Put(GetNamespacePath(renameTestBucket, "ns"), "dt-x", nil)
				fs.Put(GetTablePath(renameTestBucket, "ns", "dt-x"), "metadata", nil)
				return "dt", location
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, m := startRenameManager(t)
			name, location := tc.seed(fs)

			client := newPausedLookupClient(fs.Client, TablesPath, renameTestBucket)
			deleted := startHandler(t, m, client, "DeleteTable", deleteTableRequest(t, name))
			waitForPause(t, client.reached, deleted)

			requireOK(t, m, fs, "DeleteTable", deleteTableRequest(t, name))
			// CreateView refuses a name that a decoupled delete left a directory
			// on, so the replacement is put the way a create leaves it.
			viewMeta, _ := json.Marshal(tableMetadataInternal{
				Name:             name,
				Namespace:        "ns",
				Format:           "ICEBERG",
				OwnerAccountID:   DefaultAccountID,
				VersionToken:     "replacement-token",
				MetadataLocation: location,
			})
			fs.Put(GetNamespacePath(renameTestBucket, "ns"), name, map[string][]byte{
				ExtendedKeyMetadata:  viewMeta,
				ExtendedKeyEntryType: []byte(EntryTypeView),
			})
			close(client.release)

			got := <-deleted
			assert.Equal(t, http.StatusNotFound, got.status, "%+v", got.resp)
			assert.Equal(t, ErrCodeNoSuchTable, got.resp.Type)

			status, resp := runHandler(t, m, fs.Client, "GetView", getViewRequest(t, name))
			assert.Equal(t, http.StatusOK, status, "the replacement view must survive: %+v", resp)
			survivor := fs.Get(GetNamespacePath(renameTestBucket, "ns"), name)
			require.NotNil(t, survivor)
			assert.NotContains(t, survivor.Extended, ExtendedKeyDeleting, "a refused delete must not leave its claim")
		})
	}
}
