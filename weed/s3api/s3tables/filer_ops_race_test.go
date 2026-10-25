package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// competingCreateClient lands a foreign entry where the handler's existence
// check just answered "not there": the create that follows must lose to it
// rather than replace it, the way a second caller's create wins the window in
// a check-then-create race.
type competingCreateClient struct {
	filer_pb.SeaweedFilerClient
	f     *s3tablestest.MemFiler
	dir   string
	name  string
	plant bool
}

func (c *competingCreateClient) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.LookupDirectoryEntry(ctx, req, opts...)
	if c.plant && err != nil && req.Directory == c.dir && req.Name == c.name {
		c.plant = false
		c.f.Put(c.dir, c.name, map[string][]byte{
			ExtendedKeyTableBucket: []byte("true"),
			ExtendedKeyMetadata:    []byte(`{"OwnerAccountID":"competitor"}`),
		})
	}
	return resp, err
}

func runHandler(t *testing.T, m *Manager, client filer_pb.SeaweedFilerClient, operation string, input interface{}) (int, S3TablesError) {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	req, err := newManagerRequest(context.Background(), operation, body, "admin")
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	m.handler.HandleRequest(recorder, req, NewManagerClient(client))
	result := recorder.Result()
	defer result.Body.Close()
	var response S3TablesError
	json.NewDecoder(result.Body).Decode(&response)
	return result.StatusCode, response
}

func TestCreateTableBucketLostRaceKeepsTheFirstCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	m := NewManager()
	client := &competingCreateClient{SeaweedFilerClient: fs.Client, f: fs, dir: TablesPath, name: "bucket", plant: true}

	status, resp := runHandler(t, m, client, "CreateTableBucket", &CreateTableBucketRequest{Name: "bucket"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeBucketAlreadyExists, resp.Type)

	survivor := fs.Get(TablesPath, "bucket")
	require.NotNil(t, survivor)
	assert.Equal(t, `{"OwnerAccountID":"competitor"}`, string(survivor.Extended[ExtendedKeyMetadata]))
}

func TestCreateNamespaceLostRaceKeepsTheFirstCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	m := NewManager()
	bucketMetadata, _ := json.Marshal(&tableBucketMetadata{Name: "bucket", OwnerAccountID: DefaultAccountID})
	fs.Put(TablesPath, "bucket", map[string][]byte{ExtendedKeyTableBucket: []byte("true"), ExtendedKeyMetadata: bucketMetadata})

	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, "bucket")
	require.NoError(t, err)
	client := &competingCreateClient{SeaweedFilerClient: fs.Client, f: fs, dir: GetTableBucketPath("bucket"), name: "ns", plant: true}
	status, resp := runHandler(t, m, client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: arn,
		Namespace:      []string{"ns"},
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeNamespaceAlreadyExists, resp.Type)

	survivor := fs.Get(GetTableBucketPath("bucket"), "ns")
	require.NotNil(t, survivor)
	assert.Equal(t, `{"OwnerAccountID":"competitor"}`, string(survivor.Extended[ExtendedKeyMetadata]))
}
