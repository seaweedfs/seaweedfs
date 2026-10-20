package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A table bucket with the marker but no metadata is damaged, not ownerless for
// the taking: an ordinary caller is answered with the operation's own denial,
// and the admin or a trusted handler may repair it.
func TestCreateNamespaceOnAnOwnerlessBucket(t *testing.T) {
	const bucket = "broken"
	plant := func(fs *s3tablestest.MemFiler) {
		fs.Put(TablesPath, bucket, map[string][]byte{ExtendedKeyTableBucket: []byte("true")})
	}
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, bucket)
	require.NoError(t, err)
	request := &CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{"ns"}}

	t.Run("an outsider gets the denial and writes nothing", func(t *testing.T) {
		fs := s3tablestest.Start(t)
		plant(fs)
		m := NewManager()
		body, err := json.Marshal(request)
		require.NoError(t, err)
		identity := &testIdentity{Name: "outsider", Account: &testIdentityAccount{Id: "outsider"}, Actions: []string{"Read"}}
		req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), "CreateNamespace", body, "outsider")
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
		assert.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
		entry := fs.Get(TablesPath, bucket)
		require.NotNil(t, entry)
		assert.Empty(t, entry.Extended[ExtendedKeyMetadata], "the denied caller wrote itself in as the bucket owner")
		assert.Nil(t, fs.Get(GetTableBucketPath(bucket), "ns"))
	})

	t.Run("a trusted handler repairs it and owns it", func(t *testing.T) {
		fs := s3tablestest.Start(t)
		plant(fs)
		m := NewManager()
		m.SetTrusted(true)
		body, err := json.Marshal(request)
		require.NoError(t, err)
		identity := &testIdentity{Name: "svc", Account: &testIdentityAccount{Id: "svc"}, Actions: []string{"Read"}}
		req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), "CreateNamespace", body, "svc")
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
		assert.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		entry := fs.Get(TablesPath, bucket)
		require.NotNil(t, entry)
		var metadata tableBucketMetadata
		require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
		assert.Equal(t, "svc", metadata.OwnerAccountID)
		require.NotNil(t, fs.Get(GetTableBucketPath(bucket), "ns"))
	})

	t.Run("an untrusted admin repairs it too", func(t *testing.T) {
		fs := s3tablestest.Start(t)
		plant(fs)
		m := NewManager()
		body, err := json.Marshal(request)
		require.NoError(t, err)
		identity := &testIdentity{Name: "admin", Account: &testIdentityAccount{Id: s3_constants.AccountAdminId}, Actions: []string{"Admin"}}
		req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), "CreateNamespace", body, "admin")
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
		assert.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		entry := fs.Get(TablesPath, bucket)
		require.NotNil(t, entry)
		var metadata tableBucketMetadata
		require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
		assert.Equal(t, s3_constants.AccountAdminId, metadata.OwnerAccountID)
	})
}
