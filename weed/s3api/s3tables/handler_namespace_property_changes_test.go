package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Iceberg catalog now sends its changes as PropertyChanges. That write
// must keep what the whole-map write keeps: the namespace's owner and creation
// time, and the properties it was not asked to change.
func TestUpdateNamespacePropertyChangesEditsInPlace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	requireOK(t, m, fs, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Properties:     map[string]string{"keep": "1", "drop": "1"},
	})
	before := namespaceMetadataOf(t, fs, "ns")

	code, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN:  mustBucketARN(t),
		Namespace:       []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"edited": "true"}, Removals: []string{"drop", "absent"}},
	})

	require.Equal(t, http.StatusOK, code, "%+v", resp)
	after := namespaceMetadataOf(t, fs, "ns")
	assert.Equal(t, map[string]string{"keep": "1", "edited": "true"}, after.Properties)
	assert.True(t, after.CreatedAt.Equal(before.CreatedAt), "an update must keep the creation time: %v became %v", before.CreatedAt, after.CreatedAt)
	assert.Equal(t, before.OwnerAccountID, after.OwnerAccountID)
}

// TestUpdateNamespaceSparesARecreatedNamespace for the PropertyChanges form:
// a namespace deleted and created again before the write is refused, not
// edited with the first one's owner.
func TestUpdateNamespacePropertyChangesSparesARecreatedNamespace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreated := recreatedNamespace(t, "ns")
	fs.OnUpdate = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnUpdate = nil
		fs.Put(bucketPath, "ns", recreated)
	}

	code, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN:  mustBucketARN(t),
		Namespace:       []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"edited": "true"}},
	})

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "the new namespace is untouched")
}

// A request carrying only Properties still replaces the stored map: keys it
// leaves out are dropped, one carrying none clears them, and the answer has no
// removal report.
func TestUpdateNamespaceWithPropertiesReplacesThem(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	before := namespaceMetadataOf(t, fs, "ns")
	update := func(properties map[string]string) string {
		t.Helper()
		body, err := json.Marshal(&UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Properties: properties})
		require.NoError(t, err)
		req, err := newManagerRequest(context.Background(), "UpdateNamespace", body, "admin")
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		m.handler.HandleRequest(rec, req, NewManagerClient(fs.Client))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		return rec.Body.String()
	}

	update(map[string]string{"a": "1", "b": "2"})
	answer := update(map[string]string{"c": "3"})
	var resp UpdateNamespaceResponse
	require.NoError(t, json.Unmarshal([]byte(answer), &resp))
	assert.Equal(t, map[string]string{"c": "3"}, resp.Properties)
	assert.NotContains(t, answer, `"removed"`)
	assert.NotContains(t, answer, `"missing"`)
	assert.Equal(t, map[string]string{"c": "3"}, namespaceMetadataOf(t, fs, "ns").Properties)

	update(nil)
	after := namespaceMetadataOf(t, fs, "ns")
	assert.Empty(t, after.Properties)
	assert.True(t, after.CreatedAt.Equal(before.CreatedAt), "an update must keep the creation time: %v became %v", before.CreatedAt, after.CreatedAt)
	assert.Equal(t, before.OwnerAccountID, after.OwnerAccountID)
}

// A caller denied the update gets the answer a missing namespace gives, as with
// Properties, and nothing is written.
func TestUpdateNamespacePropertyChangesDenialMatchesMissing(t *testing.T) {
	existing, manager := startRenameManager(t)
	missing := s3tablestest.Start(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)
	before := namespaceMetadataOf(t, existing, "ns")
	request := &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"k": "v"}}}

	want := runUnauthorizedRequest(t, manager, missing, "UpdateNamespace", request)
	got := runUnauthorizedRequest(t, manager, existing, "UpdateNamespace", request)
	assert.Equal(t, want, got)
	assert.Equal(t, 404, got.status)
	assert.Equal(t, ErrCodeNoSuchNamespace, got.body.Type)
	assert.Equal(t, before, namespaceMetadataOf(t, existing, "ns"))
}

// The directory a deleted namespace leaves for renamed data is not a
// namespace, so PropertyChanges answers NoSuchNamespace and writes nothing to
// it, as Properties does.
func TestUpdateNamespacePropertyChangesOnKeptDirectory(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)
	before := extendedOf(t, fs, GetTableBucketPath(renameTestBucket), "ns")

	status, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"k": "v"}, Removals: []string{"x"}}})

	assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
	assert.Equal(t, before, extendedOf(t, fs, GetTableBucketPath(renameTestBucket), "ns"), "the kept directory must not be written to")
}

// The index namespace of a vector bucket stays read-only to PropertyChanges,
// as to Properties.
func TestUpdateNamespacePropertyChangesVectorNamespaceReadOnly(t *testing.T) {
	fs := s3tablestest.Start(t)
	trusted := seedVectorBucket(t, fs, FormatLance)
	read := func() GetNamespaceResponse {
		t.Helper()
		var resp GetNamespaceResponse
		require.NoError(t, trusted.Execute(context.Background(), NewManagerClient(fs.Client), "GetNamespace",
			&GetNamespaceRequest{TableBucketARN: vectorTestARN(t), Namespace: []string{VectorIndexNamespace}}, &resp, ""))
		return resp
	}
	before := read()

	got := runRequestAs(t, untrustedManager(), fs, "UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: vectorTestARN(t),
		Namespace: []string{VectorIndexNamespace}, PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"a": "b"}}}, []string{"s3tables:*"})

	assert.Equal(t, 403, got.status)
	assert.Equal(t, ErrCodeAccessDenied, got.body.Type)
	assert.Equal(t, vectorNamespaceReadOnly, got.body.Message)
	assert.Equal(t, before, read())
}

// UpdateNamespace does not grant GetNamespace, so a PropertyChanges answer
// carries no stored property.
func TestUpdateNamespacePropertyChangesAnswerHidesStoredProperties(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	requireOK(t, m, fs, "UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"},
		Properties: map[string]string{"stored": "1", "drop": "1"}})

	var resp UpdateNamespaceResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Removals: []string{"drop"}}}, &resp, "admin"))

	assert.Empty(t, resp.Properties)
	assert.Equal(t, []string{"drop"}, resp.Removed)
	assert.Equal(t, map[string]string{"stored": "1"}, namespaceMetadataOf(t, fs, "ns").Properties)
}
