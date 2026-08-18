package s3tables

import (
	"context"
	"encoding/json"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const renameTestBucket = "renamebkt"

func mustBucketARN(t *testing.T) string {
	t.Helper()
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, renameTestBucket)
	require.NoError(t, err)
	return arn
}

// startRenameManager seeds a bucket/namespace/table and returns a trusted Manager.
func startRenameManager(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs := s3tablestest.Start(t)

	bucketMeta, _ := json.Marshal(tableBucketMetadata{Name: renameTestBucket, OwnerAccountID: DefaultAccountID})
	fs.Put(TablesPath, renameTestBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
	})

	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "ns", map[string][]byte{ExtendedKeyMetadata: nsMeta})

	tableMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "t",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  3,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v3.metadata.json",
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "t", map[string][]byte{
		ExtendedKeyMetadata:        tableMeta,
		ExtendedKeyMetadataVersion: []byte("3"),
	})

	// Physical metadata.json and data files live under the table directory.
	tablePath := GetTablePath(renameTestBucket, "ns", "t")
	fs.Put(tablePath, "metadata", nil)
	fs.Put(tablePath, "data", nil)
	fs.Put(path.Join(tablePath, "metadata"), "v3.metadata.json", nil)

	m := NewManager()
	m.SetTrusted(true)
	return fs, m
}

func runRename(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, req *RenameTableRequest) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "RenameTable", req, nil, "")
}

func runGetTable(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace, name string) (*GetTableResponse, error) {
	t.Helper()
	resp := &GetTableResponse{}
	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "GetTable", &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
		Name:           name,
	}, resp, "")
	return resp, err
}

func TestRenameTablePreservesData(t *testing.T) {
	fs, m := startRenameManager(t)

	req := &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	}
	require.NoError(t, runRename(t, m, fs, req))

	// The source directory and its metadata.json/data children must survive: rename
	// is catalog-only and the destination still points at the original location.
	srcPath := GetTablePath(renameTestBucket, "ns", "t")
	assert.NotNil(t, fs.Get(srcPath, "metadata"), "source metadata dir must survive")
	assert.NotNil(t, fs.Get(srcPath, "data"), "source data dir must survive")
	assert.NotNil(t, fs.Get(path.Join(srcPath, "metadata"), "v3.metadata.json"), "metadata.json must survive")

	// Source catalog xattrs are dropped so the name stops resolving.
	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src, "source directory must remain to hold the data children")
	_, hasMeta := src.Extended[ExtendedKeyMetadata]
	assert.False(t, hasMeta, "source table-metadata xattr must be removed")

	_, err := runGetTable(t, m, fs, "ns", "t")
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)

	// The destination resolves to the preserved (original) MetadataLocation.
	got, err := runGetTable(t, m, fs, "ns", "t2")
	require.NoError(t, err)
	assert.Equal(t, "t2", got.Name)
	assert.Equal(t, "s3://"+renameTestBucket+"/ns/t/metadata/v3.metadata.json", got.MetadataLocation)

	dest := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t2")
	require.NotNil(t, dest)
	assert.Equal(t, []byte("3"), dest.Extended[ExtendedKeyMetadataVersion])
}

func TestRenameTableSourceMissing(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "ghost",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
}

func TestRenameTableDestExists(t *testing.T) {
	fs, m := startRenameManager(t)
	existing, _ := json.Marshal(tableMetadataInternal{Name: "t2", Namespace: "ns", OwnerAccountID: DefaultAccountID})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "t2", map[string][]byte{ExtendedKeyMetadata: existing})

	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeTableAlreadyExists, s3Err.Type)
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "source must be untouched on conflict")
}

func TestRenameTableDestNamespaceMissing(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"other"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchNamespace, s3Err.Type)
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "source must be untouched")
}

// A principal allowed to rename the source must still be denied when it cannot
// create a table in the destination namespace.
func TestRenameTableDestNamespaceUnauthorized(t *testing.T) {
	fs, m := startRenameManager(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)

	// "mover" may rename the source table but holds no rights on "dest".
	srcPolicy, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{{
			"Effect":    "Allow",
			"Principal": "mover",
			"Action":    "s3tables:RenameTable",
			"Resource":  "*",
		}},
	})
	srcEntry := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, srcEntry)
	srcEntry.Extended[ExtendedKeyPolicy] = srcPolicy

	destNsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"dest"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "dest", map[string][]byte{ExtendedKeyMetadata: destNsMeta})

	mover := &testIdentity{Name: "mover", Account: &testIdentityAccount{Id: "mover"}}
	ctx := s3_constants.SetIdentityInContext(context.Background(), mover)
	err := m.Execute(ctx, NewManagerClient(fs.Client), "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"dest"},
		DestName:        "t2",
	}, nil, "mover")
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeAccessDenied, s3Err.Type)

	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "source must be untouched")
	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "dest"), "t2"), "destination must not be written")
}

func TestRenameTableInvalidName(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "Bad/Name",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeInvalidRequest, s3Err.Type)
}

// A disabled maintenance configuration has to move with the table. Leaving it
// behind re-enables maintenance under the new name, and leaks the old settings
// onto whatever is created at the old one.
func TestRenameTableCarriesMaintenanceConfiguration(t *testing.T) {
	fs, m := startRenameManager(t)

	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	config := []byte(`{"icebergSnapshotManagement":{"status":"disabled"}}`)
	status := []byte(`{"icebergCompaction":{"status":"Successful"}}`)
	src.Extended[ExtendedKeyMaintenance] = config
	src.Extended[ExtendedKeyMaintenanceStatus] = status

	require.NoError(t, runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	}))

	dest := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t2")
	require.NotNil(t, dest)
	assert.Equal(t, config, dest.Extended[ExtendedKeyMaintenance], "the disable must move with the table")
	assert.Equal(t, status, dest.Extended[ExtendedKeyMaintenanceStatus])

	// And must not linger on the old name, where a reused name would inherit it.
	moved := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, moved)
	_, hasConfig := moved.Extended[ExtendedKeyMaintenance]
	assert.False(t, hasConfig, "source maintenance configuration must be cleared")
	_, hasStatus := moved.Extended[ExtendedKeyMaintenanceStatus]
	assert.False(t, hasStatus, "source maintenance status must be cleared")
}

// A maintenance configuration written after the rename copied the source must
// not be deleted by the source cleanup: it would vanish without ever reaching
// the destination.
func TestRenameTableRejectsConcurrentMaintenanceWrite(t *testing.T) {
	fs, _ := startRenameManager(t)

	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	copied := []byte(`{"icebergCompaction":{"status":"enabled"}}`)
	src.Extended[ExtendedKeyMaintenance] = copied

	// Stand in for the Put that lands between the copy and the cleanup.
	landedLate := []byte(`{"icebergSnapshotManagement":{"status":"disabled"}}`)
	expected := map[string][]byte{ExtendedKeyMaintenance: copied}

	src.Extended[ExtendedKeyMaintenance] = landedLate

	h := NewS3TablesHandler()
	err := NewManagerClient(fs.Client).WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.removeExtendedAttributesIf(context.Background(),
			client, GetTablePath(renameTestBucket, "ns", "t"), expected, renamedTableAttributes...)
	})

	require.Error(t, err, "cleanup must refuse to delete a configuration written after the copy")
	assert.ErrorIs(t, err, ErrConcurrentUpdate)
	assert.Equal(t, landedLate, src.Extended[ExtendedKeyMaintenance], "the late write must survive")
}
