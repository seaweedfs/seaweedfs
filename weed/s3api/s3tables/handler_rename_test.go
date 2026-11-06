package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"sync"
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

// A delete that purges the source between the rename's read and its claim loses
// the source, not the destination namespace — the answer must name the table.
func TestRenameTableSourceDeletedMidRename(t *testing.T) {
	fs, m := startRenameManager(t)

	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.BeforeUpdate = func() {
		_, _ = fs.Client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:    nsPath,
			Name:         "t",
			IsRecursive:  true,
			IsDeleteData: true,
		})
	}

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
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
	assert.Nil(t, fs.Get(nsPath, "t2"), "a failed rename must not publish the destination")
}

// A source that vanishes after the destination is claimed must not leave the
// destination behind: the failed rename removes the marked record on the way
// out, before it is ever published.
func TestRenameTableBacksOutConfirmedDest(t *testing.T) {
	fs, m := startRenameManager(t)

	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.OnUpdate = func(dir, name string) {
		if dir == nsPath && name == "t2" {
			_, _ = fs.Client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
				Directory:    nsPath,
				Name:         "t",
				IsRecursive:  true,
				IsDeleteData: true,
			})
		}
	}

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
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
	assert.Nil(t, fs.Get(nsPath, "t2"), "a failed rename must not leave the claimed destination behind")
}

// A principal allowed to rename the source must still be denied when it cannot
// create a table in the destination namespace. The denial reports the same
// not-found as a missing destination namespace so the caller cannot probe for
// namespaces it may not touch.
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
	assert.Equal(t, ErrCodeNoSuchNamespace, s3Err.Type)

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

// setRenameSourceVersionToken stamps the seeded source table with token.
func setRenameSourceVersionToken(t *testing.T, fs *s3tablestest.MemFiler, token string) {
	t.Helper()
	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	var meta tableMetadataInternal
	require.NoError(t, json.Unmarshal(src.Extended[ExtendedKeyMetadata], &meta))
	meta.VersionToken = token
	raw, err := json.Marshal(&meta)
	require.NoError(t, err)
	src.Extended[ExtendedKeyMetadata] = raw
}

// assertRenameRefused checks a refused rename left the source resolving, the
// destination absent, and no claim behind on the source.
func assertRenameRefused(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
	t.Helper()
	_, err := runGetTable(t, m, fs, "ns", "t")
	assert.NoError(t, err, "the source must still resolve")

	_, err = runGetTable(t, m, fs, "ns", "t2")
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err, "the destination must not resolve")
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t2"), "the destination must not be written")

	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	_, claimed := src.Extended[ExtendedKeyDeleting]
	assert.False(t, claimed, "a refused rename must not leave its claim on the source")
}

func renameTToT2(t *testing.T, token string) *RenameTableRequest {
	return &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
		VersionToken:    token,
	}
}

func TestRenameTableVersionTokenMatches(t *testing.T) {
	fs, m := startRenameManager(t)
	setRenameSourceVersionToken(t, fs, "tok-1")

	status, resp := runHandler(t, m, fs.Client, "RenameTable", renameTToT2(t, "tok-1"))
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	got, err := runGetTable(t, m, fs, "ns", "t2")
	require.NoError(t, err)
	assert.Equal(t, "t2", got.Name)
}

func TestRenameTableVersionTokenMismatch(t *testing.T) {
	fs, m := startRenameManager(t)
	setRenameSourceVersionToken(t, fs, "tok-1")

	status, resp := runHandler(t, m, fs.Client, "RenameTable", renameTToT2(t, "stale"))
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
	assertRenameRefused(t, m, fs)
}

// The token must be checked against the snapshot the claim asserts, not the
// metadata read at the start of the rename. An UpdateTable landing between
// the two moves the token on; checking the first read would accept the stale
// token, and the claim would then succeed on the newer snapshot.
func TestRenameTableVersionTokenCheckedAgainstClaimedSnapshot(t *testing.T) {
	fs, m := startRenameManager(t)
	setRenameSourceVersionToken(t, fs, "tok-1")

	status, resp := renameDuringUpdate(t, fs, m, "tok-1")

	assert.Equal(t, http.StatusConflict, status, "a rename carrying the pre-update token must be refused: %+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
	assertRenameRefused(t, m, fs)

	got, err := runGetTable(t, m, fs, "ns", "t")
	require.NoError(t, err)
	assert.NotEqual(t, "tok-1", got.VersionToken, "the update must have moved the token on")
}

// Without a token the same race must still not publish the metadata read
// before the update: the claim succeeds on the newer snapshot, so a rename
// built from the first read would move the table back to v3 and drop the
// update's commit.
func TestRenameTableRefusesSourceChangedBetweenReads(t *testing.T) {
	fs, m := startRenameManager(t)
	setRenameSourceVersionToken(t, fs, "tok-1")

	status, resp := renameDuringUpdate(t, fs, m, "")

	assert.Equal(t, http.StatusConflict, status, "a rename built from a torn read must be refused: %+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
	assertRenameRefused(t, m, fs)

	got, err := runGetTable(t, m, fs, "ns", "t")
	require.NoError(t, err)
	assert.Equal(t, "s3://"+renameTestBucket+"/ns/t/metadata/v4.metadata.json", got.MetadataLocation, "the update's commit must survive")
}

// renameDuringUpdate renames t to t2 carrying token while an UpdateTable to
// v4 lands between the rename's first read of the source and the lookup its
// claim snapshots, and returns the rename's answer.
func renameDuringUpdate(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, token string) (int, S3TablesError) {
	t.Helper()
	nsPath := GetNamespacePath(renameTestBucket, "ns")

	// The rename's first lookup of the source reads its metadata; the second
	// starts the reads that end in the lookupEntry the claim snapshots. The
	// update lands on that second lookup. Its own lookups re-enter this hook,
	// so it fires once under a mutex and a flag rather than a sync.Once,
	// which would deadlock on the nested call.
	var (
		mu           sync.Mutex
		lookups      int
		fired        bool
		updateStatus int
		updateResp   S3TablesError
	)
	fs.OnLookup = func(dir, name string) {
		if dir != nsPath || name != "t" {
			return
		}
		mu.Lock()
		if fired {
			mu.Unlock()
			return
		}
		lookups++
		if lookups < 2 {
			mu.Unlock()
			return
		}
		fired = true
		mu.Unlock()
		updateStatus, updateResp = runHandler(t, m, fs.Client, "UpdateTable", &UpdateTableRequest{
			TableBucketARN:   mustBucketARN(t),
			Namespace:        []string{"ns"},
			Name:             "t",
			VersionToken:     "tok-1",
			MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json",
		})
	}

	status, resp := runHandler(t, m, fs.Client, "RenameTable", renameTToT2(t, token))
	fs.OnLookup = nil

	mu.Lock()
	require.True(t, fired, "the competing update never ran")
	mu.Unlock()
	require.Equal(t, http.StatusOK, updateStatus, "competing update: %+v", updateResp)
	return status, resp
}

func TestRenameReadsMatchSnapshot(t *testing.T) {
	snapshot := map[string][]byte{
		ExtendedKeyMetadata: []byte(`{"name":"t"}`),
		ExtendedKeyPolicy:   []byte(`{"Version":"2012-10-17"}`),
		ExtendedKeyTags:     []byte(`{"team":"a"}`),
	}
	reads := func(policy string) map[string][]byte {
		return map[string][]byte{
			ExtendedKeyMetadata:    snapshot[ExtendedKeyMetadata],
			ExtendedKeyPolicy:      []byte(policy),
			ExtendedKeyMaintenance: nil,
		}
	}
	cases := []struct {
		name   string
		policy string
		tags   map[string]string
		want   bool
	}{
		{"unchanged", `{"Version":"2012-10-17"}`, map[string]string{"team": "a"}, true},
		{"policy changed", `{"Version":"2008-10-17"}`, map[string]string{"team": "a"}, false},
		{"policy removed", "", map[string]string{"team": "a"}, false},
		{"tag value changed", `{"Version":"2012-10-17"}`, map[string]string{"team": "b"}, false},
		{"tags removed", `{"Version":"2012-10-17"}`, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, renameReadsMatchSnapshot(snapshot, tc.tags, reads(tc.policy)))
		})
	}
}
