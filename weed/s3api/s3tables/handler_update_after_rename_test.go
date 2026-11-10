package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// A rename is catalog-only: a table or view renamed into another namespace keeps
// its files, and so the directory its metadata location names, under the
// namespace it came from. Its next update writes there and must be accepted,
// while an update that moves it elsewhere stays confined to its new namespace.

func addRenameNamespace(t *testing.T, fs *s3tablestest.MemFiler, namespace string) {
	t.Helper()
	nsMeta, err := json.Marshal(namespaceMetadata{Namespace: []string{namespace}, OwnerAccountID: DefaultAccountID})
	require.NoError(t, err)
	fs.Put(GetTableBucketPath(renameTestBucket), namespace, map[string][]byte{ExtendedKeyMetadata: nsMeta})
}

// renamedAcrossNamespaces seeds ns/t, whose files sit under ns/t, and renames it
// to ns2/t.
func renamedAcrossNamespaces(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startRenameManager(t)
	addRenameNamespace(t, fs, "ns2")
	require.NoError(t, runRename(t, m, fs, &RenameTableRequest{TableBucketARN: mustBucketARN(t),
		SourceNamespace: []string{"ns"}, SourceName: "t", DestNamespace: []string{"ns2"}, DestName: "t"}))
	return fs, m
}

func seedRenameTable(t *testing.T, fs *s3tablestest.MemFiler, namespace, name string) {
	t.Helper()
	meta, err := json.Marshal(tableMetadataInternal{Name: name, Namespace: namespace, Format: "ICEBERG", OwnerAccountID: DefaultAccountID,
		MetadataVersion: 1, MetadataLocation: "s3://" + renameTestBucket + "/" + namespace + "/" + name + "/metadata/v1.metadata.json"})
	require.NoError(t, err)
	fs.Put(GetNamespacePath(renameTestBucket, namespace), name, map[string][]byte{ExtendedKeyMetadata: meta})
}

func updateTableAt(ctx context.Context, t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace, location string) error {
	return m.Execute(ctx, NewManagerClient(fs.Client), "UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t),
		Namespace: []string{namespace}, Name: "t", MetadataVersion: 4, MetadataLocation: location}, nil, "")
}

func s3TablesErrorText(err error) string {
	var tableErr *S3TablesError
	if errors.As(err, &tableErr) {
		return tableErr.Type + ": " + tableErr.Message
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func TestUpdateAfterRenameAcrossNamespacesKeepsItsDirectory(t *testing.T) {
	fs, m := renamedAcrossNamespaces(t)
	location := "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json"
	require.NoError(t, updateTableAt(context.Background(), t, m, fs, "ns2", location))

	got, err := runGetTable(t, m, fs, "ns2", "t")
	require.NoError(t, err)
	require.Equal(t, location, got.MetadataLocation)
}

func TestUpdateAfterRenameAcrossNamespacesRefusesOtherDirectoriesOfTheOldNamespace(t *testing.T) {
	fs, m := renamedAcrossNamespaces(t)
	err := updateTableAt(context.Background(), t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns/elsewhere/metadata/v4.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), "must be within namespace ns2")
}

func TestUpdateAfterRenameAcrossNamespacesRefusesAnotherTablesDirectory(t *testing.T) {
	// One the old namespace holds: not the table's own directory, and outside
	// its current namespace.
	fs, m := renamedAcrossNamespaces(t)
	seedRenameTable(t, fs, "ns", "u")
	err := updateTableAt(context.Background(), t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns/u/metadata/v9.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), "must be within namespace ns2")

	// One the current namespace holds: inside the namespace, but claimed.
	fs, m = renamedAcrossNamespaces(t)
	seedRenameTable(t, fs, "ns2", "w")
	err = updateTableAt(context.Background(), t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns2/w/metadata/v9.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), ErrCodeConflict+": location is in use")
}

// A table created under the renamed table's old name has its entry on that
// directory and its files in a directory of its own, so the renamed table still
// keeps its files there alone. The record is seeded rather than created, since
// a rename may keep the old name taken.
func TestUpdateAfterRenameAcrossNamespacesKeepsItsDirectoryUnderANewTableOfTheOldName(t *testing.T) {
	ctx := context.Background()
	fs, m := renamedAcrossNamespaces(t)
	meta, err := json.Marshal(tableMetadataInternal{Name: "t", Namespace: "ns", Format: "ICEBERG", OwnerAccountID: DefaultAccountID,
		MetadataVersion: 1, WarehouseLocation: "s3://" + renameTestBucket + "/ns/t-fresh"})
	require.NoError(t, err)
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "t", map[string][]byte{ExtendedKeyMetadata: meta})

	location := "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json"
	require.NoError(t, updateTableAt(ctx, t, m, fs, "ns2", location))
	got, err := runGetTable(t, m, fs, "ns2", "t")
	require.NoError(t, err)
	require.Equal(t, location, got.MetadataLocation)
}

// A record written before locations were confined to their namespace and
// claimed can keep its files in another table's directory. It gets no
// exception: it stays held to its namespace, as before.
func TestUpdateRefusesADirectoryOutsideTheNamespaceThatAnotherTableShares(t *testing.T) {
	fs, m := startRenameManager(t)
	addRenameNamespace(t, fs, "ns2")
	meta, err := json.Marshal(tableMetadataInternal{Name: "t", Namespace: "ns2", Format: "ICEBERG", OwnerAccountID: DefaultAccountID,
		MetadataVersion: 3, MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v3.metadata.json"})
	require.NoError(t, err)
	fs.Put(GetNamespacePath(renameTestBucket, "ns2"), "t", map[string][]byte{ExtendedKeyMetadata: meta})

	err = updateTableAt(context.Background(), t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns/t/metadata/v4.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), "must be within namespace ns2")
}

func TestUpdateAfterRenameAcrossNamespacesMayMoveIntoItsNamespace(t *testing.T) {
	fs, m := renamedAcrossNamespaces(t)
	require.NoError(t, updateTableAt(context.Background(), t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns2/t-moved/metadata/v4.metadata.json"))
}

func TestUpdateStillConfinesAMoveToTheTablesNamespace(t *testing.T) {
	fs, m := startRenameManager(t)
	addRenameNamespace(t, fs, "ns2")
	err := updateTableAt(context.Background(), t, m, fs, "ns", "s3://"+renameTestBucket+"/ns2/elsewhere/metadata/v4.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), "must be within namespace ns")
}

func TestUpdateLocationIsCheckedAfterAuthorization(t *testing.T) {
	fs, m := renamedAcrossNamespaces(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)
	stranger := &testIdentity{Name: "stranger", Account: &testIdentityAccount{Id: "stranger"}}
	ctx := s3_constants.SetIdentityInContext(context.Background(), stranger)

	err := updateTableAt(ctx, t, m, fs, "ns2", "s3://"+renameTestBucket+"/ns/elsewhere/metadata/v4.metadata.json")
	require.Error(t, err)
	require.NotContains(t, s3TablesErrorText(err), "must be within namespace",
		"a caller who may not update the table must not learn where its files may go")
}

func renameViewAcrossNamespaces(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startRenameManager(t)
	seedView(t, fs, "v")
	addRenameNamespace(t, fs, "ns2")
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "RenameView", &RenameTableRequest{
		TableBucketARN: mustBucketARN(t), SourceNamespace: []string{"ns"}, SourceName: "v", DestNamespace: []string{"ns2"}, DestName: "v",
	}, nil, ""))
	return fs, m
}

func updateViewAt(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, location string) error {
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "UpdateView", &UpdateViewRequest{TableBucketARN: mustBucketARN(t),
		Namespace: []string{"ns2"}, Name: "v", MetadataVersion: 2, MetadataLocation: location}, nil, "")
}

func TestUpdateViewAfterRenameAcrossNamespacesKeepsItsDirectory(t *testing.T) {
	fs, m := renameViewAcrossNamespaces(t)
	location := "s3://" + renameTestBucket + "/ns/v/metadata/v2.metadata.json"
	require.NoError(t, updateViewAt(t, m, fs, location))

	entry := fs.Get(GetNamespacePath(renameTestBucket, "ns2"), "v")
	require.NotNil(t, entry)
	var stored tableMetadataInternal
	require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &stored))
	require.Equal(t, location, stored.MetadataLocation)
}

func TestUpdateViewAfterRenameAcrossNamespacesRefusesOtherDirectories(t *testing.T) {
	fs, m := renameViewAcrossNamespaces(t)
	err := updateViewAt(t, m, fs, "s3://"+renameTestBucket+"/ns/elsewhere/metadata/v2.metadata.json")
	require.Error(t, err)
	require.Contains(t, s3TablesErrorText(err), "must be within namespace ns2")
}

// The same-directory exception compares the directory the location names. A
// location whose directory is the bucket root (s3://bucket/metadata/...) names
// no table directory, so it must not match the root a record falls back to and
// must meet the namespace check, as it did before the exception existed.
func TestUpdateRefusesAMetadataDirectoryAtTheBucketRoot(t *testing.T) {
	bucketRoot := "s3://" + renameTestBucket + "/metadata/v9.metadata.json"
	ctx := context.Background()

	t.Run("table created without a location", func(t *testing.T) {
		fs, m := startRenameManager(t)
		require.NoError(t, m.Execute(ctx, NewManagerClient(fs.Client), "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t),
			Namespace: []string{"ns"}, Name: "fresh", Format: "ICEBERG"}, nil, ""))
		err := m.Execute(ctx, NewManagerClient(fs.Client), "UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t),
			Namespace: []string{"ns"}, Name: "fresh", MetadataVersion: 2, MetadataLocation: bucketRoot}, nil, "")
		require.Error(t, err)
		require.Contains(t, s3TablesErrorText(err), "must be within namespace ns")
	})
	t.Run("table stored at its name path", func(t *testing.T) {
		fs, m := startRenameManager(t)
		err := updateTableAt(ctx, t, m, fs, "ns", bucketRoot)
		require.Error(t, err)
		require.Contains(t, s3TablesErrorText(err), "must be within namespace ns")
	})
	t.Run("view", func(t *testing.T) {
		fs, m := startRenameManager(t)
		seedView(t, fs, "v")
		err := m.Execute(ctx, NewManagerClient(fs.Client), "UpdateView", &UpdateViewRequest{TableBucketARN: mustBucketARN(t),
			Namespace: []string{"ns"}, Name: "v", MetadataVersion: 2, MetadataLocation: bucketRoot}, nil, "")
		require.Error(t, err)
		require.Contains(t, s3TablesErrorText(err), "must be within namespace ns")
	})
}

// A table created without a location keeps its files in the warehouse directory
// it was given; its first commit writes there, also after a rename across
// namespaces.
func TestUpdateOfATableCreatedWithoutALocationWritesItsWarehouse(t *testing.T) {
	ctx := context.Background()
	fs, m := startRenameManager(t)
	addRenameNamespace(t, fs, "ns2")
	client := NewManagerClient(fs.Client)
	require.NoError(t, m.Execute(ctx, client, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t),
		Namespace: []string{"ns"}, Name: "fresh", Format: "ICEBERG"}, nil, ""))
	created, err := runGetTable(t, m, fs, "ns", "fresh")
	require.NoError(t, err)
	require.NotEmpty(t, created.WarehouseLocation)

	require.NoError(t, runRename(t, m, fs, &RenameTableRequest{TableBucketARN: mustBucketARN(t),
		SourceNamespace: []string{"ns"}, SourceName: "fresh", DestNamespace: []string{"ns2"}, DestName: "fresh"}))
	location := created.WarehouseLocation + "/metadata/v1.metadata.json"
	require.NoError(t, m.Execute(ctx, client, "UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t),
		Namespace: []string{"ns2"}, Name: "fresh", MetadataVersion: 1, MetadataLocation: location}, nil, ""))

	got, err := runGetTable(t, m, fs, "ns2", "fresh")
	require.NoError(t, err)
	require.Equal(t, location, got.MetadataLocation)
}
