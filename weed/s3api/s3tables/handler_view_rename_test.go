package s3tables

import (
	"context"
	"encoding/json"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedView adds a view alongside the table the rename harness already creates.
func seedView(t *testing.T, fs *s3tablestest.MemFiler, name string) {
	t.Helper()

	viewMeta, err := json.Marshal(tableMetadataInternal{
		Name:             name,
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  1,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/" + name + "/metadata/v1.metadata.json",
	})
	require.NoError(t, err)

	fs.Put(GetNamespacePath(renameTestBucket, "ns"), name, map[string][]byte{
		ExtendedKeyMetadata:        viewMeta,
		ExtendedKeyMetadataVersion: []byte("1"),
		ExtendedKeyEntryType:       []byte(EntryTypeView),
	})
	viewPath := GetTablePath(renameTestBucket, "ns", name)
	fs.Put(viewPath, "metadata", nil)
	fs.Put(path.Join(viewPath, "metadata"), "v1.metadata.json", nil)
}

func runRenameView(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, sourceName, destName string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "RenameView", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      sourceName,
		DestNamespace:   []string{"ns"},
		DestName:        destName,
	}, nil, "")
}

func TestRenameViewMovesCatalogPointer(t *testing.T) {
	fs, m := startRenameManager(t)
	seedView(t, fs, "v")

	require.NoError(t, runRenameView(t, m, fs, "v", "v2"))

	dest := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "v2")
	require.NotNil(t, dest)
	assert.Equal(t, EntryTypeView, EntryType(dest.Extended), "destination must stay a view")

	var moved tableMetadataInternal
	require.NoError(t, json.Unmarshal(dest.Extended[ExtendedKeyMetadata], &moved))
	assert.Equal(t, "v2", moved.Name)
	assert.Equal(t, "s3://"+renameTestBucket+"/ns/v/metadata/v1.metadata.json", moved.MetadataLocation,
		"rename is catalog-only, the metadata stays where it was written")

	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "v")
	require.NotNil(t, src)
	_, stillListed := src.Extended[ExtendedKeyMetadata]
	assert.False(t, stillListed, "source name must stop resolving")
	assert.NotNil(t, fs.Get(path.Join(GetTablePath(renameTestBucket, "ns", "v"), "metadata"), "v1.metadata.json"),
		"the view's metadata file must survive")
}

// Tables and views share one namespace directory, so each rename must refuse
// the other kind rather than moving it.
func TestRenameViewRejectsTable(t *testing.T) {
	fs, m := startRenameManager(t)

	err := runRenameView(t, m, fs, "t", "t2")
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchView, s3Err.Type)
}

func TestRenameTableRejectsView(t *testing.T) {
	fs, m := startRenameManager(t)
	seedView(t, fs, "v")

	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "v",
		DestNamespace:   []string{"ns"},
		DestName:        "v2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
}

// A policy scoped to the view ARN must authorize renaming that view. Checking
// the source against a table ARN would silently ignore it.
func TestRenameViewAuthorizesAgainstTheViewARN(t *testing.T) {
	fs, m := startRenameManager(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)
	seedView(t, fs, "v")

	const principal = "analyst"
	viewARN := "arn:aws:s3tables:" + DefaultRegion + ":" + DefaultAccountID + ":bucket/" + renameTestBucket + "/view/ns/v"
	viewPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"` + principal +
		`","Action":"s3tables:RenameView","Resource":"` + viewARN + `"}]}`
	view := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "v")
	require.NotNil(t, view)
	view.Extended[ExtendedKeyPolicy] = []byte(viewPolicy)

	// Landing in the namespace needs create permission there as well.
	namespacePolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"` + principal +
		`","Action":"s3tables:CreateView","Resource":"` + mustBucketARN(t) + `"}]}`
	namespace := fs.Get(GetTableBucketPath(renameTestBucket), "ns")
	require.NotNil(t, namespace)
	namespace.Extended[ExtendedKeyPolicy] = []byte(namespacePolicy)

	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "RenameView", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "v",
		DestNamespace:   []string{"ns"},
		DestName:        "v2",
	}, nil, principal)
	require.NoError(t, err)

	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "v2"))
}
