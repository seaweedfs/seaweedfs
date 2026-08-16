package s3tables

import (
	"context"
	"encoding/json"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedView adds a view alongside the table the rename harness already creates.
func seedView(t *testing.T, fs *memFilerServer, name string) {
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

	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), name, map[string][]byte{
		ExtendedKeyMetadata:        viewMeta,
		ExtendedKeyMetadataVersion: []byte("1"),
		ExtendedKeyEntryType:       []byte(EntryTypeView),
	})
	viewPath := GetTablePath(renameTestBucket, "ns", name)
	fs.putEntry(viewPath, "metadata", nil)
	fs.putEntry(path.Join(viewPath, "metadata"), "v1.metadata.json", nil)
}

func runRenameView(t *testing.T, m *Manager, fs *memFilerServer, sourceName, destName string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.client), "RenameView", &RenameTableRequest{
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

	dest := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "v2")
	require.NotNil(t, dest)
	assert.Equal(t, EntryTypeView, entryType(dest.Extended), "destination must stay a view")

	var moved tableMetadataInternal
	require.NoError(t, json.Unmarshal(dest.Extended[ExtendedKeyMetadata], &moved))
	assert.Equal(t, "v2", moved.Name)
	assert.Equal(t, "s3://"+renameTestBucket+"/ns/v/metadata/v1.metadata.json", moved.MetadataLocation,
		"rename is catalog-only, the metadata stays where it was written")

	src := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "v")
	require.NotNil(t, src)
	_, stillListed := src.Extended[ExtendedKeyMetadata]
	assert.False(t, stillListed, "source name must stop resolving")
	assert.NotNil(t, fs.getEntry(path.Join(GetTablePath(renameTestBucket, "ns", "v"), "metadata"), "v1.metadata.json"),
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
