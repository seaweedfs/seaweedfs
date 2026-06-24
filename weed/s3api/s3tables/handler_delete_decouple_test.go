package s3tables

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runDeleteTable(t *testing.T, m *Manager, fs *memFilerServer, namespace, name string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.client), "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
		Name:           name,
	}, nil, "")
}

// A table whose data was decoupled from its name (created over a leftover from a
// rename): catalog marker at ns/newt, data at ns/newt-x, and ns/newt still holds
// another table's leftover data. Dropping it must purge only its own data and
// clear the marker, never the data under the reused name path.
func TestDeleteTableDecoupledKeepsReusedNamePath(t *testing.T) {
	fs, m := startRenameManager(t)

	newtMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "newt",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/newt-x/metadata/v1.metadata.json",
	})
	markerKeys := []string{ExtendedKeyMetadata, ExtendedKeyMetadataVersion, ExtendedKeyPolicy, ExtendedKeyTags, ExtendedKeyEntryType}
	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), "newt", map[string][]byte{
		ExtendedKeyMetadata:        newtMeta,
		ExtendedKeyMetadataVersion: []byte("v1"),
		ExtendedKeyPolicy:          []byte(`{"Version":"2012-10-17"}`),
		ExtendedKeyTags:            []byte(`{"k":"v"}`),
		ExtendedKeyEntryType:       []byte(EntryTypeTable),
	})
	fs.putEntry(GetTablePath(renameTestBucket, "ns", "newt"), "leftover", nil) // another table's data under the name path
	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), "newt-x", nil)       // this table's own (decoupled) data
	fs.putEntry(GetTablePath(renameTestBucket, "ns", "newt-x"), "metadata", nil)

	require.NoError(t, runDeleteTable(t, m, fs, "ns", "newt"))

	assert.Nil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "newt-x"),
		"the table's own data location must be purged")
	assert.NotNil(t, fs.getEntry(GetTablePath(renameTestBucket, "ns", "newt"), "leftover"),
		"data under the reused name path must survive")
	marker := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "newt")
	require.NotNil(t, marker)
	for _, key := range markerKeys {
		_, present := marker.Extended[key]
		assert.Falsef(t, present, "catalog attribute %s must be cleared", key)
	}
}

// A table whose MetadataLocation resolves to an ancestor of its own name path
// (here the namespace root, e.g. from corrupt metadata) must not be deleted: a
// recursive purge of that data path would wipe sibling tables. The delete is
// refused and the namespace's other tables survive.
func TestDeleteTableRefusesAncestorDataPath(t *testing.T) {
	fs, m := startRenameManager(t)

	badMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "badt",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/metadata/v1.metadata.json",
	})
	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), "badt", map[string][]byte{ExtendedKeyMetadata: badMeta})

	require.Error(t, runDeleteTable(t, m, fs, "ns", "badt"))

	// The sibling table seeded by startRenameManager and its data must survive.
	assert.NotNil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t"),
		"sibling table marker must survive a refused delete")
	assert.NotNil(t, fs.getEntry(GetTablePath(renameTestBucket, "ns", "t"), "data"),
		"sibling table data must survive a refused delete")
}

// A normal colocated table (data under its own name path) is removed wholesale.
func TestDeleteTableColocatedRemovesData(t *testing.T) {
	fs, m := startRenameManager(t)

	require.NoError(t, runDeleteTable(t, m, fs, "ns", "t"))

	assert.Nil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t"), "colocated table entry must be deleted")
	assert.Nil(t, fs.getEntry(GetTablePath(renameTestBucket, "ns", "t"), "metadata"), "colocated table data must be deleted")
}
