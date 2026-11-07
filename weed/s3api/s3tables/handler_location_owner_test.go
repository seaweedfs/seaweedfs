package s3tables

import (
	"encoding/json"
	"net/http"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func renameLocation(rel string) string {
	return "s3://" + renameTestBucket + "/" + rel
}

// A table's files belong to one catalog record. A create, register, update or
// view naming a location at, under or above another record's files is refused,
// and so is one naming the files a renamed table left under its old name.
func TestLocationOwnedByOneRecord(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{TableBucketARN: bucketARN, Namespace: []string{"ns2"}})
	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{
		TableBucketARN:   bucketARN,
		Namespace:        []string{"ns"},
		Name:             "own",
		Format:           FormatIceberg,
		MetadataLocation: renameLocation("ns/own/metadata/v1.metadata.json"),
	})

	refused := func(t *testing.T, operation string, input interface{}) {
		t.Helper()
		status, resp := runHandler(t, m, fs.Client, operation, input)
		assert.Equal(t, http.StatusConflict, status, "%s: %+v", operation, resp)
		assert.Equal(t, ErrCodeConflict, resp.Type)
		assert.NotContains(t, resp.Message, "ns/t")
	}
	checkRefusals := func(t *testing.T) {
		for _, location := range []string{
			"ns/t/metadata/v3.metadata.json",
			"ns/t/data/part/metadata/v1.metadata.json",
			"ns/metadata/v1.metadata.json",
		} {
			refused(t, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "alias", Format: FormatLance, MetadataLocation: renameLocation(location)})
			refused(t, "RegisterTable", &RegisterTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "alias", MetadataLocation: renameLocation(location)})
			refused(t, "CreateView", &CreateViewRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "alias", MetadataLocation: renameLocation(location)})
			refused(t, "UpdateTable", &UpdateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "own", MetadataVersion: 2, MetadataLocation: renameLocation(location)})
		}
		assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "alias"))
	}

	t.Run("live table", checkRefusals)

	requireOK(t, m, fs, "RenameTable", &RenameTableRequest{
		TableBucketARN:  bucketARN,
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t",
	})
	t.Run("renamed table", func(t *testing.T) {
		checkRefusals(t)
		refused(t, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t", Format: FormatIceberg})
	})

	t.Run("free locations", func(t *testing.T) {
		requireOK(t, m, fs, "UpdateTable", &UpdateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "own", MetadataVersion: 2, MetadataLocation: renameLocation("ns/own/metadata/v2.metadata.json")})
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg, MetadataLocation: renameLocation("ns/t2/metadata/v1.metadata.json")})
		requireOK(t, m, fs, "RegisterTable", &RegisterTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t3", MetadataLocation: renameLocation("ns/t3/metadata/v1.metadata.json")})
	})
}

// A drop of a record that shares its files with another is refused, so the
// other record keeps them.
func TestDeleteTableKeepsSharedFiles(t *testing.T) {
	fs, m := startRenameManager(t)
	aliasMeta, err := json.Marshal(tableMetadataInternal{
		Name:             "alias",
		Namespace:        "ns",
		Format:           FormatIceberg,
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  3,
		MetadataLocation: renameLocation("ns/t/metadata/v3.metadata.json"),
	})
	require.NoError(t, err)
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "alias", map[string][]byte{
		ExtendedKeyMetadata:  aliasMeta,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})

	for _, name := range []string{"alias", "t"} {
		status, resp := runHandler(t, m, fs.Client, "DeleteTable", deleteTableRequest(t, name))
		assert.Equal(t, http.StatusConflict, status, "%s: %+v", name, resp)
	}
	tablePath := GetTablePath(renameTestBucket, "ns", "t")
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"))
	assert.NotNil(t, fs.Get(path.Join(tablePath, "metadata"), "v3.metadata.json"))
}

// A table that keeps its files elsewhere still owns its catalog entry: a view
// recorded at that entry would take it with the view's files on drop.
func TestLocationRefusesAnotherRecordsEntry(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "a", Format: FormatIceberg,
		MetadataLocation: renameLocation("ns/storage/metadata/v1.metadata.json"),
	})
	status, resp := runHandler(t, m, fs.Client, "CreateView", &CreateViewRequest{
		TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "v", MetadataLocation: renameLocation("ns/a/metadata/v1.metadata.json"),
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
}
