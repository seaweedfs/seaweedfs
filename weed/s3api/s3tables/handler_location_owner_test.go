package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

// rootRaceClient lands a rival record over the same root the moment a write
// reaches the record at dir/name, the way a second create that read the root
// free before this record existed publishes its own.
type rootRaceClient struct {
	filer_pb.SeaweedFilerClient
	f        *s3tablestest.MemFiler
	dir      string
	name     string
	location string
	planted  bool
}

func (c *rootRaceClient) plant() {
	if c.planted {
		return
	}
	c.planted = true
	rival, _ := json.Marshal(tableMetadataInternal{Name: "rival", Namespace: "ns", Format: FormatIceberg, MetadataLocation: c.location})
	c.f.Put(c.dir, "rival", map[string][]byte{
		ExtendedKeyMetadata:  rival,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
		ExtendedKeyDeleting:  []byte(createClaim),
	})
}

func (c *rootRaceClient) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.UpdateEntry(ctx, req, opts...)
	if err == nil && req.Directory == c.dir && req.Entry.Name == c.name {
		c.plant()
	}
	return resp, err
}

// Two writers that both read a root free each check it again once their record
// is written, so the one that sees the other backs out instead of both landing.
func TestLocationRaceBacksOut(t *testing.T) {
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	location := renameLocation("ns/shared/metadata/v1.metadata.json")

	t.Run("create", func(t *testing.T) {
		fs, m := startRenameManager(t)
		client := &rootRaceClient{SeaweedFilerClient: fs.Client, f: fs, dir: nsPath, name: "mine", location: location}
		status, resp := runHandler(t, m, client, "CreateTable", &CreateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "mine", Format: FormatIceberg, MetadataLocation: location,
		})
		assert.Equal(t, http.StatusConflict, status, "%+v", resp)
		require.True(t, client.planted)
		if entry := fs.Get(nsPath, "mine"); entry != nil {
			assert.NotContains(t, entry.Extended, ExtendedKeyMetadata)
		}
	})

	t.Run("update", func(t *testing.T) {
		fs, m := startRenameManager(t)
		before := fs.Get(nsPath, "t").Extended[ExtendedKeyMetadata]
		client := &rootRaceClient{SeaweedFilerClient: fs.Client, f: fs, dir: nsPath, name: "t", location: location}
		status, resp := runHandler(t, m, client, "UpdateTable", &UpdateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", MetadataVersion: 4, MetadataLocation: location,
		})
		assert.Equal(t, http.StatusConflict, status, "%+v", resp)
		require.True(t, client.planted)
		assert.Equal(t, string(before), string(fs.Get(nsPath, "t").Extended[ExtendedKeyMetadata]))
	})

	t.Run("rename", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "c", Format: FormatIceberg})
		client := &rootRaceClient{SeaweedFilerClient: fs.Client, f: fs, dir: nsPath, name: "dest", location: renameLocation("ns/dest/metadata/v1.metadata.json")}
		status, resp := runHandler(t, m, client, "RenameTable", &RenameTableRequest{
			TableBucketARN: mustBucketARN(t), SourceNamespace: []string{"ns"}, SourceName: "c", DestNamespace: []string{"ns"}, DestName: "dest",
		})
		assert.Equal(t, http.StatusConflict, status, "%+v", resp)
		require.True(t, client.planted)
		assert.Contains(t, fs.Get(nsPath, "c").Extended, ExtendedKeyMetadata)
		if entry := fs.Get(nsPath, "dest"); entry != nil {
			assert.NotContains(t, entry.Extended, ExtendedKeyMetadata)
		}
	})
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
