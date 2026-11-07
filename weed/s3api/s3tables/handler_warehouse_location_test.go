package s3tables

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// A table created without a location keeps its files in a directory of its
// own, so a rename leaves nothing under the old name and the name can be used
// again without either table reaching the other's files.
func TestCreateTableWithoutLocationGetsItsOwnDirectory(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")

	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "w", Format: FormatIceberg})
	var table GetTableResponse
	getTable(t, m, fs, bucketARN, "w", &table)
	assert.Empty(t, table.MetadataLocation)
	assert.True(t, strings.HasPrefix(table.WarehouseLocation, renameLocation("ns/w-")), table.WarehouseLocation)

	firstRoot := recordRoot(t, fs, nsPath, "w")
	assert.Equal(t, path.Join(TablesPath, strings.TrimPrefix(table.WarehouseLocation, "s3://")), firstRoot)
	require.NotNil(t, fs.Get(firstRoot, "data"))
	fs.PutFile(path.Join(firstRoot, "data"), "part-0.parquet", time.Now())
	assert.Nil(t, fs.Get(path.Join(nsPath, "w"), "data"), "the catalog entry holds no files")

	requireOK(t, m, fs, "RenameTable", &RenameTableRequest{TableBucketARN: bucketARN, SourceNamespace: []string{"ns"}, SourceName: "w", DestNamespace: []string{"ns"}, DestName: "moved"})
	assert.Equal(t, firstRoot, recordRoot(t, fs, nsPath, "moved"), "a rename keeps the directory")

	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "w", Format: FormatIceberg})
	secondRoot := recordRoot(t, fs, nsPath, "w")
	assert.NotEqual(t, firstRoot, secondRoot)
	assert.False(t, PathsOverlap(firstRoot, secondRoot))

	requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "w"})
	assert.NotNil(t, fs.Get(path.Join(firstRoot, "data"), "part-0.parquet"), "deleting the new table leaves the renamed one's files")
	requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "moved"})
	assert.Nil(t, fs.Get(path.Dir(firstRoot), path.Base(firstRoot)), "deleting the renamed table purges its directory")
}

// A Lance dataset stays at its name path, where Lance clients and the vector
// engine look for it.
func TestCreateLanceTableWithoutLocationKeepsItsPath(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "l", Format: FormatLance})
	var table GetTableResponse
	getTable(t, m, fs, bucketARN, "l", &table)
	assert.Empty(t, table.MetadataLocation)
	assert.Empty(t, table.WarehouseLocation)
	assert.Equal(t, GetTablePath(renameTestBucket, "ns", "l"), recordRoot(t, fs, GetNamespacePath(renameTestBucket, "ns"), "l"))
}

// Records written before tables got their own directory carry no warehouse
// location and keep resolving to their name path, so nothing has to be
// rewritten when a gateway is upgraded.
func TestRecordWithoutWarehouseLocationKeepsItsPath(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "old", Format: FormatIceberg})
	dropWarehouseLocation(t, fs, nsPath, "old")
	assert.Equal(t, path.Join(nsPath, "old"), recordRoot(t, fs, nsPath, "old"))
	assert.Equal(t, path.Join(nsPath, "t"), recordRoot(t, fs, nsPath, "t"), "a metadata location still decides the root")

	status, resp := runHandler(t, m, fs.Client, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "alias", Format: FormatIceberg, MetadataLocation: renameLocation("ns/old/metadata/v1.metadata.json")})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
}

// The first commit of a table writes its metadata into the assigned directory,
// which is the root the record already holds, so it scans no other record.
func TestFirstCommitIntoWarehouseLocationKeepsTheRoot(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketARN := mustBucketARN(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "w", Format: FormatIceberg})
	var table GetTableResponse
	getTable(t, m, fs, bucketARN, "w", &table)
	root := recordRoot(t, fs, nsPath, "w")

	status, resp := runHandler(t, m, &noListClient{SeaweedFilerClient: fs.Client}, "UpdateTable", &UpdateTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "w", MetadataVersion: 1,
		MetadataLocation: table.WarehouseLocation + "/metadata/v1.metadata.json",
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Equal(t, root, recordRoot(t, fs, nsPath, "w"))
	assert.NotContains(t, fs.Get(nsPath, "w").Extended, ExtendedKeyMoving)
}

// A create that backs out after making the table's directory removes it, so
// failed creates leave nothing behind under the namespace.
func TestBackedOutCreateRemovesItsDirectory(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	bucketPath := GetTableBucketPath(renameTestBucket)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	recreated := recreatedNamespace(t, "ns")
	swapped := false
	fs.OnLookup = func(dir, name string) {
		switch {
		case dir == nsPath && name == "t" && !swapped:
			swapped = true
			fs.Put(bucketPath, "ns", recreated)
		case path.Dir(dir) == nsPath && name == "data":
			// The filer makes the parents of an entry it creates.
			fs.Put(nsPath, path.Base(dir), nil)
		}
	}

	code, resp := runHandler(t, m, fs.Client, "CreateTable", createTableRequest(t, "ns", "t"))
	require.Equal(t, http.StatusNotFound, code, "%+v", resp)
	fs.OnLookup = nil

	var left []string
	require.NoError(t, eachDirEntry(context.Background(), fs.Client, nsPath, func(entry *filer_pb.Entry) bool {
		left = append(left, entry.Name)
		return true
	}))
	assert.Empty(t, left)
}

func getTable(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, bucketARN, name string, out *GetTableResponse) {
	t.Helper()
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "GetTable", &GetTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: name}, out, ""))
}

type noListClient struct {
	filer_pb.SeaweedFilerClient
}

func (c *noListClient) ListEntries(context.Context, *filer_pb.ListEntriesRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return nil, errors.New("listing unavailable")
}
