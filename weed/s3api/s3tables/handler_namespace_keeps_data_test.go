package s3tables

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// replaceAfterLookupClient swaps the entry at dir/name for a file as soon as
// its nth lookup has returned, so the next read finds a file where the one
// before it found a directory.
type replaceAfterLookupClient struct {
	filer_pb.SeaweedFilerClient
	f       *s3tablestest.MemFiler
	dir     string
	name    string
	nth     int
	lookups int
}

func (c *replaceAfterLookupClient) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.LookupDirectoryEntry(ctx, req, opts...)
	if req.Directory == c.dir && req.Name == c.name {
		c.lookups++
		if c.lookups == c.nth {
			c.f.PutFile(c.dir, c.name, time.Now())
		}
	}
	return resp, err
}

const keptDataFile = "part-0.parquet"

// keptDataDir is where the renamed table's data stays: a rename is catalog-only,
// so the destination keeps pointing at the original table directory.
func keptDataDir() string {
	return path.Join(GetTablePath(renameTestBucket, "ns", "t"), "data")
}

func listedNamespaces(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) []string {
	t.Helper()
	resp := &ListNamespacesResponse{}
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListNamespaces",
		&ListNamespacesRequest{TableBucketARN: mustBucketARN(t)}, resp, ""))
	names := make([]string, 0, len(resp.Namespaces))
	for _, namespace := range resp.Namespaces {
		names = append(names, flattenNamespace(namespace.Namespace))
	}
	return names
}

// renameTableToNs2 moves ns/t to ns2/t, which leaves ns/t behind as a directory
// without a record.
func renameTableToNs2(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
	t.Helper()
	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
	})
	requireOK(t, m, fs, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t",
	})
	require.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "the renamed-away directory is the leftover under test")
}

// startDeletedNamespaceWithRenamedData renames ns/t to ns2/t, then deletes ns.
// The table's data file sits under ns/t, where the renamed table still reads it.
func startDeletedNamespaceWithRenamedData(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startRenameManager(t)
	fs.PutFile(keptDataDir(), keptDataFile, time.Now())
	renameTableToNs2(t, m, fs)
	requireOK(t, m, fs, "DeleteNamespace", deleteNamespaceRequest(t))
	return fs, m
}

// A rename leaves the source table directory behind, holding the data the
// renamed table still points at. Deleting the namespace must not take it: the
// namespace stops existing, and the directory stays with the data under it.
func TestDeleteNamespaceKeepsRenamedTableData(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)

	status, resp := runHandler(t, m, fs.Client, "GetNamespace", &GetNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})
	assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
	assert.Equal(t, []string{"ns2"}, listedNamespaces(t, m, fs))

	assert.NotNil(t, fs.Get(keptDataDir(), keptDataFile), "the renamed table's data must survive")
	kept := fs.Get(GetTableBucketPath(renameTestBucket), "ns")
	require.NotNil(t, kept, "the directory holding the data must stay")
	for _, key := range s3tablesExtendedKeys {
		assert.NotContains(t, kept.Extended, key, "the kept directory must carry no catalog attribute, claim included")
	}

	status, resp = runHandler(t, m, fs.Client, "GetTable", &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
		Name:           "t",
	})
	assert.Equal(t, http.StatusOK, status, "the renamed table must still resolve: %+v", resp)
}

// The kept directory is not a namespace, so every call that names it answers
// NoSuchNamespace and writes nothing to it.
func TestKeptDirectoryIsNoSuchNamespace(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)
	before := extendedOf(t, fs, GetTableBucketPath(renameTestBucket), "ns")

	ops := []struct {
		operation string
		input     interface{}
	}{
		{"GetNamespace", &GetNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}},
		{"UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Properties: map[string]string{"k": "v"}}},
		{"DeleteNamespace", deleteNamespaceRequest(t)},
	}
	for _, op := range ops {
		t.Run(op.operation, func(t *testing.T) {
			status, resp := runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
		})
	}

	assert.Equal(t, before, extendedOf(t, fs, GetTableBucketPath(renameTestBucket), "ns"), "the kept directory must not be written to")
	assert.NotNil(t, fs.Get(keptDataDir(), keptDataFile))
}

// Directories with no file under them at any depth are not worth keeping, so a
// namespace left with only those is removed whole, as before.
func TestDeleteNamespaceRemovesEmptyLeftovers(t *testing.T) {
	fs, m := startRenameManager(t)
	renameTableToNs2(t, m, fs)

	requireOK(t, m, fs, "DeleteNamespace", deleteNamespaceRequest(t))

	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"), "a namespace holding no file is removed")
	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"))
	assert.Equal(t, []string{"ns2"}, listedNamespaces(t, m, fs))
}

// A file that sits deep under a leftover directory, past other empty branches,
// is still found.
func TestDeleteNamespaceKeepsDeeplyNestedData(t *testing.T) {
	fs, m := startRenameManager(t)
	renameTableToNs2(t, m, fs)
	leftover := GetTablePath(renameTestBucket, "ns", "leftover")
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "leftover", nil)
	fs.Put(leftover, "empty", nil)
	fs.Put(leftover, "year=2026", nil)
	fs.Put(path.Join(leftover, "year=2026"), "month=10", nil)
	fs.PutFile(path.Join(leftover, "year=2026", "month=10"), keptDataFile, time.Now())

	requireOK(t, m, fs, "DeleteNamespace", deleteNamespaceRequest(t))

	assert.NotNil(t, fs.Get(path.Join(leftover, "year=2026", "month=10"), keptDataFile))
	assert.Equal(t, []string{"ns2"}, listedNamespaces(t, m, fs))
}

// The walk follows a directory's later pages. With more children than one page
// holds, the only file sits past the first page — directly in the leftover, or
// under a directory listed on the second page — and missing it would let the
// delete take the data.
func TestDeleteNamespaceKeepsDataPastTheFirstListingPage(t *testing.T) {
	const children = dirListPageSize + 10
	cases := []struct {
		name string
		put  func(fs *s3tablestest.MemFiler, leftover string)
	}{
		{"file on the second page", func(fs *s3tablestest.MemFiler, leftover string) {
			fs.PutFile(leftover, "z-"+keptDataFile, time.Now())
		}},
		{"file under a directory on the second page", func(fs *s3tablestest.MemFiler, leftover string) {
			fs.PutFile(path.Join(leftover, fmt.Sprintf("d%04d", children-1)), keptDataFile, time.Now())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, m := startRenameManager(t)
			renameTableToNs2(t, m, fs)
			leftover := GetTablePath(renameTestBucket, "ns", "wide")
			fs.Put(GetNamespacePath(renameTestBucket, "ns"), "wide", nil)
			for i := 0; i < children; i++ {
				fs.Put(leftover, fmt.Sprintf("d%04d", i), nil)
			}
			tc.put(fs, leftover)

			requireOK(t, m, fs, "DeleteNamespace", deleteNamespaceRequest(t))

			require.NotNil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"), "the directory holding the data must stay")
			assert.Equal(t, []string{"ns2"}, listedNamespaces(t, m, fs))
		})
	}
}

// listContextClient remembers the context of every listing it serves.
type listContextClient struct {
	filer_pb.SeaweedFilerClient
	contexts []context.Context
}

func (c *listContextClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.contexts = append(c.contexts, ctx)
	return c.SeaweedFilerClient.ListEntries(ctx, in, opts...)
}

// The walk stops at the first file with a page still unread. Its listings must
// end with the call, not with the request, so the filer stops producing that page.
func TestDirHoldsFilesEndsItsListings(t *testing.T) {
	fs, m := startRenameManager(t)
	leftover := GetTablePath(renameTestBucket, "ns", "leftover")
	fs.Put(leftover, "a-dir", nil)
	fs.PutFile(leftover, "b-file", time.Now())
	fs.PutFile(leftover, "c-file", time.Now())
	client := &listContextClient{SeaweedFilerClient: fs.Client}

	holds, err := m.handler.dirHoldsFiles(context.Background(), client, leftover)

	require.NoError(t, err)
	assert.True(t, holds)
	require.NotEmpty(t, client.contexts)
	for _, ctx := range client.contexts {
		assert.Error(t, ctx.Err(), "a listing must not outlive the walk")
	}
}

// A table still in the namespace keeps it from being deleted, even with a
// leftover directory holding data beside it.
func TestDeleteNamespaceWithTableIsStillNotEmpty(t *testing.T) {
	fs, m := startRenameManager(t)
	fs.PutFile(keptDataDir(), keptDataFile, time.Now())
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "leftover", nil)
	fs.PutFile(GetTablePath(renameTestBucket, "ns", "leftover"), keptDataFile, time.Now())

	status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeNamespaceNotEmpty, resp.Type, "%+v", resp)

	namespace := fs.Get(GetTableBucketPath(renameTestBucket), "ns")
	require.NotNil(t, namespace)
	assert.Contains(t, namespace.Extended, ExtendedKeyMetadata, "a refused delete leaves the namespace a namespace")
	assert.NotContains(t, namespace.Extended, ExtendedKeyDeleting, "a refused delete releases its claim")
}

// The name of a deleted namespace is free again even though its directory is
// still there: the create takes the directory back, with no claim stuck on it,
// and the data under it is untouched.
func TestCreateNamespaceTakesBackAKeptDirectory(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)

	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})
	requireOK(t, m, fs, "CreateTable", createTableRequest(t, "ns", "x"))

	assert.ElementsMatch(t, []string{"ns", "ns2"}, listedNamespaces(t, m, fs))
	assert.NotNil(t, fs.Get(keptDataDir(), keptDataFile), "the renamed table's data must survive")
	status, resp := runHandler(t, m, fs.Client, "GetTable", &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
		Name:           "t",
	})
	assert.Equal(t, http.StatusOK, status, "%+v", resp)

	// The directory is a real namespace again, so a second create is refused.
	status, resp = runHandler(t, m, fs.Client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeNamespaceAlreadyExists, resp.Type, "%+v", resp)
}

// A file that replaces the leftover directory while the create is taking it
// over must not end up tagged as a namespace: the create checks the type on the
// entry it writes, and the filer refuses to write a directory over the file.
func TestCreateNamespaceDoesNotTagAFileThatReplacesTheDirectory(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	// The create looks the name up twice: once to see whether it exists, and
	// once to read the leftover it takes over. The file lands after the second.
	client := &replaceAfterLookupClient{SeaweedFilerClient: fs.Client, f: fs, dir: bucketPath, name: "ns", nth: 2}

	status, resp := runHandler(t, m, client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})

	require.GreaterOrEqual(t, client.lookups, 2, "the replacement has to land after the create's second read")
	assert.NotEqual(t, http.StatusOK, status, "%+v", resp)
	file := fs.Get(bucketPath, "ns")
	require.NotNil(t, file)
	assert.False(t, file.IsDirectory)
	assert.NotContains(t, file.Extended, ExtendedKeyMetadata, "the file must not be tagged as a namespace")
}

// A file is not a leftover directory to take over: the create keeps refusing a
// name a file already holds.
func TestCreateNamespaceRefusesAFileInTheWay(t *testing.T) {
	fs, m := startRenameManager(t)
	fs.PutFile(GetTableBucketPath(renameTestBucket), "occupied", time.Now())

	status, resp := runHandler(t, m, fs.Client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"occupied"},
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeNamespaceAlreadyExists, resp.Type, "%+v", resp)
	assert.Empty(t, fs.Get(GetTableBucketPath(renameTestBucket), "occupied").Extended, "the file must not be tagged as a namespace")
}
