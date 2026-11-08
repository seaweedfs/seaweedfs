package s3tables

import (
	"context"
	"encoding/json"
	"errors"
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
	t.Run("renamed table", checkRefusals)

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
	then     func()
}

func (c *rootRaceClient) plant() {
	if c.planted {
		return
	}
	c.planted = true
	if c.then != nil {
		defer c.then()
	}
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

// listFailClient fails every listing once a write reaches the record at
// dir/name, so the check that follows the write cannot run.
type listFailClient struct {
	filer_pb.SeaweedFilerClient
	dir     string
	name    string
	failing bool
}

func (c *listFailClient) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.UpdateEntry(ctx, req, opts...)
	if err == nil && req.Directory == c.dir && req.Entry.Name == c.name {
		c.failing = true
	}
	return resp, err
}

func (c *listFailClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	if c.failing {
		return nil, errors.New("listing unavailable")
	}
	return c.SeaweedFilerClient.ListEntries(ctx, in, opts...)
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
		assert.NotContains(t, fs.Get(nsPath, "t").Extended, ExtendedKeyMoving)
	})

	// A rival reading the old root while the move is checked still finds it
	// held, so a move that backs out never hands back a root someone took.
	for _, operation := range []string{"UpdateTable", "UpdateView"} {
		t.Run(operation+" keeps its old root while moving", func(t *testing.T) {
			fs, m := startRenameManager(t)
			oldLocation := renameLocation("ns/old/metadata/v1.metadata.json")
			var update interface{}
			if operation == "UpdateTable" {
				requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", Format: FormatIceberg, MetadataLocation: oldLocation})
				update = &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", MetadataVersion: 2, MetadataLocation: location}
			} else {
				requireOK(t, m, fs, "CreateView", &CreateViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", MetadataLocation: oldLocation})
				update = &UpdateViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", MetadataVersion: 2, MetadataLocation: location}
			}
			before := fs.Get(nsPath, "a").Extended[ExtendedKeyMetadata]
			var oldRootErr error
			client := &rootRaceClient{SeaweedFilerClient: fs.Client, f: fs, dir: nsPath, name: "a", location: location}
			client.then = func() {
				oldRootErr = checkLocationUnclaimed(context.Background(), fs.Client, renameTestBucket, path.Join(nsPath, "b"), CatalogRecordRoot(renameTestBucket, path.Join(nsPath, "b"), oldLocation, ""))
			}
			status, resp := runHandler(t, m, client, operation, update)
			assert.Equal(t, http.StatusConflict, status, "%+v", resp)
			require.True(t, client.planted)
			assert.ErrorIs(t, oldRootErr, ErrLocationInUse)
			assert.Equal(t, string(before), string(fs.Get(nsPath, "a").Extended[ExtendedKeyMetadata]))
			assert.NotContains(t, fs.Get(nsPath, "a").Extended, ExtendedKeyMoving)
		})
	}

	// Another update of the record landing while a move is checked cannot
	// leave the record on the root a racing create took.
	for _, other := range []string{"ns/shared/metadata/v2.metadata.json", "ns/old/metadata/v2.metadata.json"} {
		t.Run("update moving under an update to "+other, func(t *testing.T) {
			fs, m := startRenameManager(t)
			requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", Format: FormatIceberg, MetadataLocation: renameLocation("ns/old/metadata/v1.metadata.json")})
			client := &rootRaceClient{SeaweedFilerClient: fs.Client, f: fs, dir: nsPath, name: "a", location: location}
			client.then = func() {
				runHandler(t, m, fs.Client, "UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", MetadataVersion: 3, MetadataLocation: renameLocation(other)})
			}
			status, resp := runHandler(t, m, client, "UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "a", MetadataVersion: 2, MetadataLocation: location})
			assert.Equal(t, http.StatusConflict, status, "%+v", resp)
			require.True(t, client.planted)
			assert.False(t, RootClaimed(mustRoots(t, fs), path.Join(nsPath, "rival"), path.Join(nsPath, "shared")))
			assert.NotContains(t, fs.Get(nsPath, "a").Extended, ExtendedKeyMoving)
		})
	}

	t.Run("update with a failed check", func(t *testing.T) {
		fs, m := startRenameManager(t)
		before := fs.Get(nsPath, "t").Extended[ExtendedKeyMetadata]
		client := &listFailClient{SeaweedFilerClient: fs.Client, dir: nsPath, name: "t"}
		status, resp := runHandler(t, m, client, "UpdateTable", &UpdateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", MetadataVersion: 4, MetadataLocation: location,
		})
		assert.NotEqual(t, http.StatusOK, status, "%+v", resp)
		require.True(t, client.failing)
		assert.Equal(t, string(before), string(fs.Get(nsPath, "t").Extended[ExtendedKeyMetadata]))
		assert.NotContains(t, fs.Get(nsPath, "t").Extended, ExtendedKeyMoving)
	})

	t.Run("rename", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "c", Format: FormatIceberg})
		dropWarehouseLocation(t, fs, nsPath, "c")
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

// A move reserves its new root on the record, so a second move of the record
// waits for it, and an update that keeps the root clears a reservation a
// crashed move left behind.
func TestMoveReservationLeftBehind(t *testing.T) {
	fs, m := startRenameManager(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	bucketARN := mustBucketARN(t)
	entry := fs.Get(nsPath, "t")
	leftover, _ := json.Marshal(tableMetadataInternal{Name: "t", Namespace: "ns", MetadataLocation: renameLocation("ns/elsewhere/metadata/v4.metadata.json")})
	entry.Extended[ExtendedKeyMoving] = leftover
	fs.Put(nsPath, "t", entry.Extended)

	refused := renameLocation("ns/moved/metadata/v1.metadata.json")
	assert.True(t, RootClaimed(mustRoots(t, fs), path.Join(nsPath, "other"), path.Join(nsPath, "elsewhere")))
	status, resp := runHandler(t, m, fs.Client, "UpdateTable", &UpdateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t", MetadataVersion: 4, MetadataLocation: refused})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)

	requireOK(t, m, fs, "UpdateTable", &UpdateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t", MetadataVersion: 4, MetadataLocation: renameLocation("ns/t/metadata/v4.metadata.json")})
	assert.NotContains(t, fs.Get(nsPath, "t").Extended, ExtendedKeyMoving)
	assert.False(t, RootClaimed(mustRoots(t, fs), path.Join(nsPath, "other"), path.Join(nsPath, "elsewhere")))

	requireOK(t, m, fs, "UpdateTable", &UpdateTableRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t", MetadataVersion: 5, MetadataLocation: refused})
	assert.NotContains(t, fs.Get(nsPath, "t").Extended, ExtendedKeyMoving)
}

// recordRoot returns the directory the record at dir/name keeps its files under.
func recordRoot(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) string {
	t.Helper()
	entry := fs.Get(dir, name)
	require.NotNil(t, entry)
	var metadata tableMetadataInternal
	require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
	return metadata.root(renameTestBucket, path.Join(dir, name))
}

// dropWarehouseLocation turns the record at dir/name into one written before
// tables got a directory of their own, which keeps its files under its path.
func dropWarehouseLocation(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) {
	t.Helper()
	entry := fs.Get(dir, name)
	require.NotNil(t, entry)
	var metadata tableMetadataInternal
	require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
	metadata.WarehouseLocation = ""
	data, err := json.Marshal(&metadata)
	require.NoError(t, err)
	entry.Extended[ExtendedKeyMetadata] = data
}

func mustRoots(t *testing.T, fs *s3tablestest.MemFiler) map[string][]string {
	t.Helper()
	roots, err := CatalogRecordRoots(context.Background(), fs.Client, renameTestBucket)
	require.NoError(t, err)
	return roots
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

// A record taken away by a rename or a delete while a move holds its
// reservation leaves the old name free: the move can no longer clear the
// reservation once the record is gone.
func TestRecordGoneDropsItsMoveReservation(t *testing.T) {
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	reserve := func(t *testing.T, fs *s3tablestest.MemFiler, name string) {
		t.Helper()
		moving, _ := json.Marshal(tableMetadataInternal{Name: name, Namespace: "ns", Format: FormatIceberg, MetadataLocation: renameLocation("ns/target/metadata/v9.metadata.json")})
		fs.Get(nsPath, name).Extended[ExtendedKeyMoving] = moving
	}
	reuse := func(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, name string) {
		t.Helper()
		assert.NotContains(t, fs.Get(nsPath, name).Extended, ExtendedKeyMoving)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: name, Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/fresh-" + name + "/metadata/v1.metadata.json"),
		})
	}

	t.Run("rename", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "r", Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/r-files/metadata/v1.metadata.json"),
		})
		reserve(t, fs, "r")
		requireOK(t, m, fs, "RenameTable", &RenameTableRequest{
			TableBucketARN: mustBucketARN(t), SourceNamespace: []string{"ns"}, SourceName: "r", DestNamespace: []string{"ns"}, DestName: "u",
		})
		assert.NotContains(t, fs.Get(nsPath, "u").Extended, ExtendedKeyMoving)
		reuse(t, m, fs, "r")
	})

	t.Run("delete", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{
			TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "d", Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/d-files/metadata/v1.metadata.json"),
		})
		reserve(t, fs, "d")
		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "d"})
		reuse(t, m, fs, "d")
	})
}
