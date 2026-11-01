package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startEmptyBucket seeds a table bucket with nothing in it, not even a namespace.
func startEmptyBucket(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs := s3tablestest.Start(t)
	bucketMeta, err := json.Marshal(tableBucketMetadata{Name: renameTestBucket, OwnerAccountID: DefaultAccountID})
	require.NoError(t, err)
	fs.Put(TablesPath, renameTestBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
	})
	m := NewManager()
	m.SetTrusted(true)
	return fs, m
}

func deleteTableBucketRequest(t *testing.T) *DeleteTableBucketRequest {
	return &DeleteTableBucketRequest{TableBucketARN: mustBucketARN(t)}
}

// Deleting a namespace that holds renamed-away data leaves its directory behind
// with no record. Once the renamed table is dropped the data is gone, and the
// directory is an empty leftover that must not keep the bucket from being deleted.
func TestDeleteTableBucketRemovesEmptyLeftovers(t *testing.T) {
	fs, m := startDeletedNamespaceWithRenamedData(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	require.NotNil(t, fs.Get(bucketPath, "ns"), "the delete of ns keeps its directory, which is the leftover under test")

	requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
		Name:           "t",
	})
	requireOK(t, m, fs, "DeleteNamespace", &DeleteNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
	})
	require.Nil(t, fs.Get(keptDataDir(), keptDataFile), "dropping the renamed table purges the data the leftover kept")
	require.NotNil(t, fs.Get(bucketPath, "ns"), "the leftover directory is still in the bucket")

	requireOK(t, m, fs, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Nil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is deleted")
	assert.Nil(t, fs.Get(bucketPath, "ns"), "the empty leftover goes with it")
}

// No table refers to a file under a directory with no record, but deleting the
// bucket would destroy it without anyone deciding to. The bucket stays, and so
// does the file.
func TestDeleteTableBucketRefusesOrphanData(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	orphan := path.Join(bucketPath, "old", "t", "data")
	fs.Put(bucketPath, "old", nil)
	fs.Put(path.Join(bucketPath, "old"), "t", nil)
	fs.Put(path.Join(bucketPath, "old", "t"), "data", nil)
	fs.PutFile(orphan, keptDataFile, time.Now())

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
	assert.Contains(t, resp.Message, "directory old still holds files", "the answer names the directory that holds the file")
	assert.NotNil(t, fs.Get(orphan, keptDataFile), "the file must survive")
	assert.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
}

// pauseListClient runs pause just before the nth listing of dir, so a test can
// land a competing write at a chosen step of a delete's directory walk.
type pauseListClient struct {
	filer_pb.SeaweedFilerClient
	dir      string
	nth      int
	listings int
	pause    func()
}

func (c *pauseListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	if in.Directory == c.dir {
		c.listings++
		if c.listings == c.nth {
			c.pause()
		}
	}
	return c.SeaweedFilerClient.ListEntries(ctx, in, opts...)
}

func createNamespaceNsRequest(t *testing.T) *CreateNamespaceRequest {
	return &CreateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}
}

// A create could take a leftover directory back after the scan listed it, and a
// namespace with nothing in it holds no file for the walk to find, so the
// recursive delete would remove a namespace the create had already answered
// for. The delete's claim on the bucket turns the create away instead.
func TestDeleteTableBucketTurnsAwayATakeBackAfterTheScan(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	fs.Put(bucketPath, "ns", nil)
	var createStatus int
	var createResp S3TablesError
	// The scan lists the bucket again for the page after the leftover, so the
	// create lands once the scan has recorded it and before the delete walks it.
	client := &pauseListClient{SeaweedFilerClient: fs.Client, dir: bucketPath, nth: 2}
	client.pause = func() {
		createStatus, createResp = runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
	}

	status, resp := runHandler(t, m, client, "DeleteTableBucket", deleteTableBucketRequest(t))

	require.GreaterOrEqual(t, client.listings, 2, "the create has to land after the scan listed the leftover")
	assert.Equal(t, http.StatusConflict, createStatus, "%+v", createResp)
	assert.Equal(t, ErrCodeConflict, createResp.Type, "%+v", createResp)
	assert.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Nil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is deleted")
}

// A create that finds the bucket claimed by a delete is refused and writes
// nothing, as a create under a claimed namespace is.
func TestCreateNamespaceRefusedInClaimedBucket(t *testing.T) {
	fs, m := startEmptyBucket(t)
	plantDeleteClaim(fs, TablesPath, renameTestBucket)

	status, resp := runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"), "a refused create writes nothing")
}

// A create is authorized against the bucket it read. If that bucket is deleted
// and another is created under the same name before the create writes, the
// namespace would land in a bucket nobody authorized it for, carrying the first
// bucket's owner. The create checks the bucket by its creation time, reads the
// new one as the old one gone, and writes nothing into it.
func TestCreateNamespaceTreatsARecreatedBucketAsGone(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreatedMeta, err := json.Marshal(tableBucketMetadata{
		Name:           renameTestBucket,
		OwnerAccountID: "111122223333",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
	recreated := map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    recreatedMeta,
	}
	// The create looks its namespace up right before it writes; the bucket is
	// replaced there.
	fs.OnLookup = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnLookup = nil
		fs.Put(TablesPath, renameTestBucket, recreated)
	}

	status, resp := runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))

	assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchBucket, resp.Type, "%+v", resp)
	assert.Nil(t, fs.Get(bucketPath, "ns"), "nothing is written into the new bucket")
	require.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the new bucket stays")
	assert.Equal(t, recreated, fs.Get(TablesPath, renameTestBucket).Extended, "and is left as it was created")
}

// A delete is authorized against the bucket it read. If that bucket is deleted
// and another is created under the same name before the delete claims it, the
// claim and the removal would act on a bucket nobody authorized the delete for.
// The claim takes only the bucket with the creation time the delete read, so
// the new one reads as the old one gone and is left as it was.
func TestDeleteTableBucketTreatsARecreatedBucketAsGone(t *testing.T) {
	fs, m := startEmptyBucket(t)
	recreatedMeta, err := json.Marshal(tableBucketMetadata{
		Name:           renameTestBucket,
		OwnerAccountID: "111122223333",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
	recreated := map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    recreatedMeta,
	}
	// The claim is the delete's first write. The bucket is replaced as it lands,
	// so the write loses and its retry reads the new bucket.
	fs.BeforeUpdate = func() { fs.Put(TablesPath, renameTestBucket, recreated) }

	code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchBucket, resp.Type, "%+v", resp)
	require.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the new bucket stays")
	assert.Equal(t, recreated, fs.Get(TablesPath, renameTestBucket).Extended, "and is left as it was created")
}

// A delete that has scanned an empty bucket must not lose to a create that
// lands before the bucket is removed: the claim the delete holds since before
// the scan turns the create away, so no namespace is answered for and deleted.
func TestDeleteTableBucketTurnsAwayACreateAfterTheScan(t *testing.T) {
	fs, m := startEmptyBucket(t)
	var createStatus int
	var createResp S3TablesError
	fs.BeforeDelete = func() {
		createStatus, createResp = runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	require.NotZero(t, createStatus, "the create has to land between the scan and the delete")
	assert.Equal(t, http.StatusConflict, createStatus, "%+v", createResp)
	assert.Equal(t, ErrCodeConflict, createResp.Type, "%+v", createResp)
	assert.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Nil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is deleted")
}

// A create can pass the claim check before the delete claims the bucket, and
// then write a namespace that was not there when the delete scanned. The scan
// never saw it, and the recursive delete would remove it after the create had
// answered. The create's confirmation after its write finds the claim, so it
// backs out and reports the conflict instead of success.
func TestCreateNamespaceWrittenAfterTheScanBacksOut(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	reached := make(chan struct{})
	release := make(chan struct{})
	// The create looks its namespace up right after the claim check; hold it there.
	fs.OnLookup = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnLookup = nil
		close(reached)
		<-release
	}
	created := startHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
	waitForPause(t, reached, created)
	// The delete claims the bucket and scans it empty. The create writes only
	// now, before the delete removes the bucket.
	var createResult handlerResult
	fs.BeforeDelete = func() {
		close(release)
		createResult = <-created
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	require.NotZero(t, createResult.status, "the create has to write between the scan and the delete")
	assert.Equal(t, http.StatusConflict, createResult.status, "%+v", createResult.resp)
	assert.Equal(t, ErrCodeConflict, createResult.resp.Type, "%+v", createResult.resp)
	assert.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Nil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is deleted")
	assert.Nil(t, fs.Get(bucketPath, "ns"), "no namespace the create answered for is left behind")
}

// The claim a delete takes on the bucket can be replaced by a second delete,
// which lifts it when it refuses. A create then finds the bucket open, and the
// first delete, which scanned before any of that, must not remove the bucket
// on a claim it no longer holds: it would take the namespace the create
// answered for, and the file under it, with it. It answers 409 and leaves both.
func TestDeleteTableBucketRefusesAfterItsClaimIsLifted(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	nsPath := path.Join(bucketPath, "ns")
	var refusedStatus, createStatus int
	var refusedResp, createResp S3TablesError
	// The first delete has scanned an empty bucket when it reaches its removal.
	fs.BeforeDelete = func() {
		// A directory with a file, kept by a namespace that was deleted, turns up
		// and the second delete refuses on it, lifting its own claim.
		fs.Put(bucketPath, "ns", nil)
		fs.PutFile(nsPath, keptDataFile, time.Now())
		refusedStatus, refusedResp = runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))
		// The bucket is open again, so the create takes the directory back.
		createStatus, createResp = runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	require.NotZero(t, createStatus, "the create has to land between the first delete's scan and its removal")
	assert.Equal(t, http.StatusConflict, refusedStatus, "%+v", refusedResp)
	assert.Equal(t, ErrCodeBucketNotEmpty, refusedResp.Type, "%+v", refusedResp)
	assert.Equal(t, http.StatusOK, createStatus, "%+v", createResp)
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	require.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
	assert.NotContains(t, fs.Get(TablesPath, renameTestBucket).Extended, ExtendedKeyDeleting)
	assert.NotNil(t, fs.Get(bucketPath, "ns"), "the namespace the create answered for must survive")
	assert.NotNil(t, fs.Get(nsPath, keptDataFile), "the file under it must survive")
}

// The walk that finds a leftover directory empty is over before the bucket is
// removed, and a file can be written in between: an upload through the S3 API
// takes no claim. Nothing the removal does may take a file it was not shown, so
// it is refused at the directory that holds one, and a file written into the
// bucket itself makes the removal of the bucket refuse the same way.
func TestDeleteTableBucketKeepsAFileUploadedAfterTheWalk(t *testing.T) {
	for _, tc := range []struct {
		name string
		dir  func(bucketPath string) string
	}{
		{"into a leftover directory", func(bucketPath string) string { return path.Join(bucketPath, "old") }},
		{"into the bucket", func(bucketPath string) string { return bucketPath }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			bucketPath := GetTableBucketPath(renameTestBucket)
			fs.Put(bucketPath, "old", nil)
			uploadDir := tc.dir(bucketPath)
			fs.BeforeDelete = func() {
				fs.PutFile(uploadDir, keptDataFile, time.Now())
			}

			status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

			assert.Equal(t, http.StatusConflict, status, "%+v", resp)
			assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
			assert.NotNil(t, fs.Get(uploadDir, keptDataFile), "the uploaded file must survive")
			require.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
			assert.NotContains(t, fs.Get(TablesPath, renameTestBucket).Extended, ExtendedKeyDeleting, "a refused delete lifts its claim")
			if uploadDir != bucketPath {
				require.NotNil(t, fs.Get(bucketPath, "old"))
				assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "and its claim on the leftover")
			}
		})
	}
}

// A second delete can replace the first one's claim and lift it, and a create
// then takes a leftover directory back as a namespace after the first delete
// scanned it bare, and an empty table is created in it. Neither holds a file,
// so only the attributes the scan saw on the directory tell the removal it is
// no longer the same directory, and that has to be settled before anything
// under it goes: the delete refuses with a conflict and leaves both.
func TestDeleteTableBucketKeepsALeftoverTakenBackAfterItsClaimIsLifted(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	nsPath := path.Join(bucketPath, "ns")
	fs.Put(bucketPath, "ns", nil)
	var refusedStatus, createStatus, tableStatus int
	var refusedResp, createResp, tableResp S3TablesError
	fs.BeforeDelete = func() {
		// A file under another leftover makes the second delete refuse, and
		// lift its claim; the bucket is open again for the create.
		fs.Put(bucketPath, "old", nil)
		fs.PutFile(path.Join(bucketPath, "old"), keptDataFile, time.Now())
		refusedStatus, refusedResp = runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))
		createStatus, createResp = runHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
		tableStatus, tableResp = runHandler(t, m, fs.Client, "CreateTable", createTableRequest(t, "ns", "t"))
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	require.NotZero(t, tableStatus, "the creates have to land between the first delete's scan and its removal")
	assert.Equal(t, http.StatusConflict, refusedStatus, "%+v", refusedResp)
	assert.Equal(t, http.StatusOK, createStatus, "%+v", createResp)
	assert.Equal(t, http.StatusOK, tableStatus, "%+v", tableResp)
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	require.NotNil(t, fs.Get(bucketPath, "ns"), "the namespace the create answered for must survive")
	assert.Contains(t, fs.Get(bucketPath, "ns").Extended, ExtendedKeyMetadata)
	require.NotNil(t, fs.Get(nsPath, "t"), "the table the create answered for must survive")
	assert.Contains(t, fs.Get(nsPath, "t").Extended, ExtendedKeyMetadata)
	assert.NotNil(t, fs.Get(path.Join(nsPath, "t"), "data"), "and so must its data directory")
	assert.NotNil(t, fs.Get(path.Join(bucketPath, "old"), keptDataFile), "the file under the other leftover must survive")
	assert.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
}

// A catalog record under a leftover directory is not the leftover's to remove,
// whatever it holds: an empty table is still a table. The delete refuses at
// it, and lifts the claim it took on the leftover.
func TestDeleteTableBucketRefusesACatalogRecordInALeftover(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	leftover := path.Join(bucketPath, "old")
	fs.Put(bucketPath, "old", nil)
	fs.Put(leftover, "t", map[string][]byte{
		ExtendedKeyMetadata:  []byte("{}"),
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
	require.NotNil(t, fs.Get(leftover, "t"), "the record must survive")
	assert.Contains(t, fs.Get(leftover, "t").Extended, ExtendedKeyMetadata)
	require.NotNil(t, fs.Get(bucketPath, "old"))
	assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "a refused delete lifts its claim on the leftover")
	assert.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
}

// A namespace a create left marked unfinished must not wedge the name: the mark
// says its writer never confirmed it, so the next create takes it over, and
// tables can be created in it once it is confirmed.
func TestCreateNamespaceAdoptsAnUnfinishedNamespace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	plantCreateMark(fs, bucketPath, "ns")

	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))

	assert.NotContains(t, fs.Get(bucketPath, "ns").Extended, ExtendedKeyDeleting, "the adopted namespace ends confirmed")
	requireOK(t, m, fs, "CreateTable", createTableRequest(t, "ns", "t"))
}

// A table answers success once its namespace is open, and says nothing of the
// bucket, so a table created while the namespace's own create still has to
// confirm the bucket would survive as a table under a namespace that
// confirmation backs out. The namespace carries the unfinished mark until the
// confirmation has passed, and a create under it is turned away meanwhile.
func TestCreateTableRefusedInNamespaceStillBeingCreated(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	reached := make(chan struct{})
	release := make(chan struct{})
	// The create confirms the bucket right after writing the namespace; hold it there.
	fs.OnLookup = func(dir, name string) {
		if dir != TablesPath || name != renameTestBucket || fs.Get(bucketPath, "ns") == nil {
			return
		}
		fs.OnLookup = nil
		close(reached)
		<-release
	}
	created := startHandler(t, m, fs.Client, "CreateNamespace", createNamespaceNsRequest(t))
	waitForPause(t, reached, created)

	status, resp := runHandler(t, m, fs.Client, "CreateTable", createTableRequest(t, "ns", "t"))
	close(release)
	createResult := <-created

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	assert.Nil(t, fs.Get(path.Join(bucketPath, "ns"), "t"), "a refused create writes nothing")
	assert.Equal(t, http.StatusOK, createResult.status, "%+v", createResult.resp)
	assert.NotContains(t, fs.Get(bucketPath, "ns").Extended, ExtendedKeyDeleting, "a confirmed namespace carries no mark")
	requireOK(t, m, fs, "CreateTable", createTableRequest(t, "ns", "t"))
}

// A delete that claims the bucket between the namespace's confirmation and the
// end of its create must still see the namespace, or it would remove a
// namespace the create answered for. The mark does not hide it from the scan.
func TestDeleteTableBucketCountsANamespaceStillBeingCreated(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	plantCreateMark(fs, GetTableBucketPath(renameTestBucket), "ns")

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
	assert.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is kept")
}

// The claim on the bucket is lifted whenever the delete refuses, or the bucket
// would refuse every create; a claim left by a delete that died does not keep
// the next delete from running.
func TestDeleteTableBucketClaimOnTheBucket(t *testing.T) {
	lifted := func(t *testing.T, fs *s3tablestest.MemFiler, m *Manager) {
		t.Helper()
		status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))
		assert.Equal(t, http.StatusConflict, status, "%+v", resp)
		assert.NotContains(t, fs.Get(TablesPath, renameTestBucket).Extended, ExtendedKeyDeleting)
		requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
			TableBucketARN: mustBucketARN(t),
			Namespace:      []string{"other"},
		})
	}
	t.Run("lifted when a namespace is found", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
		lifted(t, fs, m)
	})
	t.Run("lifted when a directory holds files", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		bucketPath := GetTableBucketPath(renameTestBucket)
		fs.Put(bucketPath, "old", nil)
		fs.PutFile(path.Join(bucketPath, "old"), keptDataFile, time.Now())
		lifted(t, fs, m)
	})
	t.Run("replaces the claim of a delete that died", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		plantDeleteClaim(fs, TablesPath, renameTestBucket)

		requireOK(t, m, fs, "DeleteTableBucket", deleteTableBucketRequest(t))

		assert.Nil(t, fs.Get(TablesPath, renameTestBucket), "the bucket is deleted")
	})
}

// putDirectoryObject stores an S3 object with content on the directory entry
// dir/name, the way the S3 API stores a key that other keys sit under: with the
// prefix-object mark the filer expects on an object landing on a directory.
func putDirectoryObject(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) {
	t.Helper()
	_, err := fs.Client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mime: "application/octet-stream"},
			Content:     []byte("object"),
			Extended:    map[string][]byte{s3_constants.SeaweedFSPrefixObject: []byte("true")},
		},
	})
	require.NoError(t, err)
}

// markPrefixObject marks the directory dir/name as also holding an empty S3
// object, which leaves no trace but the mark, keeping its other attributes.
func markPrefixObject(fs *s3tablestest.MemFiler, dir, name string) {
	extended := map[string][]byte{}
	for key, value := range fs.Get(dir, name).Extended {
		extended[key] = value
	}
	extended[s3_constants.SeaweedFSPrefixObject] = []byte("true")
	fs.Put(dir, name, extended)
}

// beforeDeleteClient runs before just ahead of the first delete of target.
type beforeDeleteClient struct {
	filer_pb.SeaweedFilerClient
	target string
	before func()
}

func (c *beforeDeleteClient) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest, opts ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	if before := c.before; before != nil && path.Join(req.Directory, req.Name) == c.target {
		c.before = nil
		before()
	}
	return c.SeaweedFilerClient.DeleteEntry(ctx, req, opts...)
}

// An S3 object can be stored on a directory entry: a key other keys sit under,
// or an empty object that leaves nothing but a mark on the directory. Such a
// directory holds no file, yet it is user data, not a leftover: the delete
// refuses wherever it finds one and the object stays.
func TestDeleteTableBucketKeepsADirectoryBackedObject(t *testing.T) {
	bucketPath := GetTableBucketPath(renameTestBucket)
	leftover := path.Join(bucketPath, "old")
	refused := func(t *testing.T, code int, resp S3TablesError, wantType string) {
		t.Helper()
		assert.Equal(t, http.StatusConflict, code, "%+v", resp)
		assert.Equal(t, wantType, resp.Type, "%+v", resp)
	}

	t.Run("an empty object in the bucket", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", map[string][]byte{s3_constants.SeaweedFSPrefixObject: []byte("true")})

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		refused(t, code, resp, ErrCodeBucketNotEmpty)
		require.NotNil(t, fs.Get(bucketPath, "old"), "the object must survive")
		assert.True(t, fs.Get(bucketPath, "old").IsPrefixObject())
	})
	t.Run("an object under a leftover directory", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		putDirectoryObject(t, fs, leftover, "a")

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		refused(t, code, resp, ErrCodeBucketNotEmpty)
		require.NotNil(t, fs.Get(leftover, "a"), "the object must survive")
		assert.Equal(t, []byte("object"), fs.Get(leftover, "a").Content)
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "a refused delete lifts its claim on the leftover")
	})
	t.Run("a leftover stored as an object after the scan", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		fs.BeforeDelete = func() { putDirectoryObject(t, fs, bucketPath, "old") }

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		refused(t, code, resp, ErrCodeBucketNotEmpty)
		require.NotNil(t, fs.Get(bucketPath, "old"), "the object must survive")
		assert.Equal(t, []byte("object"), fs.Get(bucketPath, "old").Content)
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "no claim is taken on an object")
	})
	t.Run("a leftover stored as an object between the read and the write of its claim", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		// The claim read the bare directory; the object lands before its write,
		// which would otherwise put the bare directory back over it.
		fs.OnUpdate = func(dir, name string) {
			if dir != bucketPath || name != "old" {
				return
			}
			fs.OnUpdate = nil
			putDirectoryObject(t, fs, bucketPath, "old")
		}

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		refused(t, code, resp, ErrCodeBucketNotEmpty)
		require.NotNil(t, fs.Get(bucketPath, "old"), "the object must survive")
		assert.Equal(t, []byte("object"), fs.Get(bucketPath, "old").Content)
		assert.True(t, fs.Get(bucketPath, "old").IsPrefixObject())
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "no claim is taken on an object")
	})
	t.Run("a leftover marked as an empty object after the walk", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		client := &beforeDeleteClient{SeaweedFilerClient: fs.Client, target: leftover}
		client.before = func() { markPrefixObject(fs, bucketPath, "old") }

		code, resp := runHandler(t, m, client, "DeleteTableBucket", deleteTableBucketRequest(t))

		refused(t, code, resp, ErrCodeConflict)
		require.NotNil(t, fs.Get(bucketPath, "old"), "the object must survive")
		assert.True(t, fs.Get(bucketPath, "old").IsPrefixObject())
	})
}

// A directory the delete listed can be removed by another client and a file
// uploaded at its path before the delete reaches it. The file carries no
// attribute a directory would not, so only the directory's type and the claim
// written onto it tell the two apart: the delete refuses and the file stays,
// whether it replaced the leftover itself or a directory under it, and whether
// it landed before that directory was claimed or after.
func TestDeleteTableBucketKeepsAFileThatReplacedADirectory(t *testing.T) {
	bucketPath := GetTableBucketPath(renameTestBucket)
	leftover := path.Join(bucketPath, "old")
	kept := func(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) {
		t.Helper()
		entry := fs.Get(dir, name)
		require.NotNil(t, entry, "the uploaded file must survive")
		assert.False(t, entry.IsDirectory)
		assert.NotContains(t, entry.Extended, ExtendedKeyDeleting, "no claim is left on the file")
		require.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
	}

	t.Run("the leftover, after the scan", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		fs.BeforeDelete = func() { fs.PutFile(bucketPath, "old", time.Now()) }

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		assert.Equal(t, http.StatusConflict, code, "%+v", resp)
		assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
		kept(t, fs, bucketPath, "old")
	})
	t.Run("a directory under it, after the walk listed it", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		fs.Put(leftover, "child", nil)
		// The scan's look for files and the removal's walk each list old, then
		// old/child. The file takes child's place as the walk reaches it, after
		// the walk listed child as a directory and before child is claimed.
		client := &pauseListClient{SeaweedFilerClient: fs.Client, dir: path.Join(leftover, "child"), nth: 2}
		client.pause = func() { fs.PutFile(leftover, "child", time.Now()) }

		code, resp := runHandler(t, m, client, "DeleteTableBucket", deleteTableBucketRequest(t))

		require.Equal(t, 2, client.listings, "the file has to land during the removal's walk")
		assert.Equal(t, http.StatusConflict, code, "%+v", resp)
		assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
		kept(t, fs, leftover, "child")
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "a refused delete lifts its claim on the leftover")
	})
	t.Run("a directory under it, between the read and the write of its claim", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		fs.Put(leftover, "child", nil)
		// The filer refuses to write the claimed directory over the file and
		// says so only as an opaque failure.
		fs.OnUpdate = func(dir, name string) {
			if dir != leftover || name != "child" {
				return
			}
			fs.OnUpdate = nil
			fs.PutFile(leftover, "child", time.Now())
		}

		code, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

		assert.Equal(t, http.StatusConflict, code, "%+v", resp)
		assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
		kept(t, fs, leftover, "child")
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "a refused delete lifts its claim on the leftover")
	})
	t.Run("a directory under it, after its claim", func(t *testing.T) {
		fs, m := startEmptyBucket(t)
		fs.Put(bucketPath, "old", nil)
		fs.Put(leftover, "child", nil)
		client := &beforeDeleteClient{SeaweedFilerClient: fs.Client, target: path.Join(leftover, "child")}
		client.before = func() { fs.PutFile(leftover, "child", time.Now()) }

		code, resp := runHandler(t, m, client, "DeleteTableBucket", deleteTableBucketRequest(t))

		require.Nil(t, client.before, "the file has to land right before the delete")
		assert.Equal(t, http.StatusConflict, code, "%+v", resp)
		assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
		kept(t, fs, leftover, "child")
		assert.NotContains(t, fs.Get(bucketPath, "old").Extended, ExtendedKeyDeleting, "a refused delete lifts its claim on the leftover")
	})
}

// failClaimReleaseClient answers the first write that lifts a delete claim from
// target with Unavailable, the way a filer that is briefly unreachable would.
type failClaimReleaseClient struct {
	filer_pb.SeaweedFilerClient
	target string
	failed int
}

func (c *failClaimReleaseClient) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	if c.failed == 0 && req.Entry != nil && path.Join(req.Directory, req.Entry.Name) == c.target &&
		req.Entry.Extended[ExtendedKeyDeleting] == nil && len(req.ExpectedExtended[ExtendedKeyDeleting]) > 0 {
		c.failed++
		return nil, status.Error(codes.Unavailable, "filer unavailable")
	}
	return c.SeaweedFilerClient.UpdateEntry(ctx, req, opts...)
}

// A claim the delete fails to lift refuses every create in the bucket until the
// next delete replaces it, so a release the filer turns away once is retried.
func TestDeleteTableBucketRetriesTheClaimRelease(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	fs.Put(bucketPath, "old", nil)
	fs.PutFile(path.Join(bucketPath, "old"), keptDataFile, time.Now())
	client := &failClaimReleaseClient{SeaweedFilerClient: fs.Client, target: bucketPath}

	code, resp := runHandler(t, m, client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
	require.Equal(t, 1, client.failed, "the first release has to fail")
	assert.NotContains(t, fs.Get(TablesPath, renameTestBucket).Extended, ExtendedKeyDeleting, "a later attempt lifts the claim")
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
}

// The scan reads the bucket a page at a time, and the leftovers it collects
// along the way do not decide anything. With more bare directories than one page
// holds, the namespace sorted after them is on a later page: stopping at the
// first page would find nothing but leftovers and delete the bucket with it.
func TestDeleteTableBucketFindsANamespacePastLeftovers(t *testing.T) {
	fs, m := startEmptyBucket(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	for i := 0; i <= 10; i++ {
		fs.Put(bucketPath, fmt.Sprintf("a%02d", i), nil)
	}
	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"zz"},
	})

	status, resp := runHandler(t, m, fs.Client, "DeleteTableBucket", deleteTableBucketRequest(t))

	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeBucketNotEmpty, resp.Type, "%+v", resp)
	assert.NotNil(t, fs.Get(bucketPath, "zz"), "the namespace must survive")
	assert.NotNil(t, fs.Get(TablesPath, renameTestBucket), "the bucket must survive")
}
