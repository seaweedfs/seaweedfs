package s3tables

import (
	"context"
	"net/http"
	"path"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// lateEntry is the name every create in these tests tries to add.
const lateEntry = "late"

// pausedCreateClient holds the first CreateEntry for target until release is
// closed, after closing reached, so a test can land another operation after a
// create has passed its checks and before it writes anything.
type pausedCreateClient struct {
	filer_pb.SeaweedFilerClient
	target  string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPausedCreateClient(client filer_pb.SeaweedFilerClient, target string) *pausedCreateClient {
	return &pausedCreateClient{
		SeaweedFilerClient: client,
		target:             target,
		reached:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (c *pausedCreateClient) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	if req.Entry != nil && path.Join(req.Directory, req.Entry.Name) == c.target {
		c.once.Do(func() {
			close(c.reached)
			<-c.release
		})
	}
	return c.SeaweedFilerClient.CreateEntry(ctx, req, opts...)
}

// pausedAfterWriteClient holds the create that has just written the first
// update to target until release is closed, after closing reached, so a test
// can land other requests after a create wrote its record and before it checks
// its parent.
type pausedAfterWriteClient struct {
	filer_pb.SeaweedFilerClient
	target  string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPausedAfterWriteClient(client filer_pb.SeaweedFilerClient, target string) *pausedAfterWriteClient {
	return &pausedAfterWriteClient{
		SeaweedFilerClient: client,
		target:             target,
		reached:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (c *pausedAfterWriteClient) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	resp, err := c.SeaweedFilerClient.UpdateEntry(ctx, req, opts...)
	if err == nil && req.Entry != nil && path.Join(req.Directory, req.Entry.Name) == c.target {
		c.once.Do(func() {
			close(c.reached)
			<-c.release
		})
	}
	return resp, err
}

type handlerResult struct {
	status int
	resp   S3TablesError
}

func startHandler(t *testing.T, m *Manager, client filer_pb.SeaweedFilerClient, operation string, input interface{}) <-chan handlerResult {
	t.Helper()
	done := make(chan handlerResult, 1)
	go func() {
		status, resp := runHandler(t, m, client, operation, input)
		done <- handlerResult{status, resp}
	}()
	return done
}

// waitForPause returns once the create is held. A create that ends before that
// fails the test instead of hanging it.
func waitForPause(t *testing.T, reached <-chan struct{}, created <-chan handlerResult) {
	t.Helper()
	select {
	case <-reached:
	case early := <-created:
		t.Fatalf("the create ended before it was held: %+v", early)
	}
}

func dropSeedTable(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) {
	t.Helper()
	require.NoError(t, runDeleteTable(t, m, fs, "ns", "t"))
}

// plantDeleteClaim puts a delete's claim on an entry, the way a delete that is
// still scanning leaves it.
func plantDeleteClaim(fs *s3tablestest.MemFiler, dir, name string) {
	extended := map[string][]byte{}
	for key, value := range fs.Get(dir, name).Extended {
		extended[key] = value
	}
	extended[ExtendedKeyDeleting] = []byte("planted-token")
	fs.Put(dir, name, extended)
}

func createTableRequest(t *testing.T, namespace, name string) *CreateTableRequest {
	return &CreateTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
		Name:           name,
		Format:         FormatIceberg,
	}
}

// createOp is one of the operations that add an entry to a namespace, with the
// read that loads the entry back and the error code that read gives for an
// entry that is not there.
type createOp struct {
	operation   string
	input       interface{}
	read        string
	readInput   interface{}
	readMissing string
}

func getLateTableRequest(t *testing.T) *GetTableRequest {
	return &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           lateEntry,
	}
}

func createTableOp(t *testing.T) createOp {
	t.Helper()
	return createOp{
		operation:   "CreateTable",
		input:       createTableRequest(t, "ns", lateEntry),
		read:        "GetTable",
		readInput:   getLateTableRequest(t),
		readMissing: ErrCodeNoSuchTable,
	}
}

// createOps lists every operation that creates a table or view under "ns",
// each adding lateEntry.
func createOps(t *testing.T) []createOp {
	t.Helper()
	location := "s3://" + renameTestBucket + "/ns/" + lateEntry + "/metadata/v1.metadata.json"
	return []createOp{
		createTableOp(t),
		{
			operation: "RegisterTable",
			input: &RegisterTableRequest{
				TableBucketARN:   mustBucketARN(t),
				Namespace:        []string{"ns"},
				Name:             lateEntry,
				MetadataLocation: location,
			},
			read:        "GetTable",
			readInput:   getLateTableRequest(t),
			readMissing: ErrCodeNoSuchTable,
		},
		{
			operation: "CreateView",
			input: &CreateViewRequest{
				TableBucketARN:   mustBucketARN(t),
				Namespace:        []string{"ns"},
				Name:             lateEntry,
				MetadataLocation: location,
			},
			read: "GetView",
			readInput: &GetViewRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns"},
				Name:           lateEntry,
			},
			readMissing: ErrCodeNoSuchView,
		},
	}
}

func deleteNamespaceRequest(t *testing.T) *DeleteNamespaceRequest {
	return &DeleteNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	}
}

// A create that finds the namespace already claimed by a delete is refused and
// writes nothing.
func TestCreatesRefusedInClaimedNamespace(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			plantDeleteClaim(fs, GetTableBucketPath(renameTestBucket), "ns")

			status, resp := runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusConflict, status, "%+v", resp)
			assert.Equal(t, ErrCodeConflict, resp.Type)
			assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), lateEntry), "nothing may be written under a namespace being deleted")
		})
	}
}

// A create that passed its checks writes after the delete scanned the namespace
// empty and before it removed the directory. The create sees the claim, backs
// out, and answers 409 instead of reporting an entry the delete then removes.
func TestCreateBetweenNamespaceScanAndDeleteBacksOut(t *testing.T) {
	type gap struct {
		create   handlerResult
		leftover *filer_pb.Entry
	}
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			client := newPausedCreateClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			// The create finishes inside the delete's check-to-delete window.
			inGap := make(chan gap, 1)
			fs.BeforeDelete = func() {
				close(client.release)
				create := <-created
				inGap <- gap{create, fs.Get(nsPath, lateEntry)}
			}
			status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
			require.Equal(t, http.StatusOK, status, "%+v", resp)

			got := <-inGap
			assert.Equal(t, http.StatusConflict, got.create.status, "an entry the delete removes must not be reported created: %+v", got.create.resp)
			assert.Equal(t, ErrCodeConflict, got.create.resp.Type)
			assert.Nil(t, got.leftover, "the create must remove what it wrote before the delete runs")
			assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"))
		})
	}
}

// A create that passed its checks writes after the delete finished. The filer
// makes the missing namespace directory for it, without the namespace record,
// so the create answers 404 and removes that directory too.
func TestCreateAfterNamespaceDeleteBacksOut(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			bucketPath := GetTableBucketPath(renameTestBucket)
			client := newPausedCreateClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
			require.Equal(t, http.StatusOK, status, "%+v", resp)
			// The filer creates missing parents for the entry directory, so the
			// namespace directory comes back, without its record.
			fs.Put(bucketPath, "ns", nil)
			close(client.release)

			create := <-created
			assert.Equal(t, http.StatusNotFound, create.status, "an entry under a deleted namespace must not be reported created: %+v", create.resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, create.resp.Type)
			assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), lateEntry))
			assert.Nil(t, fs.Get(bucketPath, "ns"), "the re-created namespace directory must go with the entry")
		})
	}
}

// A create that has written its record but not yet checked the namespace may
// still back the record out, so nothing may report it as existing. Otherwise a
// caller is told the entry exists, a delete then claims the namespace, and the
// create removes the entry that caller was told about.
func TestUnfinishedCreateIsNotReportedAsExisting(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			bucketPath := GetTableBucketPath(renameTestBucket)
			client := newPausedAfterWriteClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			status, resp := runHandler(t, m, fs.Client, op.read, op.readInput)
			assert.Equal(t, http.StatusNotFound, status, "an unfinished entry must not load: %+v", resp)
			assert.Equal(t, op.readMissing, resp.Type)
			status, resp = runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusConflict, status, "a second create must not be told the entry exists: %+v", resp)

			plantDeleteClaim(fs, bucketPath, "ns")
			close(client.release)

			create := <-created
			assert.Equal(t, http.StatusConflict, create.status, "%+v", create.resp)
			assert.Equal(t, ErrCodeConflict, create.resp.Type)
			status, resp = runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
			require.Equal(t, http.StatusOK, status, "%+v", resp)
			assert.Nil(t, fs.Get(bucketPath, "ns"))
		})
	}
}

// A caller who may not create is told so whether or not a delete holds the
// namespace; the claim must not show through a refusal.
func TestDeniedCreatesDoNotSeeADeleteClaim(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			m.SetTrusted(false)
			m.SetDefaultAllow(false)
			renameOnly := []string{"s3tables:RenameTable"}

			unclaimed := runRequestAs(t, m, fs, op.operation, op.input, renameOnly)
			require.Equal(t, http.StatusForbidden, unclaimed.status, "%+v", unclaimed.body)

			plantDeleteClaim(fs, GetTableBucketPath(renameTestBucket), "ns")
			claimed := runRequestAs(t, m, fs, op.operation, op.input, renameOnly)
			assert.Equal(t, unclaimed, claimed)
			assert.NotEqual(t, http.StatusConflict, claimed.status)
		})
	}
}

// A delete refused because the namespace still holds a table lifts its claim
// again, so the namespace takes creates as before.
func TestDeleteNamespaceWithTableLiftsItsClaim(t *testing.T) {
	for _, op := range []createOp{createTableOp(t)} {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			bucketPath := GetTableBucketPath(renameTestBucket)
			before := fs.Get(bucketPath, "ns").Extended

			status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
			assert.Equal(t, http.StatusConflict, status)
			assert.Equal(t, ErrCodeNamespaceNotEmpty, resp.Type)
			assert.Equal(t, before, fs.Get(bucketPath, "ns").Extended, "the refused delete must not leave its claim behind")

			status, resp = runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusOK, status, "%+v", resp)
			created := fs.Get(GetNamespacePath(renameTestBucket, "ns"), lateEntry)
			require.NotNil(t, created)
			assert.NotContains(t, created.Extended, ExtendedKeyDeleting, "a finished create must lift its unfinished mark")
		})
	}
}
