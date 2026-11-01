package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
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

// pausedConfirmClient holds the update that lifts a create's mark on target —
// the write that publishes the record — until release is closed, so a test
// can land a namespace claim after the create's parent check and before the
// record goes live.
type pausedConfirmClient struct {
	filer_pb.SeaweedFilerClient
	target  string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPausedConfirmClient(client filer_pb.SeaweedFilerClient, target string) *pausedConfirmClient {
	return &pausedConfirmClient{
		SeaweedFilerClient: client,
		target:             target,
		reached:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (c *pausedConfirmClient) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	if req.Entry != nil && path.Join(req.Directory, req.Entry.Name) == c.target && req.Entry.Extended[ExtendedKeyDeleting] == nil {
		c.once.Do(func() {
			close(c.reached)
			<-c.release
		})
	}
	return c.SeaweedFilerClient.UpdateEntry(ctx, req, opts...)
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

// plantCreateMark puts an unfinished create's mark on an entry, the way a
// create still confirming its parent leaves it.
func plantCreateMark(fs *s3tablestest.MemFiler, dir, name string) {
	extended := map[string][]byte{}
	for key, value := range fs.Get(dir, name).Extended {
		extended[key] = value
	}
	extended[ExtendedKeyDeleting] = []byte(createClaim)
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

// A create that passed its parent check publishes the record while the delete
// holds the namespace claim: the delete already scanned the marked record and
// skipped it, so the create must re-check the parent after lifting the mark —
// reporting success here leaves the delete to remove a record its caller was
// told exists.
func TestCreateConfirmingUnderANamespaceClaimBacksOut(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			client := newPausedConfirmClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			// The delete claims the namespace and scans the still-marked
			// record before the create's publishing write lands.
			inGap := make(chan handlerResult, 1)
			fs.BeforeDelete = func() {
				close(client.release)
				inGap <- <-created
			}
			status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
			require.Equal(t, http.StatusOK, status, "%+v", resp)

			create := <-inGap
			assert.Equal(t, http.StatusConflict, create.status, "a create confirmed under the claim must not report success: %+v", create.resp)
			assert.Equal(t, ErrCodeConflict, create.resp.Type)
			assert.Nil(t, fs.Get(nsPath, lateEntry))
			assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"))
		})
	}
}

// A namespace update inside the confirm window edits the same record — its
// createdAt survives — so the create still confirms: reading the edit as a
// delete+recreate would back a live entry out and report 404 for a namespace
// that never went away.
func TestCreateConfirmingAcrossANamespaceUpdateSucceeds(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			bucketPath := GetTableBucketPath(renameTestBucket)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			client := newPausedConfirmClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			// Rewrite the namespace metadata the way UpdateNamespace does —
			// same record, new properties — while the create's record waits.
			extended := map[string][]byte{}
			for key, value := range fs.Get(bucketPath, "ns").Extended {
				extended[key] = value
			}
			var nsMeta namespaceMetadata
			require.NoError(t, json.Unmarshal(extended[ExtendedKeyMetadata], &nsMeta))
			nsMeta.Properties = map[string]string{"edited": "true"}
			data, err := json.Marshal(&nsMeta)
			require.NoError(t, err)
			extended[ExtendedKeyMetadata] = data
			fs.Put(bucketPath, "ns", extended)

			close(client.release)
			create := <-created
			assert.Equal(t, http.StatusOK, create.status, "a namespace update must not read as a delete: %+v", create.resp)
			assert.NotNil(t, fs.Get(nsPath, lateEntry), "the create's record must survive an unchanged namespace")
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
			// A second create is not told the entry exists; it takes the marked
			// record over, since nothing a reader can see lives there.
			status, resp = runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusOK, status, "%+v", resp)

			plantDeleteClaim(fs, bucketPath, "ns")
			close(client.release)

			create := <-created
			assert.Equal(t, http.StatusConflict, create.status, "%+v", create.resp)
			assert.Equal(t, ErrCodeConflict, create.resp.Type)
			status, resp = runHandler(t, m, fs.Client, op.read, op.readInput)
			assert.Equal(t, http.StatusOK, status, "the adopted record survives the loser's back-out: %+v", resp)
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

// A record still marked by its create is absent to readers, so listings skip
// it the way the point reads do.
func TestListingsSkipAnUnfinishedCreate(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			client := newPausedAfterWriteClient(fs.Client, GetTablePath(renameTestBucket, "ns", lateEntry))
			created := startHandler(t, m, client, op.operation, op.input)
			waitForPause(t, client.reached, created)

			var names []string
			if op.operation == "CreateView" {
				list := &ListViewsResponse{}
				require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListViews", &ListViewsRequest{
					TableBucketARN: mustBucketARN(t),
					Namespace:      []string{"ns"},
				}, list, ""))
				for _, v := range list.Views {
					names = append(names, v.Name)
				}
			} else {
				list := &ListTablesResponse{}
				require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTables", &ListTablesRequest{
					TableBucketARN: mustBucketARN(t),
					Namespace:      []string{"ns"},
				}, list, ""))
				for _, tb := range list.Tables {
					names = append(names, tb.Name)
				}
			}
			assert.NotContains(t, names, lateEntry, "an unfinished create must not be listed")

			close(client.release)
			require.Equal(t, http.StatusOK, (<-created).status)
		})
	}
}

// A write that lands on a record a create marked refuses instead of updating
// an entry every reader reports absent and breaking the create's back-out.
func TestUpdateRefusedOnAMarkedRecord(t *testing.T) {
	fs, m := startRenameManager(t)
	fs.BeforeUpdate = func() {
		plantCreateMark(fs, GetNamespacePath(renameTestBucket, "ns"), "t")
	}
	status, resp := runHandler(t, m, fs.Client, "UpdateTable", &UpdateTableRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "t",
		MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json",
	})
	assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchTable, resp.Type)
}

// A rename into a namespace a delete claimed is refused before it touches the
// source: the destination cannot be confirmed, so the name must stay where it
// was.
func TestRenameIntoClaimedNamespaceIsRefused(t *testing.T) {
	fs, m := startRenameManager(t)
	ns2Meta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns2"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "ns2", map[string][]byte{ExtendedKeyMetadata: ns2Meta})
	plantDeleteClaim(fs, GetTableBucketPath(renameTestBucket), "ns2")

	status, resp := runHandler(t, m, fs.Client, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t2",
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)

	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	assert.Contains(t, src.Extended, ExtendedKeyMetadata, "the refused rename must leave the source record")
	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns2"), "t2"))
}

// The rename's destination goes up with the same unfinished mark a create
// leaves and ends without it once the namespace is confirmed.
func TestRenameClearsTheDestinationMark(t *testing.T) {
	fs, m := startRenameManager(t)
	require.NoError(t, runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	}))
	dest := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t2")
	require.NotNil(t, dest)
	assert.NotContains(t, dest.Extended, ExtendedKeyDeleting, "a finished rename must lift its unfinished mark")
}

// A rename destination is marked like a create but still owed a publish by the
// rename that wrote it: a create that adopted it the way it adopts an orphaned
// mark would strand the renamed record once its source is cleared.
func TestCreateCannotAdoptARenameDestination(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			dropSeedTable(t, m, fs)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			marked, _ := json.Marshal(tableMetadataInternal{
				Name:             lateEntry,
				Namespace:        "ns",
				Format:           "ICEBERG",
				OwnerAccountID:   DefaultAccountID,
				MetadataLocation: "s3://" + renameTestBucket + "/ns/" + lateEntry + "/metadata/v1.metadata.json",
			})
			fs.Put(nsPath, lateEntry, map[string][]byte{
				ExtendedKeyMetadata:  marked,
				ExtendedKeyEntryType: []byte(EntryTypeTable),
				ExtendedKeyDeleting:  []byte(renameDestClaim),
			})

			status, resp := runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusConflict, status, "a rename destination must refuse a create: %+v", resp)
			entry := fs.Get(nsPath, lateEntry)
			require.NotNil(t, entry)
			assert.Equal(t, []byte(renameDestClaim), entry.Extended[ExtendedKeyDeleting], "the rename's record must be untouched")
		})
	}
}

// A rename that commits while a delete claims the destination namespace has
// the same shape as a confirming create: the delete scanned the still-marked
// destination and skipped it, so the commit must re-check the namespace after
// the record goes live and restore the source instead of losing it.
func TestRenameCommittingUnderAClaimedNamespaceRestoresSource(t *testing.T) {
	fs, m := startRenameManager(t)
	ns2Meta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns2"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "ns2", map[string][]byte{ExtendedKeyMetadata: ns2Meta})

	client := newPausedConfirmClient(fs.Client, GetTablePath(renameTestBucket, "ns2", "t2"))
	renamed := startHandler(t, m, client, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t2",
	})
	waitForPause(t, client.reached, renamed)

	// The delete claims ns2 and scans the still-marked destination before the
	// rename's publishing write lands.
	inGap := make(chan handlerResult, 1)
	fs.BeforeDelete = func() {
		close(client.release)
		inGap <- <-renamed
	}
	status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", &DeleteNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	rename := <-inGap
	assert.Equal(t, http.StatusConflict, rename.status, "a rename committed under the claim must not report success: %+v", rename.resp)
	src := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, src)
	assert.Contains(t, src.Extended, ExtendedKeyMetadata, "the failed rename must restore its source record")
	assert.NotContains(t, src.Extended, ExtendedKeyDeleting)
	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns2"))
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

// A claim the delete fails to lift refuses every create in the namespace, so a
// release the filer turns away once is retried.
func TestDeleteNamespaceRetriesTheClaimRelease(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	before := fs.Get(bucketPath, "ns").Extended
	client := &failClaimReleaseClient{SeaweedFilerClient: fs.Client, target: GetNamespacePath(renameTestBucket, "ns")}

	code, resp := runHandler(t, m, client, "DeleteNamespace", deleteNamespaceRequest(t))

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, ErrCodeNamespaceNotEmpty, resp.Type, "%+v", resp)
	require.Equal(t, 1, client.failed, "the first release has to fail")
	assert.Equal(t, before, fs.Get(bucketPath, "ns").Extended, "a later attempt lifts the claim")
}

// orphanMarkedRecord plants what a create that crashed after writing its
// record leaves behind: catalog attributes under the unfinished mark.
func orphanMarkedRecord(fs *s3tablestest.MemFiler, dir, name string) {
	meta, _ := json.Marshal(tableMetadataInternal{
		Name:             name,
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  1,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/" + name + "/metadata/v1.metadata.json",
	})
	fs.Put(dir, name, map[string][]byte{
		ExtendedKeyMetadata:  meta,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
		ExtendedKeyPolicy:    []byte("stale"),
		ExtendedKeyDeleting:  []byte(createClaim),
	})
}

// A record whose create never lifted its mark — the request died after the
// write — must not wedge the name: the marked record is absent to every
// reader, so a create takes it over instead of refusing the name forever.
func TestCreateAdoptsAnOrphanedMark(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			orphanMarkedRecord(fs, nsPath, lateEntry)

			status, resp := runHandler(t, m, fs.Client, op.operation, op.input)
			require.Equal(t, http.StatusOK, status, "%+v", resp)

			entry := fs.Get(nsPath, lateEntry)
			require.NotNil(t, entry)
			assert.NotContains(t, entry.Extended, ExtendedKeyDeleting, "the adopted record must end confirmed")
			assert.NotContains(t, entry.Extended, ExtendedKeyPolicy, "the stale record's keys must not survive")

			status, resp = runHandler(t, m, fs.Client, op.read, op.readInput)
			assert.Equal(t, http.StatusOK, status, "%+v", resp)
		})
	}
}

// A marked record is no destination a rename must refuse: the rename takes
// the record over like a create does.
func TestRenameAdoptsAMarkedDestination(t *testing.T) {
	fs, m := startRenameManager(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	orphanMarkedRecord(fs, nsPath, "t2")

	require.NoError(t, runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	}))

	dest := fs.Get(nsPath, "t2")
	require.NotNil(t, dest)
	assert.NotContains(t, dest.Extended, ExtendedKeyDeleting)
	assert.NotContains(t, dest.Extended, ExtendedKeyPolicy, "the stale record's keys must not survive")
	var metadata tableMetadataInternal
	require.NoError(t, json.Unmarshal(dest.Extended[ExtendedKeyMetadata], &metadata))
	assert.Equal(t, "t2", metadata.Name)
}

// A marked record is no child either: its create cannot confirm under the
// claim the delete holds, and an abandoned one would refuse the delete
// forever. The delete removes it with the namespace.
func TestDeleteNamespaceSkipsAnUnfinishedCreate(t *testing.T) {
	fs, m := startRenameManager(t)
	dropSeedTable(t, m, fs)
	orphanMarkedRecord(fs, GetNamespacePath(renameTestBucket, "ns"), "late")

	status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
	require.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"))
}

// A namespace whose first page of entries is all skippable — bare directories
// a dropped table left behind — can still hold a table on a later page: the
// scan must keep listing instead of deleting the namespace out from under it.
func TestDeleteNamespaceScansPastASkippedPage(t *testing.T) {
	fs, m := startRenameManager(t)
	dropSeedTable(t, m, fs)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	for i := 0; i < 12; i++ {
		fs.Put(nsPath, fmt.Sprintf("a%02d", i), nil)
	}
	tableMeta, _ := json.Marshal(tableMetadataInternal{
		Name:           "zz",
		Namespace:      "ns",
		Format:         "ICEBERG",
		OwnerAccountID: DefaultAccountID,
	})
	fs.Put(nsPath, "zz", map[string][]byte{ExtendedKeyMetadata: tableMeta})

	status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeNamespaceNotEmpty, resp.Type)
	assert.NotNil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"))
}

// A first page of skippables and nothing after it is still an empty
// namespace: the scan must page to the end of the listing, not stop early.
func TestDeleteNamespacePagesToTheEnd(t *testing.T) {
	fs, m := startRenameManager(t)
	dropSeedTable(t, m, fs)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	for i := 0; i < 12; i++ {
		fs.Put(nsPath, fmt.Sprintf("a%02d", i), nil)
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteNamespace", deleteNamespaceRequest(t))
	require.Equal(t, http.StatusOK, status, "%+v", resp)
	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"))
}
