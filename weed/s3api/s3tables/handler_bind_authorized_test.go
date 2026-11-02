package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// recreatedNamespace is the record of a namespace deleted and created again
// under name: another owner, and a creation time of its own.
func recreatedNamespace(t *testing.T, name string) map[string][]byte {
	t.Helper()
	meta, err := json.Marshal(namespaceMetadata{
		Namespace:      []string{name},
		OwnerAccountID: "111122223333",
		CreatedAt:      time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	return map[string][]byte{ExtendedKeyMetadata: meta}
}

// replaceNamespaceAtLookup replaces namespace with a recreated one at the first
// lookup of namespace/child, the create's look for its own name, which comes
// after the create was authorized against the first namespace.
func replaceNamespaceAtLookup(t *testing.T, fs *s3tablestest.MemFiler, namespace, child string) map[string][]byte {
	t.Helper()
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreated := recreatedNamespace(t, namespace)
	fs.OnLookup = func(dir, name string) {
		if dir != GetNamespacePath(renameTestBucket, namespace) || name != child {
			return
		}
		fs.OnLookup = nil
		fs.Put(bucketPath, namespace, recreated)
	}
	return recreated
}

// A create is authorized against the namespace it read. If that namespace is
// deleted and another is created under the same name before the create writes,
// the entry would land in a namespace nobody authorized it for, carrying the
// first one's owner. The create checks the namespace by its creation time,
// reads the new one as the old one gone, and backs out what it wrote.
func TestCreateInNamespaceTreatsARecreatedNamespaceAsGone(t *testing.T) {
	for _, tc := range []struct {
		operation string
		input     func(t *testing.T) interface{}
	}{
		{"CreateTable", func(t *testing.T) interface{} { return createTableRequest(t, "ns", "t") }},
		{"RegisterTable", func(t *testing.T) interface{} {
			return &RegisterTableRequest{
				TableBucketARN:   mustBucketARN(t),
				Namespace:        []string{"ns"},
				Name:             "t",
				MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v1.metadata.json",
			}
		}},
		{"CreateView", func(t *testing.T) interface{} { return createViewRequest(t, "t") }},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			bucketPath := GetTableBucketPath(renameTestBucket)
			requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
			recreated := replaceNamespaceAtLookup(t, fs, "ns", "t")

			code, resp := runHandler(t, m, fs.Client, tc.operation, tc.input(t))

			assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
			assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "nothing is left in the new namespace")
			require.NotNil(t, fs.Get(bucketPath, "ns"), "the new namespace stays")
			assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "and is left as it was created")
		})
	}
}

// A rename is authorized to create in the destination namespace it read. If
// that namespace is deleted and created again before the rename publishes, the
// renamed table would land in a namespace nobody authorized it for. The rename
// reads the new one as the old one gone, backs out, and keeps the source.
func TestRenameTableTreatsARecreatedDestinationAsGone(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
	})
	source := fs.Get(nsPath, "t").Extended
	// The rename looks the destination name up right after it read the
	// destination namespace.
	recreated := replaceNamespaceAtLookup(t, fs, "ns2", "t")

	code, resp := runHandler(t, m, fs.Client, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t",
	})

	assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
	assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns2"), "t"), "nothing is left in the new namespace")
	require.NotNil(t, fs.Get(bucketPath, "ns2"), "the new namespace stays")
	assert.Equal(t, recreated, fs.Get(bucketPath, "ns2").Extended, "and is left as it was created")
	require.NotNil(t, fs.Get(nsPath, "t"), "the source stays")
	assert.Equal(t, source, fs.Get(nsPath, "t").Extended, "with its record and no claim")
}

// recreatedRecord is the record of a table or view deleted and created again
// under ns/name: another owner, and a creation time of its own.
func recreatedRecord(t *testing.T, name, entryType string) map[string][]byte {
	t.Helper()
	now := time.Now().Add(time.Hour)
	meta, err := json.Marshal(tableMetadataInternal{
		Name:             name,
		Namespace:        "ns",
		Format:           FormatIceberg,
		CreatedAt:        now,
		ModifiedAt:       now,
		OwnerAccountID:   "111122223333",
		VersionToken:     generateVersionToken(),
		MetadataVersion:  1,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/" + name + "/metadata/v1-recreated.metadata.json",
	})
	require.NoError(t, err)
	return map[string][]byte{
		ExtendedKeyMetadata:  meta,
		ExtendedKeyEntryType: []byte(entryType),
	}
}

// startViewV seeds the namespace ns holding the view v.
func startViewV(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	requireOK(t, m, fs, "CreateView", createViewRequest(t, "v"))
	return fs, m
}

func updateViewRequest(t *testing.T, token, version string) *UpdateViewRequest {
	return &UpdateViewRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "v",
		VersionToken:     token,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/v/metadata/" + version + ".metadata.json",
	}
}

// An update is authorized against the view it read. If that view is deleted
// and another is created under the same name before the update writes, the
// write would land on a view nobody authorized it for. The write is conditional
// on the record the update read, so it is refused and the new view kept.
func TestUpdateViewSparesARecreatedView(t *testing.T) {
	fs, m := startViewV(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	recreated := recreatedRecord(t, "v", EntryTypeView)
	// The update's write is its first; the view is replaced as it lands, so the
	// write loses and its retry reads the new view.
	fs.BeforeUpdate = func() { fs.Put(nsPath, "v", recreated) }

	code, resp := runHandler(t, m, fs.Client, "UpdateView", updateViewRequest(t, "", "v2"))

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	require.NotNil(t, fs.Get(nsPath, "v"), "the new view stays")
	assert.Equal(t, recreated, fs.Get(nsPath, "v").Extended, "and is left as it was created")
}

// Two updates that read the same version both pass the version-token check.
// The second to write must not overwrite the first one's metadata pointer, or
// the version the first published is lost.
func TestUpdateViewRejectsLostUpdate(t *testing.T) {
	fs, m := startViewV(t)
	token := versionTokenOf(t, fs, "v")
	var winnerStatus int
	var winnerResp S3TablesError
	// The winner runs whole between the loser's read and its write.
	fs.BeforeUpdate = func() {
		winnerStatus, winnerResp = runHandler(t, m, fs.Client, "UpdateView", updateViewRequest(t, token, "v2-winner"))
	}

	code, resp := runHandler(t, m, fs.Client, "UpdateView", updateViewRequest(t, token, "v2-loser"))

	require.Equal(t, http.StatusOK, winnerStatus, "%+v", winnerResp)
	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	got := &GetViewResponse{}
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "GetView", getViewRequest(t, "v"), got, ""))
	assert.Equal(t, updateViewRequest(t, token, "v2-winner").MetadataLocation, got.MetadataLocation, "the first update must survive")
}

// A policy written between the authorization check and the write decides
// whether this caller may still update the view, so the update is refused
// rather than applied on a decision that has been overtaken.
func TestUpdateViewRejectsWhenTheAuthorizingPolicyChanges(t *testing.T) {
	fs, m := startViewV(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	fs.BeforeUpdate = func() {
		extended := map[string][]byte{}
		for key, value := range fs.Get(nsPath, "v").Extended {
			extended[key] = value
		}
		extended[ExtendedKeyPolicy] = policy
		fs.Put(nsPath, "v", extended)
	}

	code, resp := runHandler(t, m, fs.Client, "UpdateView", updateViewRequest(t, "", "v2"))

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
	assert.Equal(t, policy, fs.Get(nsPath, "v").Extended[ExtendedKeyPolicy], "the concurrent policy write must survive")
}

const bindTestPolicy = `{"Statement":[{"Effect":"Allow","Principal":"writer","Action":"s3tables:*","Resource":"*"}]}`

// tableWriteOp is a write to the table ns/t, with the attributes the table
// carries for it besides its record.
type tableWriteOp struct {
	operation string
	input     func(t *testing.T) interface{}
	extra     map[string][]byte
}

func tableWriteOps() []tableWriteOp {
	tableARN := func(t *testing.T) string {
		arn, err := BuildTableARN(DefaultRegion, DefaultAccountID, renameTestBucket, "ns", "t")
		require.NoError(t, err)
		return arn
	}
	return []tableWriteOp{
		{operation: "PutTablePolicy", input: func(t *testing.T) interface{} {
			return &PutTablePolicyRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", ResourcePolicy: bindTestPolicy}
		}},
		{operation: "DeleteTablePolicy", input: func(t *testing.T) interface{} {
			return &DeleteTablePolicyRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t"}
		}, extra: map[string][]byte{ExtendedKeyPolicy: []byte(bindTestPolicy)}},
		{operation: "TagResource", input: func(t *testing.T) interface{} {
			return &TagResourceRequest{ResourceARN: tableARN(t), Tags: map[string]string{"k": "v"}}
		}},
		{operation: "UntagResource", input: func(t *testing.T) interface{} {
			return &UntagResourceRequest{ResourceARN: tableARN(t), TagKeys: []string{"k"}}
		}, extra: map[string][]byte{ExtendedKeyTags: []byte(`{"k":"v","keep":"1"}`)}},
		{operation: "PutTableMaintenanceConfiguration", input: func(t *testing.T) interface{} {
			return &PutTableMaintenanceConfigurationRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns"},
				Name:           "t",
				Type:           MaintenanceTypeIcebergCompaction,
				Value:          &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled},
			}
		}},
	}
}

// startTableT seeds the namespace ns holding the table t, carrying extra
// besides its record.
func startTableT(t *testing.T, extra map[string][]byte) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startRenameManager(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	extended := extendedOf(t, fs, nsPath, "t")
	for key, value := range extra {
		extended[key] = value
	}
	fs.Put(nsPath, "t", extended)
	return fs, m
}

// A write to a table's policy, tags or maintenance settings is authorized
// against the table it read. If that table is deleted and another is created
// under the same name before the write, it would land on a table nobody
// authorized it for: the policy or tag values it compares can match the new
// table's just as well. The write checks the table by its creation time and is
// refused, and the new table is kept as it was created.
func TestTableWritesSpareARecreatedTable(t *testing.T) {
	for _, op := range tableWriteOps() {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startTableT(t, op.extra)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			recreated := recreatedRecord(t, "t", EntryTypeTable)
			for key, value := range op.extra {
				recreated[key] = value
			}
			// The write is the operation's first; the table is replaced as it
			// lands, so the write loses and its retry reads the new table.
			fs.BeforeUpdate = func() { fs.Put(nsPath, "t", recreated) }

			code, resp := runHandler(t, m, fs.Client, op.operation, op.input(t))

			assert.Equal(t, http.StatusConflict, code, "%+v", resp)
			assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
			require.NotNil(t, fs.Get(nsPath, "t"), "the new table stays")
			assert.Equal(t, recreated, fs.Get(nsPath, "t").Extended, "and is left as it was created")
		})
	}
}

// The same writes compare only the table's creation time, not its metadata:
// an Iceberg commit to the table between the read and the write is the same
// table, and must not turn the write into a conflict.
func TestTableWritesTolerateACommitInBetween(t *testing.T) {
	for _, op := range tableWriteOps() {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startTableT(t, op.extra)
			nsPath := GetNamespacePath(renameTestBucket, "ns")
			committedLocation := "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json"
			fs.BeforeUpdate = func() {
				extended := extendedOf(t, fs, nsPath, "t")
				var metadata tableMetadataInternal
				require.NoError(t, json.Unmarshal(extended[ExtendedKeyMetadata], &metadata))
				metadata.MetadataVersion = 4
				metadata.MetadataLocation = committedLocation
				metadata.VersionToken = generateVersionToken()
				metadata.ModifiedAt = time.Now()
				data, err := json.Marshal(metadata)
				require.NoError(t, err)
				extended[ExtendedKeyMetadata] = data
				fs.Put(nsPath, "t", extended)
			}

			requireOK(t, m, fs, op.operation, op.input(t))

			got, err := runGetTable(t, m, fs, "ns", "t")
			require.NoError(t, err)
			assert.Equal(t, committedLocation, got.MetadataLocation, "the commit stays")
		})
	}
}

// namespaceMetadataOf reads the record of a namespace in the test bucket.
func namespaceMetadataOf(t *testing.T, fs *s3tablestest.MemFiler, namespace string) namespaceMetadata {
	t.Helper()
	entry := fs.Get(GetTableBucketPath(renameTestBucket), namespace)
	require.NotNil(t, entry)
	var metadata namespaceMetadata
	require.NoError(t, json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata))
	return metadata
}

// updateNamespaceAtLookup runs a real UpdateNamespace the first time child is
// looked up under namespace: after the request was authorized against the
// namespace and before it writes. The answer fills in once the update has run.
func updateNamespaceAtLookup(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, namespace, child string) *handlerResult {
	t.Helper()
	update := &handlerResult{}
	fs.OnLookup = func(dir, name string) {
		if dir != GetNamespacePath(renameTestBucket, namespace) || name != child {
			return
		}
		fs.OnLookup = nil
		update.status, update.resp = runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
			TableBucketARN: mustBucketARN(t),
			Namespace:      []string{namespace},
			Properties:     map[string]string{"edited": "true"},
		})
	}
	return update
}

// requireNamespaceUpdatedInPlace checks that the update ran, took effect, and
// kept the namespace the record it was: same owner, same creation time.
func requireNamespaceUpdatedInPlace(t *testing.T, fs *s3tablestest.MemFiler, namespace string, before namespaceMetadata, update *handlerResult) {
	t.Helper()
	require.NotZero(t, update.status, "the update has to land between the authorization and the write")
	require.Equal(t, http.StatusOK, update.status, "%+v", update.resp)
	after := namespaceMetadataOf(t, fs, namespace)
	assert.Equal(t, map[string]string{"edited": "true"}, after.Properties)
	assert.True(t, after.CreatedAt.Equal(before.CreatedAt), "an update must keep the creation time: %v became %v", before.CreatedAt, after.CreatedAt)
	assert.Equal(t, before.OwnerAccountID, after.OwnerAccountID)
}

// A namespace update edits the record a request was authorized against; it
// does not replace it. The creation time survives, so the request still writes:
// reading the edit as a delete and create would back the entry out and answer
// 404 for a namespace that never went away. Unlike
// TestCreateConfirmingAcrossANamespaceUpdateSucceeds, the update here is the
// real UpdateNamespace and lands before the first check against the authorized
// record, not after it.
func TestCreateAuthorizedBeforeANamespaceUpdateSucceeds(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
			before := namespaceMetadataOf(t, fs, "ns")
			update := updateNamespaceAtLookup(t, fs, m, "ns", lateEntry)

			code, resp := runHandler(t, m, fs.Client, op.operation, op.input)

			assert.Equal(t, http.StatusOK, code, "%+v", resp)
			requireNamespaceUpdatedInPlace(t, fs, "ns", before, update)
			entry := fs.Get(GetNamespacePath(renameTestBucket, "ns"), lateEntry)
			require.NotNil(t, entry, "the entry the request answered for must exist")
			assert.Contains(t, entry.Extended, ExtendedKeyMetadata)
			assert.NotContains(t, entry.Extended, ExtendedKeyDeleting, "and carry no unfinished mark")
		})
	}

	// A rename is bound to its destination namespace the same way.
	for _, tc := range []struct {
		operation string
		name      string
		seed      func(t *testing.T, fs *s3tablestest.MemFiler, name string)
	}{
		{"RenameTable", "t", nil},
		{"RenameView", "v", seedView},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			if tc.seed != nil {
				tc.seed(t, fs, tc.name)
			}
			requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns2"},
			})
			before := namespaceMetadataOf(t, fs, "ns2")
			update := updateNamespaceAtLookup(t, fs, m, "ns2", tc.name)

			code, resp := runHandler(t, m, fs.Client, tc.operation, &RenameTableRequest{
				TableBucketARN:  mustBucketARN(t),
				SourceNamespace: []string{"ns"},
				SourceName:      tc.name,
				DestNamespace:   []string{"ns2"},
				DestName:        tc.name,
			})

			assert.Equal(t, http.StatusOK, code, "%+v", resp)
			requireNamespaceUpdatedInPlace(t, fs, "ns2", before, update)
			dest := fs.Get(GetNamespacePath(renameTestBucket, "ns2"), tc.name)
			require.NotNil(t, dest, "the renamed entry must exist")
			assert.Contains(t, dest.Extended, ExtendedKeyMetadata)
			assert.NotContains(t, dest.Extended, ExtendedKeyDeleting, "and carry no unfinished mark")
		})
	}
}

// writeRecorder remembers the writes that reach a directory tree, so a test can
// tell a request that never wrote there from one that wrote and backed out.
type writeRecorder struct {
	filer_pb.SeaweedFilerClient
	tree   string
	mu     sync.Mutex
	writes []string
}

func (c *writeRecorder) note(op, dir, name string) {
	if dir != c.tree && !strings.HasPrefix(dir, c.tree+"/") {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, op+" "+dir+"/"+name)
}

func (c *writeRecorder) CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	c.note("create", in.Directory, in.Entry.GetName())
	return c.SeaweedFilerClient.CreateEntry(ctx, in, opts...)
}

func (c *writeRecorder) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	c.note("update", in.Directory, in.Entry.GetName())
	return c.SeaweedFilerClient.UpdateEntry(ctx, in, opts...)
}

func (c *writeRecorder) DeleteEntry(ctx context.Context, in *filer_pb.DeleteEntryRequest, opts ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	c.note("delete", in.Directory, in.Name)
	return c.SeaweedFilerClient.DeleteEntry(ctx, in, opts...)
}

// A namespace deleted and created again between the authorization and the
// early check is not the one the caller was authorized against, so the request
// is refused before it writes anything: the check after the write only backs
// the record out, and a back-out the filer turns away would leave it in a
// namespace nobody authorized it for.
func TestCreateRefusedBeforeWritingIntoAReplacedNamespace(t *testing.T) {
	for _, op := range createOps(t) {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
			bucketPath := GetTableBucketPath(renameTestBucket)
			recreated := recreatedNamespace(t, "ns")
			// The create reads the namespace record three times before it writes: for
			// its metadata, for its policy, and for the early check. The namespace is
			// replaced as the third read starts.
			const earlyCheck = 3
			reads := 0
			fs.OnLookup = func(dir, name string) {
				if dir != bucketPath || name != "ns" {
					return
				}
				if reads++; reads == earlyCheck {
					fs.OnLookup = nil
					fs.Put(bucketPath, "ns", recreated)
				}
			}
			recorder := &writeRecorder{SeaweedFilerClient: fs.Client, tree: GetNamespacePath(renameTestBucket, "ns")}

			code, resp := runHandler(t, m, recorder, op.operation, op.input)

			require.GreaterOrEqual(t, reads, earlyCheck, "the namespace has to be replaced before the early check")
			assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
			assert.Empty(t, recorder.writes, "nothing is written into the namespace that replaced the authorized one")
			assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "and it is left as it was created")
		})
	}

	// A rename reads its destination namespace and then the destination name,
	// both before the early check.
	for _, tc := range []struct {
		operation string
		name      string
		seed      func(t *testing.T, fs *s3tablestest.MemFiler, name string)
	}{
		{"RenameTable", "t", nil},
		{"RenameView", "v", seedView},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			fs, m := startRenameManager(t)
			if tc.seed != nil {
				tc.seed(t, fs, tc.name)
			}
			requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns2"},
			})
			recreated := replaceNamespaceAtLookup(t, fs, "ns2", tc.name)
			recorder := &writeRecorder{SeaweedFilerClient: fs.Client, tree: GetNamespacePath(renameTestBucket, "ns2")}

			code, resp := runHandler(t, m, recorder, tc.operation, &RenameTableRequest{
				TableBucketARN:  mustBucketARN(t),
				SourceNamespace: []string{"ns"},
				SourceName:      tc.name,
				DestNamespace:   []string{"ns2"},
				DestName:        tc.name,
			})

			assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type, "%+v", resp)
			assert.Empty(t, recorder.writes, "nothing is written into the namespace that replaced the authorized one")
			assert.Equal(t, recreated, fs.Get(GetTableBucketPath(renameTestBucket), "ns2").Extended, "and it is left as it was created")
		})
	}
}

// UpdateNamespace writes the record it was authorized against, owner and
// creation time included. A namespace deleted and created again before that
// write is refused, not handed the first one's owner.
func TestUpdateNamespaceSparesARecreatedNamespace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreated := recreatedNamespace(t, "ns")
	fs.OnUpdate = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnUpdate = nil
		fs.Put(bucketPath, "ns", recreated)
	}

	code, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Properties:     map[string]string{"edited": "true"},
	})

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "the new namespace is untouched")
}

// DeleteNamespace removes only the namespace it was authorized against: one
// deleted and created again before the claim reads as the first one gone.
func TestDeleteNamespaceSparesARecreatedNamespace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreated := recreatedNamespace(t, "ns")
	fs.OnUpdate = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnUpdate = nil
		fs.Put(bucketPath, "ns", recreated)
	}

	code, resp := runHandler(t, m, fs.Client, "DeleteNamespace", &DeleteNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})

	assert.Equal(t, http.StatusNotFound, code, "%+v", resp)
	require.NotNil(t, fs.Get(bucketPath, "ns"), "the new namespace survives")
	assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "the new namespace is untouched")
}
