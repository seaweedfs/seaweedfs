package s3tables

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const vectorTestBucket = "vecns"

func vectorTestARN(t *testing.T) string {
	t.Helper()
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, vectorTestBucket)
	require.NoError(t, err)
	return arn
}

// seedVectorBucket lays down, through a trusted Manager, a bucket of the given
// format holding vector/idx (with a policy and a tag) and other/t, the shape
// the S3 Vectors gateway leaves behind.
func seedVectorBucket(t *testing.T, fs *s3tablestest.MemFiler, format string) *Manager {
	t.Helper()
	trusted := NewManager()
	trusted.SetTrusted(true)
	run := func(operation string, req interface{}) {
		t.Helper()
		require.NoError(t, trusted.Execute(context.Background(), NewManagerClient(fs.Client), operation, req, nil, ""))
	}
	arn := vectorTestARN(t)
	run("CreateTableBucket", &CreateTableBucketRequest{Name: vectorTestBucket, Format: format})
	for _, ns := range []string{VectorIndexNamespace, "other"} {
		run("CreateNamespace", &CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{ns}})
	}
	run("CreateTable", &CreateTableRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}, Name: "idx", Format: format})
	run("CreateTable", &CreateTableRequest{TableBucketARN: arn, Namespace: []string{"other"}, Name: "t", Format: format})
	run("PutTablePolicy", &PutTablePolicyRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}, Name: "idx",
		ResourcePolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}]}`})
	idx, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	run("TagResource", &TagResourceRequest{ResourceARN: idx.TableARN, Tags: map[string]string{"k": "v"}})
	return trusted
}

func getTestTable(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace, name string) (*GetTableResponse, error) {
	t.Helper()
	var resp GetTableResponse
	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "GetTable", &GetTableRequest{
		TableBucketARN: vectorTestARN(t), Namespace: []string{namespace}, Name: name}, &resp, "")
	return &resp, err
}

// vectorWrite labels a case "Operation detail" when an operation appears twice.
type vectorWrite struct {
	operation string
	request   interface{}
}

func (w vectorWrite) op() string {
	op, _, _ := strings.Cut(w.operation, " ")
	return op
}

// vectorNamespaceWrites lists the catalog writes that reach vector/idx or move
// a table across the namespace boundary.
func vectorNamespaceWrites(t *testing.T, indexARN, versionToken string) []vectorWrite {
	arn := vectorTestARN(t)
	vector := []string{VectorIndexNamespace}
	return []vectorWrite{
		{"UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: arn, Namespace: vector, Properties: map[string]string{"a": "b"}}},
		{"CreateTable", &CreateTableRequest{TableBucketARN: arn, Namespace: vector, Name: "fresh", Format: FormatLance}},
		{"RegisterTable", &RegisterTableRequest{TableBucketARN: arn, Namespace: vector, Name: "registered",
			MetadataLocation: "s3://" + vectorTestBucket + "/vector/registered/metadata/v1.metadata.json"}},
		{"UpdateTable", &UpdateTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx", VersionToken: versionToken,
			MetadataLocation: "s3://" + vectorTestBucket + "/vector/elsewhere"}},
		{"DeleteTable", &DeleteTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}},
		{"RenameTable out", &RenameTableRequest{TableBucketARN: arn, SourceNamespace: vector, SourceName: "idx",
			DestNamespace: []string{"other"}, DestName: "moved"}},
		{"RenameTable in", &RenameTableRequest{TableBucketARN: arn, SourceNamespace: []string{"other"}, SourceName: "t",
			DestNamespace: vector, DestName: "t"}},
		{"PutTablePolicy", &PutTablePolicyRequest{TableBucketARN: arn, Namespace: vector, Name: "idx",
			ResourcePolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*"}]}`}},
		{"DeleteTablePolicy", &DeleteTablePolicyRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}},
		{"TagResource", &TagResourceRequest{ResourceARN: indexARN, Tags: map[string]string{"x": "y"}}},
		{"UntagResource", &UntagResourceRequest{ResourceARN: indexARN, TagKeys: []string{"k"}}},
		{"PutTableMaintenanceConfiguration", &PutTableMaintenanceConfigurationRequest{TableBucketARN: arn, Namespace: vector, Name: "idx",
			Type: MaintenanceTypeIcebergCompaction, Value: &MaintenanceConfigurationValue{Status: "enabled"}}},
	}
}

func untrustedManager() *Manager {
	m := NewManager()
	m.SetDefaultAllow(false)
	return m
}

// A caller holding s3tables:* still cannot change the index namespace of a
// vector bucket, and nothing it tried is left behind.
func TestVectorNamespaceRefusesUntrustedWrites(t *testing.T) {
	fs := s3tablestest.Start(t)
	trusted := seedVectorBucket(t, fs, FormatLance)
	before, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	m := untrustedManager()
	granted := []string{"s3tables:*"}

	for _, tc := range vectorNamespaceWrites(t, before.TableARN, before.VersionToken) {
		t.Run(tc.operation, func(t *testing.T) {
			got := runRequestAs(t, m, fs, tc.op(), tc.request, granted)
			assert.Equal(t, 403, got.status)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type)
			assert.Equal(t, vectorNamespaceReadOnly, got.body.Message)
		})
	}

	after, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	assert.Equal(t, before.VersionToken, after.VersionToken)
	assert.Equal(t, before.MetadataLocation, after.MetadataLocation)
	_, err = getTestTable(t, trusted, fs, "other", "t")
	assert.NoError(t, err)
	for _, absent := range [][2]string{{VectorIndexNamespace, "fresh"}, {VectorIndexNamespace, "registered"}, {VectorIndexNamespace, "t"}, {"other", "moved"}} {
		_, err := getTestTable(t, trusted, fs, absent[0], absent[1])
		assert.Error(t, err, "%s/%s should not exist", absent[0], absent[1])
	}
	var tags ListTagsForResourceResponse
	require.NoError(t, trusted.Execute(context.Background(), NewManagerClient(fs.Client), "ListTagsForResource",
		&ListTagsForResourceRequest{ResourceARN: before.TableARN}, &tags, ""))
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)
}

func TestVectorNamespaceCannotBeCreatedOrDeleted(t *testing.T) {
	fs := s3tablestest.Start(t)
	trusted := NewManager()
	trusted.SetTrusted(true)
	m := untrustedManager()
	granted := []string{"s3tables:*"}
	arn := vectorTestARN(t)
	ctx := context.Background()
	client := NewManagerClient(fs.Client)
	require.NoError(t, trusted.Execute(ctx, client, "CreateTableBucket", &CreateTableBucketRequest{Name: vectorTestBucket, Format: FormatLance}, nil, ""))

	create := &CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}}
	got := runRequestAs(t, m, fs, "CreateNamespace", create, granted)
	assert.Equal(t, 403, got.status)
	assert.Equal(t, vectorNamespaceReadOnly, got.body.Message)

	require.NoError(t, trusted.Execute(ctx, client, "CreateNamespace", create, nil, ""))
	got = runRequestAs(t, m, fs, "DeleteNamespace", &DeleteNamespaceRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}}, granted)
	assert.Equal(t, 403, got.status)
	assert.Equal(t, vectorNamespaceReadOnly, got.body.Message)
	require.NoError(t, trusted.Execute(ctx, client, "GetNamespace", &GetNamespaceRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}}, nil, ""))

	other := &CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{"other"}}
	assert.Equal(t, 200, runRequestAs(t, m, fs, "CreateNamespace", other, granted).status)
}

// The vector engine declares an index the gateway already created; that
// CreateTable writes nothing and keeps succeeding, as do reads.
func TestVectorNamespaceAllowsReadsAndExistingCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	trusted := seedVectorBucket(t, fs, FormatLance)
	before, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	m := untrustedManager()
	granted := []string{"s3tables:*"}
	arn := vectorTestARN(t)
	vector := []string{VectorIndexNamespace}

	for _, tc := range []vectorWrite{
		{"CreateTable", &CreateTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx", Format: FormatLance}},
		{"GetTable", &GetTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}},
		{"ListTables", &ListTablesRequest{TableBucketARN: arn, Namespace: vector}},
		{"GetNamespace", &GetNamespaceRequest{TableBucketARN: arn, Namespace: vector}},
		{"GetTablePolicy", &GetTablePolicyRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}},
		{"ListTagsForResource", &ListTagsForResourceRequest{ResourceARN: before.TableARN}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			assert.Equal(t, 200, runRequestAs(t, m, fs, tc.op(), tc.request, granted).status)
		})
	}
	after, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	assert.Equal(t, before.VersionToken, after.VersionToken)

	require.NoError(t, trusted.Execute(context.Background(), NewManagerClient(fs.Client), "DeleteTable",
		&DeleteTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}, nil, ""), "the trusted path stays writable")
}

// An Iceberg bucket's namespace named vector is an ordinary namespace.
func TestVectorNamespaceOfIcebergBucketIsWritable(t *testing.T) {
	fs := s3tablestest.Start(t)
	trusted := seedVectorBucket(t, fs, FormatIceberg)
	before, err := getTestTable(t, trusted, fs, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	m := untrustedManager()
	granted := []string{"s3tables:*"}

	for _, tc := range vectorNamespaceWrites(t, before.TableARN, before.VersionToken) {
		switch tc.operation {
		case "CreateTable", "UpdateTable", "DeleteTable", "RenameTable out":
			continue // covered below: they need an Iceberg request or would consume idx
		}
		t.Run(tc.operation, func(t *testing.T) {
			got := runRequestAs(t, m, fs, tc.op(), tc.request, granted)
			assert.Equal(t, 200, got.status, "%+v", got.body)
		})
	}
	arn := vectorTestARN(t)
	vector := []string{VectorIndexNamespace}
	assert.Equal(t, 200, runRequestAs(t, m, fs, "CreateTable",
		&CreateTableRequest{TableBucketARN: arn, Namespace: vector, Name: "fresh", Format: FormatIceberg}, granted).status)
	assert.Equal(t, 200, runRequestAs(t, m, fs, "DeleteTable",
		&DeleteTableRequest{TableBucketARN: arn, Namespace: vector, Name: "idx"}, granted).status)
}

// A caller without a grant gets the answer an Iceberg bucket of the same name
// and shape gives: the read-only rule never tells it the bucket exists or
// holds vectors.
func TestVectorNamespaceDenialHidesBucketFormat(t *testing.T) {
	lanceFS := s3tablestest.Start(t)
	lance := seedVectorBucket(t, lanceFS, FormatLance)
	icebergFS := s3tablestest.Start(t)
	seedVectorBucket(t, icebergFS, FormatIceberg)
	idx, err := getTestTable(t, lance, lanceFS, VectorIndexNamespace, "idx")
	require.NoError(t, err)
	m := untrustedManager()

	arn := vectorTestARN(t)
	writes := append(vectorNamespaceWrites(t, idx.TableARN, idx.VersionToken),
		vectorWrite{"CreateNamespace", &CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}}},
		vectorWrite{"DeleteNamespace", &DeleteNamespaceRequest{TableBucketARN: arn, Namespace: []string{VectorIndexNamespace}}})
	for _, tc := range writes {
		t.Run(tc.operation, func(t *testing.T) {
			got := runUnauthorizedRequest(t, m, lanceFS, tc.op(), tc.request)
			want := runUnauthorizedRequest(t, m, icebergFS, tc.op(), tc.request)
			assert.Equal(t, want, got)
			assert.NotEqual(t, vectorNamespaceReadOnly, got.body.Message)
		})
	}
}

// A deferred create writes into the bucket before it registers anything, so
// its gate refuses the vector namespace as the create itself would.
func TestAuthorizeCreateTableRefusesVectorNamespace(t *testing.T) {
	identity := &testIdentity{Name: "reader", Account: &testIdentityAccount{Id: "reader"}, Actions: []string{"s3tables:*"}}
	ctx := s3_constants.SetIdentityInContext(context.Background(), identity)
	req := &CreateTableRequest{TableBucketARN: vectorTestARN(t), Namespace: []string{VectorIndexNamespace}, Name: "fresh"}

	lanceFS := s3tablestest.Start(t)
	seedVectorBucket(t, lanceFS, FormatLance)
	err := untrustedManager().AuthorizeCreateTable(ctx, NewManagerClient(lanceFS.Client), req, "reader")
	var apiErr *S3TablesError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, ErrCodeAccessDenied, apiErr.Type)
	assert.Equal(t, vectorNamespaceReadOnly, apiErr.Message)

	icebergFS := s3tablestest.Start(t)
	seedVectorBucket(t, icebergFS, FormatIceberg)
	assert.NoError(t, untrustedManager().AuthorizeCreateTable(ctx, NewManagerClient(icebergFS.Client), req, "reader"))
}

// Only a CreateTable that finds a finished index passes unfenced, so the
// declare keeps working even where the idempotent path writes.
func TestVectorNamespaceFenceSparesOnlyExistingIndexCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedVectorBucket(t, fs, FormatLance)
	fs.Put(GetNamespacePath(vectorTestBucket, VectorIndexNamespace), "pending", map[string][]byte{
		ExtendedKeyMetadata: []byte(`{"name":"pending","format":"LANCE"}`),
		ExtendedKeyDeleting: []byte(createClaim),
	})
	h := untrustedManager().handler

	for name, fenced := range map[string]bool{"idx": false, "fresh": true, "pending": true} {
		t.Run(name, func(t *testing.T) {
			body := `{"tableBucketARN":"` + vectorTestARN(t) + `","namespace":["vector"],"name":"` + name + `","format":"LANCE"}`
			req, err := newManagerRequest(context.Background(), "CreateTable", []byte(body), "reader")
			require.NoError(t, err)
			got := h.fenceVectorNamespace(nil, req, NewManagerClient(fs.Client), "CreateTable")
			assert.Equal(t, fenced, got != nil)
		})
	}
}
