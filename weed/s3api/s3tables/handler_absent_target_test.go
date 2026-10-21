package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runAbsentOp(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, operation string, input interface{}, name string, actions []string) s3TablesHTTPError {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	identity := &testIdentity{Name: name, Account: &testIdentityAccount{Id: name}, Actions: actions}
	req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), operation, body, name)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
	result := recorder.Result()
	defer result.Body.Close()

	var response S3TablesError
	if result.StatusCode >= 400 {
		require.NoError(t, json.NewDecoder(result.Body).Decode(&response))
	}
	return s3TablesHTTPError{status: result.StatusCode, body: response}
}

func absentBucketARN(t *testing.T, name string) string {
	t.Helper()
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, name)
	require.NoError(t, err)
	return arn
}

func absentTableARN(t *testing.T, bucket, namespace, table string) string {
	t.Helper()
	arn, err := BuildTableARN(DefaultRegion, DefaultAccountID, bucket, namespace, table)
	require.NoError(t, err)
	return arn
}

// An outsider gets the operation's own denial for an absent name, a name a
// plain S3 bucket holds, and a name it is denied on, so none of them can be
// told apart.
func TestAbsentTargetsAnswerLikeDenials(t *testing.T) {
	fs, m := startRenameManager(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)

	ghostARN := absentBucketARN(t, "ghostbkt")
	plainARN := absentBucketARN(t, "plainbkt")
	fs.Put(TablesPath, "plainbkt", nil) // a plain S3 bucket holds the name

	outsider := []string{"Read"}

	t.Run("ListTables", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			req  *ListTablesRequest
		}{
			{"denied namespace", &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}},
			{"missing namespace", &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
			{"namespace in missing bucket", &ListTablesRequest{TableBucketARN: ghostARN, Namespace: []string{"ns"}}},
			{"namespace in plain bucket", &ListTablesRequest{TableBucketARN: plainARN, Namespace: []string{"ns"}}},
			{"denied bucket", &ListTablesRequest{TableBucketARN: mustBucketARN(t)}},
			{"missing bucket", &ListTablesRequest{TableBucketARN: ghostARN}},
			{"plain bucket", &ListTablesRequest{TableBucketARN: plainARN}},
		} {
			got := runAbsentOp(t, m, fs, "ListTables", tc.req, "attacker", outsider)
			assert.Equal(t, 403, got.status, "%s: %s", tc.name, got.body.Message)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type, tc.name)
		}
	})

	t.Run("ListViews", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			req  *ListViewsRequest
		}{
			{"denied namespace", &ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}},
			{"missing namespace", &ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
			{"missing bucket", &ListViewsRequest{TableBucketARN: ghostARN, Namespace: []string{"ns"}}},
			{"plain bucket", &ListViewsRequest{TableBucketARN: plainARN, Namespace: []string{"ns"}}},
		} {
			got := runAbsentOp(t, m, fs, "ListViews", tc.req, "attacker", outsider)
			assert.Equal(t, 403, got.status, "%s: %s", tc.name, got.body.Message)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type, tc.name)
		}
	})

	t.Run("CreateNamespace", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			arn  string
		}{
			{"denied bucket", mustBucketARN(t)},
			{"missing bucket", ghostARN},
			{"plain bucket", plainARN},
		} {
			got := runAbsentOp(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{TableBucketARN: tc.arn, Namespace: []string{"ns"}}, "attacker", outsider)
			assert.Equal(t, 403, got.status, "%s: %s", tc.name, got.body.Message)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type, tc.name)
		}
	})

	t.Run("GetTableBucket and ListNamespaces", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			arn  string
		}{
			{"denied bucket", mustBucketARN(t)},
			{"missing bucket", ghostARN},
			{"plain bucket", plainARN},
		} {
			for _, op := range []string{"GetTableBucket", "ListNamespaces"} {
				var req interface{} = &GetTableBucketRequest{TableBucketARN: tc.arn}
				if op == "ListNamespaces" {
					req = &ListNamespacesRequest{TableBucketARN: tc.arn}
				}
				got := runAbsentOp(t, m, fs, op, req, "attacker", outsider)
				assert.Equal(t, 404, got.status, "%s %s: %s", op, tc.name, got.body.Message)
				assert.Equal(t, ErrCodeNoSuchBucket, got.body.Type, "%s %s", op, tc.name)
			}
		}
	})

	t.Run("tag operations", func(t *testing.T) {
		missingTableARN := absentTableARN(t, renameTestBucket, "ns", "ghost")
		deniedTableARN := absentTableARN(t, renameTestBucket, "ns", "t")
		for _, tc := range []struct {
			name string
			op   string
			req  interface{}
		}{
			{"TagResource denied table", "TagResource", &TagResourceRequest{ResourceARN: deniedTableARN, Tags: map[string]string{"k": "v"}}},
			{"TagResource missing table", "TagResource", &TagResourceRequest{ResourceARN: missingTableARN, Tags: map[string]string{"k": "v"}}},
			{"TagResource missing bucket", "TagResource", &TagResourceRequest{ResourceARN: ghostARN, Tags: map[string]string{"k": "v"}}},
			{"ListTagsForResource denied table", "ListTagsForResource", &ListTagsForResourceRequest{ResourceARN: deniedTableARN}},
			{"ListTagsForResource missing table", "ListTagsForResource", &ListTagsForResourceRequest{ResourceARN: missingTableARN}},
			{"ListTagsForResource missing bucket", "ListTagsForResource", &ListTagsForResourceRequest{ResourceARN: ghostARN}},
			{"UntagResource denied table", "UntagResource", &UntagResourceRequest{ResourceARN: deniedTableARN, TagKeys: []string{"k"}}},
			{"UntagResource missing table", "UntagResource", &UntagResourceRequest{ResourceARN: missingTableARN, TagKeys: []string{"k"}}},
			{"UntagResource missing bucket", "UntagResource", &UntagResourceRequest{ResourceARN: ghostARN, TagKeys: []string{"k"}}},
		} {
			got := runAbsentOp(t, m, fs, tc.op, tc.req, "attacker", outsider)
			assert.Equal(t, 403, got.status, "%s: %s", tc.name, got.body.Message)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type, tc.name)
		}
	})
}

// A caller the operation would have authorized — the bucket owner, a bucket-
// policy grantee, an identity-level grant, the admin or a trusted handler —
// may still be told the name is absent: the empty list or the not-found stays.
func TestGrantedCallersLearnAbsence(t *testing.T) {
	fs, m := startRenameManager(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)

	ghostARN := absentBucketARN(t, "ghostbkt")
	missingTableARN := absentTableARN(t, renameTestBucket, "ns", "ghost")
	adminActions := []string{s3_constants.ACTION_ADMIN}

	// The bucket owner, and a grantee the bucket policy names, learn absence.
	bucketGrant := fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":"%s","Action":"s3tables:*","Resource":"*"}]}`, "grantee")
	fs.Get(TablesPath, renameTestBucket).Extended[ExtendedKeyPolicy] = []byte(bucketGrant)

	t.Run("bucket grant learns an absent namespace", func(t *testing.T) {
		req := &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}
		for _, name := range []string{DefaultAccountID, "grantee"} {
			got := runAbsentOp(t, m, fs, "ListTables", req, name, nil)
			assert.Equal(t, 200, got.status, "%s", name)
		}
		// And on a namespace that exists they still see the table.
		got := runAbsentOp(t, m, fs, "ListTables", &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}, "grantee", nil)
		assert.Equal(t, 200, got.status)
	})

	t.Run("identity grant learns an absent bucket", func(t *testing.T) {
		got := runAbsentOp(t, m, fs, "ListTables", &ListTablesRequest{TableBucketARN: ghostARN}, "lister", []string{"s3tables:ListTables"})
		assert.Equal(t, 200, got.status)
		got = runAbsentOp(t, m, fs, "ListViews", &ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}, "lister", []string{"s3tables:ListViews"})
		assert.Equal(t, 200, got.status)
	})

	t.Run("admin learns an absent bucket", func(t *testing.T) {
		got := runAbsentOp(t, m, fs, "ListTables", &ListTablesRequest{TableBucketARN: ghostARN}, s3_constants.AccountAdminId, adminActions)
		assert.Equal(t, 200, got.status)
		got = runAbsentOp(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{TableBucketARN: ghostARN, Namespace: []string{"ns"}}, s3_constants.AccountAdminId, adminActions)
		assert.Equal(t, 404, got.status)
		assert.Equal(t, ErrCodeNoSuchBucket, got.body.Type)
	})

	t.Run("tag operations answer 404 to granted callers", func(t *testing.T) {
		got := runAbsentOp(t, m, fs, "TagResource", &TagResourceRequest{ResourceARN: missingTableARN, Tags: map[string]string{"k": "v"}}, "grantee", nil)
		assert.Equal(t, 404, got.status, got.body.Message)
		assert.Equal(t, ErrCodeNoSuchTable, got.body.Type)
		got = runAbsentOp(t, m, fs, "ListTagsForResource", &ListTagsForResourceRequest{ResourceARN: missingTableARN}, DefaultAccountID, nil)
		assert.Equal(t, 404, got.status)
		got = runAbsentOp(t, m, fs, "UntagResource", &UntagResourceRequest{ResourceARN: missingTableARN, TagKeys: []string{"k"}}, s3_constants.AccountAdminId, adminActions)
		assert.Equal(t, 404, got.status)
	})

	t.Run("a trusted handler still lists an absent namespace", func(t *testing.T) {
		m.SetTrusted(true)
		got := runAbsentOp(t, m, fs, "ListTables", &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}, "svc", nil)
		assert.Equal(t, 200, got.status)
	})
}

// A bucket-policy Deny still vetoes the operation on an absent name: a denied
// caller must not trade the denial for a not-found or an empty list.
func TestAbsentResourceBucketDenyStillDenies(t *testing.T) {
	fs, m := startRenameManager(t)
	m.SetTrusted(false)
	m.SetDefaultAllow(false)

	deny := `{"Statement":[{"Effect":"Deny","Principal":"lister","Action":"s3tables:*","Resource":"*"}]}`
	fs.Get(TablesPath, renameTestBucket).Extended[ExtendedKeyPolicy] = []byte(deny)

	got := runAbsentOp(t, m, fs, "TagResource",
		&TagResourceRequest{ResourceARN: absentTableARN(t, renameTestBucket, "ns", "ghost"), Tags: map[string]string{"k": "v"}},
		"lister", []string{"s3tables:TagResource"})
	assert.Equal(t, 403, got.status, got.body.Message)
	assert.Equal(t, ErrCodeAccessDenied, got.body.Type)

	for _, tc := range []struct {
		op  string
		req interface{}
	}{
		{"ListTables", &ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
		{"ListTables", &ListTablesRequest{TableBucketARN: mustBucketARN(t)}},
		{"ListViews", &ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
	} {
		got := runAbsentOp(t, m, fs, tc.op, tc.req, "lister", []string{"s3tables:ListTables", "s3tables:ListViews"})
		assert.Equal(t, 403, got.status, "%s: %s", tc.op, got.body.Message)
	}
}
