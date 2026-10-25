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

type s3TablesHTTPError struct {
	status int
	body   S3TablesError
}

func runUnauthorizedRequest(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, operation string, input interface{}) s3TablesHTTPError {
	t.Helper()

	body, err := json.Marshal(input)
	require.NoError(t, err)
	identity := &testIdentity{Name: "attacker", Account: &testIdentityAccount{Id: "attacker"}, Actions: []string{"Read"}}
	ctx := s3_constants.SetIdentityInContext(context.Background(), identity)
	req, err := newManagerRequest(ctx, operation, body, "attacker")
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
	result := recorder.Result()
	defer result.Body.Close()

	var response S3TablesError
	require.NoError(t, json.NewDecoder(result.Body).Decode(&response))
	return s3TablesHTTPError{status: result.StatusCode, body: response}
}

func TestTableBucketAuthorizationDenialMatchesMissing(t *testing.T) {
	existing, manager := startRenameManager(t)
	missing := s3tablestest.Start(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	for _, operation := range []string{"GetTableBucket", "DeleteTableBucket"} {
		t.Run(operation, func(t *testing.T) {
			var request interface{} = &GetTableBucketRequest{TableBucketARN: mustBucketARN(t)}
			if operation == "DeleteTableBucket" {
				request = &DeleteTableBucketRequest{TableBucketARN: mustBucketARN(t)}
			}

			want := runUnauthorizedRequest(t, manager, missing, operation, request)
			got := runUnauthorizedRequest(t, manager, existing, operation, request)
			assert.Equal(t, want, got)
			assert.Equal(t, 404, got.status)
			assert.Equal(t, ErrCodeNoSuchBucket, got.body.Type)
		})
	}
}

func TestNamespaceAuthorizationDenialMatchesMissing(t *testing.T) {
	existing, manager := startRenameManager(t)
	missing := s3tablestest.Start(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	for _, operation := range []string{"GetNamespace", "UpdateNamespace", "DeleteNamespace"} {
		t.Run(operation, func(t *testing.T) {
			var request interface{} = &GetNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}
			switch operation {
			case "UpdateNamespace":
				request = &UpdateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}
			case "DeleteNamespace":
				request = &DeleteNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}
			}

			want := runUnauthorizedRequest(t, manager, missing, operation, request)
			got := runUnauthorizedRequest(t, manager, existing, operation, request)
			assert.Equal(t, want, got)
			assert.Equal(t, 404, got.status)
			assert.Equal(t, ErrCodeNoSuchNamespace, got.body.Type)
		})
	}
}

func TestTableAuthorizationDenialMatchesMissing(t *testing.T) {
	existing, manager := startRenameManager(t)
	missing := s3tablestest.Start(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	for _, operation := range []string{"GetTable", "UpdateTable", "DeleteTable", "RenameTable"} {
		t.Run(operation, func(t *testing.T) {
			var request interface{} = &GetTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t"}
			switch operation {
			case "UpdateTable":
				request = &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", VersionToken: "wrong"}
			case "DeleteTable":
				request = &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", VersionToken: "wrong"}
			case "RenameTable":
				request = &RenameTableRequest{TableBucketARN: mustBucketARN(t), SourceNamespace: []string{"ns"}, SourceName: "t", DestNamespace: []string{"ns"}, DestName: "t2"}
			}

			want := runUnauthorizedRequest(t, manager, missing, operation, request)
			got := runUnauthorizedRequest(t, manager, existing, operation, request)
			assert.Equal(t, want, got)
			assert.Equal(t, 404, got.status)
			assert.Equal(t, ErrCodeNoSuchTable, got.body.Type)
		})
	}
}

func TestDeleteTableAuthorizedVersionMismatchStillConflicts(t *testing.T) {
	fs, manager := startRenameManager(t)
	err := manager.Execute(context.Background(), NewManagerClient(fs.Client), "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "t",
		VersionToken:   "wrong",
	}, nil, "")

	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeConflict, s3Err.Type)
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"))
}

func runRequestAs(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, operation string, input interface{}, actions []string) s3TablesHTTPError {
	t.Helper()

	body, err := json.Marshal(input)
	require.NoError(t, err)
	identity := &testIdentity{Name: "reader", Account: &testIdentityAccount{Id: "reader"}, Actions: actions}
	ctx := s3_constants.SetIdentityInContext(context.Background(), identity)
	req, err := newManagerRequest(ctx, operation, body, "reader")
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

// A principal that may read the table is told the write was refused; the
// not-found veil only hides the entry from callers that cannot see it.
func TestDeniedWritesReportForbiddenToReaders(t *testing.T) {
	fs, manager := startRenameManager(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)
	readActions := []string{"s3tables:GetTable"}

	got := runRequestAs(t, manager, fs, "GetTable", &GetTableRequest{
		TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t"}, readActions)
	assert.Equal(t, 200, got.status)

	for _, tc := range []struct {
		operation string
		request   interface{}
	}{
		{"UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t", VersionToken: "any"}},
		{"DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t"}},
		{"RenameTable", &RenameTableRequest{TableBucketARN: mustBucketARN(t), SourceNamespace: []string{"ns"}, SourceName: "t", DestNamespace: []string{"ns"}, DestName: "t2"}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			got := runRequestAs(t, manager, fs, tc.operation, tc.request, readActions)
			assert.Equal(t, 403, got.status)
			assert.Equal(t, ErrCodeAccessDenied, got.body.Type)
		})
	}
}

// A view stored at a table name is hidden from table operations: a principal
// with only s3tables:GetTable must get the same not-found it would see for a
// genuinely missing name, never a 403 that reveals the entry exists.
func TestDeniedTableWritesDoNotRevealViews(t *testing.T) {
	fs, manager := startRenameManager(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	viewMeta, _ := json.Marshal(tableMetadataInternal{
		Name:           "v",
		Namespace:      "ns",
		OwnerAccountID: DefaultAccountID,
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "v", map[string][]byte{
		ExtendedKeyMetadata:  viewMeta,
		ExtendedKeyEntryType: []byte(EntryTypeView),
	})

	readActions := []string{"s3tables:GetTable"}
	for _, tc := range []struct {
		operation string
		request   interface{}
	}{
		{"GetTable", &GetTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v"}},
		{"UpdateTable", &UpdateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v", VersionToken: "any"}},
		{"DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v"}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			got := runRequestAs(t, manager, fs, tc.operation, tc.request, readActions)
			assert.Equal(t, 404, got.status)
			assert.Equal(t, ErrCodeNoSuchTable, got.body.Type)
		})
	}
}

// A caller without a grant must get the same answer whether a list target
// exists or not, and whether the bucket behind it exists at all.
func TestAbsentListTargetsMatchDeniedAnswers(t *testing.T) {
	existing, manager := startRenameManager(t)
	missing := s3tablestest.Start(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	for _, tc := range []struct {
		name      string
		fs        *s3tablestest.MemFiler
		operation string
		denied    interface{}
		request   interface{}
	}{
		{"ListTables missing namespace", existing, "ListTables",
			&ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}},
			&ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
		{"ListTables missing bucket", missing, "ListTables",
			&ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}},
			&ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}}},
		{"ListTables missing bucket, all namespaces", missing, "ListTables",
			&ListTablesRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}},
			&ListTablesRequest{TableBucketARN: mustBucketARN(t)}},
		{"ListViews missing namespace", existing, "ListViews",
			&ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}},
			&ListViewsRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ghost"}}},
		{"CreateNamespace missing bucket", missing, "CreateNamespace",
			&CreateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"newns"}},
			&CreateNamespaceRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"newns"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			denied := runUnauthorizedRequest(t, manager, existing, tc.operation, tc.denied)
			assert.Equal(t, 403, denied.status)
			got := runUnauthorizedRequest(t, manager, tc.fs, tc.operation, tc.request)
			assert.Equal(t, denied, got)
		})
	}
}

// A denied tag operation and the same operation on a missing table must answer
// alike; a granted caller still gets the truthful not-found.
func TestAbsentTagTargetsMatchDeniedAnswers(t *testing.T) {
	existing, manager := startRenameManager(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	existingARN, err := BuildTableARN(DefaultRegion, DefaultAccountID, renameTestBucket, "ns", "t")
	require.NoError(t, err)
	missingARN, err := BuildTableARN(DefaultRegion, DefaultAccountID, renameTestBucket, "ns", "ghost")
	require.NoError(t, err)

	requestFor := func(operation, arn string) interface{} {
		switch operation {
		case "TagResource":
			return &TagResourceRequest{ResourceARN: arn, Tags: map[string]string{"k": "v"}}
		case "UntagResource":
			return &UntagResourceRequest{ResourceARN: arn, TagKeys: []string{"k"}}
		}
		return &ListTagsForResourceRequest{ResourceARN: arn}
	}

	for _, operation := range []string{"TagResource", "ListTagsForResource", "UntagResource"} {
		t.Run(operation, func(t *testing.T) {
			denied := runUnauthorizedRequest(t, manager, existing, operation, requestFor(operation, existingARN))
			assert.Equal(t, 403, denied.status)
			got := runUnauthorizedRequest(t, manager, existing, operation, requestFor(operation, missingARN))
			assert.Equal(t, denied, got)
		})
	}
}

// A directory that holds a plain S3 bucket name is not a table bucket: every
// caller gets the same 404 an absent name gets, not a 500 that reveals the
// name resolves in the filer.
func TestPlainBucketReadsAsAbsent(t *testing.T) {
	existing, manager := startRenameManager(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)
	existing.Put(TablesPath, "plainbkt", nil)

	plainARN, err := BuildBucketARN(DefaultRegion, DefaultAccountID, "plainbkt")
	require.NoError(t, err)

	for _, tc := range []struct {
		operation string
		request   interface{}
	}{
		{"GetTableBucket", &GetTableBucketRequest{TableBucketARN: plainARN}},
		{"ListNamespaces", &ListNamespacesRequest{TableBucketARN: plainARN}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			got := runUnauthorizedRequest(t, manager, existing, tc.operation, tc.request)
			assert.Equal(t, 404, got.status)
			assert.Equal(t, ErrCodeNoSuchBucket, got.body.Type)
		})
	}
}

// A caller with an identity-level grant still learns that a name is absent.
func TestGrantedCallerLearnsAbsence(t *testing.T) {
	missing := s3tablestest.Start(t)
	manager := NewManager()
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	got := runRequestAs(t, manager, missing, "ListTables", &ListTablesRequest{
		TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}},
		[]string{"s3tables:ListTables"})
	assert.Equal(t, 200, got.status)

	missingTableARN, err := BuildTableARN(DefaultRegion, DefaultAccountID, renameTestBucket, "ns", "ghost")
	require.NoError(t, err)
	got = runRequestAs(t, manager, missing, "ListTagsForResource",
		&ListTagsForResourceRequest{ResourceARN: missingTableARN},
		[]string{"s3tables:ListTagsForResource"})
	assert.Equal(t, 404, got.status)
	assert.Equal(t, ErrCodeNoSuchTable, got.body.Type)
}

// A request ARN that names a foreign account still resolves to the local
// names, so the absent-target answer must match the denial the canonical ARN
// gets; otherwise a reformatted ARN restores the existence oracle.
func TestAbsentTagTargetDenySurvivesForeignAccountARN(t *testing.T) {
	fs, manager := startRenameManager(t)
	manager.SetTrusted(false)
	manager.SetDefaultAllow(false)

	bucketMeta, _ := json.Marshal(tableBucketMetadata{Name: renameTestBucket, OwnerAccountID: DefaultAccountID})
	fs.Put(TablesPath, renameTestBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
		ExtendedKeyPolicy: []byte(fmt.Sprintf(
			`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":%q}]}`,
			mustBucketARN(t)+"/table/*")),
	})

	foreignARN := func(table string) string {
		return fmt.Sprintf("arn:aws:s3tables:%s:999999999999:bucket/%s/table/ns/%s",
			DefaultRegion, renameTestBucket, table)
	}
	requestFor := func(operation, arn string) interface{} {
		switch operation {
		case "TagResource":
			return &TagResourceRequest{ResourceARN: arn, Tags: map[string]string{"k": "v"}}
		case "UntagResource":
			return &UntagResourceRequest{ResourceARN: arn, TagKeys: []string{"k"}}
		}
		return &ListTagsForResourceRequest{ResourceARN: arn}
	}

	for _, operation := range []string{"TagResource", "ListTagsForResource", "UntagResource"} {
		t.Run(operation, func(t *testing.T) {
			actions := []string{"s3tables:" + operation}
			denied := runRequestAs(t, manager, fs, operation, requestFor(operation, foreignARN("t")), actions)
			assert.Equal(t, 403, denied.status)
			got := runRequestAs(t, manager, fs, operation, requestFor(operation, foreignARN("ghost")), actions)
			assert.Equal(t, denied, got)
		})
	}
}
