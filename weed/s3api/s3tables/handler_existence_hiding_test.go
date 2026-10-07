package s3tables

import (
	"context"
	"encoding/json"
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
