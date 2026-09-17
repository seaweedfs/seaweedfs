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
