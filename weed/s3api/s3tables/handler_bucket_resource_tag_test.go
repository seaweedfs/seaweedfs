package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bucket policy Deny keyed on aws:ResourceTag must match the bucket's own
// tags on every operation whose resource is the bucket.
func TestBucketPolicyDenyHonorsBucketResourceTag(t *testing.T) {
	bucketARN := policyDenyBucketARN(t)
	seed := func(t *testing.T, env string) func(op string, req interface{}, resp interface{}) error {
		t.Helper()
		fs, m := startPolicyDenyManager(t)
		entry := fs.Get(TablesPath, policyDenyBucket)
		require.NotNil(t, entry)
		entry.Extended[ExtendedKeyPolicy] = []byte(fmt.Sprintf(`{"Statement":[`+
			`{"Effect":"Allow","Principal":%q,"Action":"s3tables:*","Resource":"*"},`+
			`{"Effect":"Deny","Principal":%q,"Action":"s3tables:*","Resource":%q,"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`,
			policyDenyWriter, policyDenyWriter, bucketARN))
		tags, _ := json.Marshal(map[string]string{"env": env})
		entry.Extended[ExtendedKeyTags] = tags
		nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"empty"}, OwnerAccountID: DefaultAccountID})
		fs.Put(GetTableBucketPath(policyDenyBucket), "empty", map[string][]byte{ExtendedKeyMetadata: nsMeta})
		return func(op string, req interface{}, resp interface{}) error {
			return m.Execute(context.Background(), NewManagerClient(fs.Client), op, req, resp, policyDenyWriter)
		}
	}
	ops := []struct {
		op  string
		req interface{}
	}{
		{"GetTableBucket", &GetTableBucketRequest{TableBucketARN: bucketARN}},
		{"GetTableBucketPolicy", &GetTableBucketPolicyRequest{TableBucketARN: bucketARN}},
		{"GetTableBucketMaintenanceConfiguration", &GetTableBucketMaintenanceConfigurationRequest{TableBucketARN: bucketARN}},
		{"CreateNamespace", &CreateNamespaceRequest{TableBucketARN: bucketARN, Namespace: []string{"fresh"}}},
		{"GetNamespace", &GetNamespaceRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}}},
		{"ListNamespaces", &ListNamespacesRequest{TableBucketARN: bucketARN}},
		{"ListTables", &ListTablesRequest{TableBucketARN: bucketARN}},
		{"DeleteNamespace", &DeleteNamespaceRequest{TableBucketARN: bucketARN, Namespace: []string{"empty"}}},
		{"DeleteTableBucketPolicy", &DeleteTableBucketPolicyRequest{TableBucketARN: bucketARN}},
	}

	listed := func(run func(string, interface{}, interface{}) error) []TableBucketSummary {
		var resp ListTableBucketsResponse
		require.NoError(t, run("ListTableBuckets", &ListTableBucketsRequest{}, &resp))
		return resp.TableBuckets
	}

	t.Run("matching tag", func(t *testing.T) {
		for _, o := range ops {
			require.Error(t, seed(t, "prod")(o.op, o.req, nil), o.op)
		}
		require.Empty(t, listed(seed(t, "prod")))
	})

	t.Run("other tag value", func(t *testing.T) {
		for _, o := range ops {
			require.NoError(t, seed(t, "dev")(o.op, o.req, nil), o.op)
		}
		require.Len(t, listed(seed(t, "dev")), 1)
	})
}

// Deleting the bucket policy is refused when the bucket's tags change between
// the authorization and the deletion.
func TestDeleteTableBucketPolicyRechecksTagsAtDeletion(t *testing.T) {
	bucketARN := policyDenyBucketARN(t)
	fs, m := startPolicyDenyManager(t)
	entry := fs.Get(TablesPath, policyDenyBucket)
	require.NotNil(t, entry)
	policy := []byte(fmt.Sprintf(`{"Statement":[`+
		`{"Effect":"Allow","Principal":%q,"Action":"s3tables:*","Resource":"*"},`+
		`{"Effect":"Deny","Principal":%q,"Action":"s3tables:*","Resource":%q,"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`,
		policyDenyWriter, policyDenyWriter, bucketARN))
	entry.Extended[ExtendedKeyPolicy] = policy
	entry.Extended[ExtendedKeyTags] = []byte(`{"env":"dev"}`)
	fs.Put(TablesPath, policyDenyBucket, entry.Extended)
	fs.BeforeUpdate = func() {
		current := fs.Get(TablesPath, policyDenyBucket)
		current.Extended[ExtendedKeyTags] = []byte(`{"env":"prod"}`)
		fs.Put(TablesPath, policyDenyBucket, current.Extended)
	}

	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "DeleteTableBucketPolicy",
		&DeleteTableBucketPolicyRequest{TableBucketARN: bucketARN}, nil, policyDenyWriter)
	var apiErr *S3TablesError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, ErrCodeConflict, apiErr.Type)
	require.Equal(t, policy, fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy])
}
