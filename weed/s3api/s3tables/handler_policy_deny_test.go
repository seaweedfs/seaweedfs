package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const policyDenyBucket = "denybkt"
const policyDenyWriter = "writer"

func policyDenyBucketARN(t *testing.T) string {
	t.Helper()
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, policyDenyBucket)
	require.NoError(t, err)
	return arn
}

func policyDenyTableARN(t *testing.T) string {
	t.Helper()
	arn, err := BuildTableARN(DefaultRegion, DefaultAccountID, policyDenyBucket, "ns", "t")
	require.NoError(t, err)
	return arn
}

// startPolicyDenyManager seeds a bucket/namespace/table where the bucket policy
// grants writer s3tables:* while the table policy denies writer everything.
func startPolicyDenyManager(t *testing.T) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs := s3tablestest.Start(t)

	bucketMeta, _ := json.Marshal(tableBucketMetadata{Name: policyDenyBucket, OwnerAccountID: DefaultAccountID})
	bucketAllow := fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":"%s","Action":"s3tables:*","Resource":"*"}]}`, policyDenyWriter)
	fs.Put(TablesPath, policyDenyBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
		ExtendedKeyPolicy:      []byte(bucketAllow),
	})

	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(policyDenyBucket), "ns", map[string][]byte{ExtendedKeyMetadata: nsMeta})

	tableMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "t",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  3,
		MetadataLocation: "s3://" + policyDenyBucket + "/ns/t/metadata/v3.metadata.json",
	})
	tableDeny := fmt.Sprintf(`{"Statement":[{"Effect":"Deny","Principal":"%s","Action":"s3tables:*","Resource":%q}]}`, policyDenyWriter, policyDenyTableARN(t))
	fs.Put(GetNamespacePath(policyDenyBucket, "ns"), "t", map[string][]byte{
		ExtendedKeyMetadata:        tableMeta,
		ExtendedKeyMetadataVersion: []byte("3"),
		ExtendedKeyPolicy:          []byte(tableDeny),
	})

	tablePath := GetTablePath(policyDenyBucket, "ns", "t")
	fs.Put(tablePath, "metadata", nil)
	fs.Put(tablePath, "data", nil)
	fs.Put(path.Join(tablePath, "metadata"), "v3.metadata.json", nil)

	return fs, NewManager()
}

func runPolicyOp(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, op string, req interface{}, identity string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.Client), op, req, nil, identity)
}

func deniedTableReq(t *testing.T) (string, string, string) {
	return policyDenyBucketARN(t), "ns", "t"
}

// A table policy Deny must hold even when the bucket policy allows the same
// action: the writer may not strip or replace the policy guarding the table.
func TestTablePolicyDenyProtectsPolicyOps(t *testing.T) {
	fs, m := startPolicyDenyManager(t)
	bucketARN, ns, name := deniedTableReq(t)

	require.Error(t, runPolicyOp(t, m, fs, "GetTablePolicy", &GetTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, policyDenyWriter), "writer must not read the table policy")

	require.Error(t, runPolicyOp(t, m, fs, "PutTablePolicy", &PutTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
		ResourcePolicy: `{"Statement":[{"Effect":"Allow","Principal":"writer","Action":"s3tables:*","Resource":"*"}]}`,
	}, policyDenyWriter), "writer must not replace the table policy")

	require.Error(t, runPolicyOp(t, m, fs, "DeleteTablePolicy", &DeleteTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, policyDenyWriter), "writer must not delete the table policy")

	// The deny still holds, so the table cannot be read or deleted.
	require.Error(t, runPolicyOp(t, m, fs, "GetTable", &GetTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, policyDenyWriter))
	require.Error(t, runPolicyOp(t, m, fs, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, policyDenyWriter))

	entry := fs.Get(GetNamespacePath(policyDenyBucket, "ns"), "t")
	require.NotNil(t, entry)
	_, ok := entry.Extended[ExtendedKeyPolicy]
	assert.True(t, ok, "table policy must still be in place")
	_, ok = entry.Extended[ExtendedKeyMetadata]
	assert.True(t, ok, "table must still exist")
}

// Tag and maintenance operations on a table carry the table ARN as their
// resource, so the table's own policy Deny applies to them as well.
func TestTablePolicyDenyCoversTagAndMaintenanceOps(t *testing.T) {
	fs, m := startPolicyDenyManager(t)
	bucketARN, ns, name := deniedTableReq(t)
	tableARN := policyDenyTableARN(t)

	require.Error(t, runPolicyOp(t, m, fs, "TagResource", &TagResourceRequest{
		ResourceARN: tableARN, Tags: map[string]string{"k": "v"},
	}, policyDenyWriter), "writer must not tag the table")

	require.Error(t, runPolicyOp(t, m, fs, "UntagResource", &UntagResourceRequest{
		ResourceARN: tableARN, TagKeys: []string{"k"},
	}, policyDenyWriter), "writer must not untag the table")

	require.Error(t, runPolicyOp(t, m, fs, "ListTagsForResource", &ListTagsForResourceRequest{
		ResourceARN: tableARN,
	}, policyDenyWriter), "writer must not list the table's tags")

	forged, err := BuildTableARN(DefaultRegion, "111111111111", policyDenyBucket, ns, name)
	require.NoError(t, err)
	require.Error(t, runPolicyOp(t, m, fs, "TagResource", &TagResourceRequest{
		ResourceARN: forged, Tags: map[string]string{"k": "v"},
	}, policyDenyWriter), "a forged account in the ARN must not bypass the table Deny")

	require.Error(t, runPolicyOp(t, m, fs, "PutTableMaintenanceConfiguration", &PutTableMaintenanceConfigurationRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
		Type: MaintenanceTypeIcebergCompaction, Value: &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled},
	}, policyDenyWriter), "writer must not change the table's maintenance configuration")

	require.Error(t, runPolicyOp(t, m, fs, "GetTableMaintenanceJobStatus", &GetTableMaintenanceJobStatusRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, policyDenyWriter), "writer must not read the table's maintenance status")
}

// A Deny conditioned on a resource tag applies only while the stored tag
// matches, so the veto must evaluate the tags held on the table.
func TestTablePolicyDenyHonorsTagCondition(t *testing.T) {
	bucketARN, ns, name := deniedTableReq(t)
	conditionalDeny := fmt.Sprintf(`{"Statement":[{"Effect":"Deny","Principal":%q,"Action":"s3tables:*","Resource":%q,"Condition":{"StringEquals":{"aws:ResourceTag/protected":"true"}}}]}`,
		policyDenyWriter, policyDenyTableARN(t))

	seed := func(t *testing.T, tags map[string]string) (*s3tablestest.MemFiler, *Manager) {
		t.Helper()
		fs, m := startPolicyDenyManager(t)
		entry := fs.Get(GetNamespacePath(policyDenyBucket, ns), name)
		require.NotNil(t, entry)
		entry.Extended[ExtendedKeyPolicy] = []byte(conditionalDeny)
		if tagData, err := json.Marshal(tags); err == nil {
			entry.Extended[ExtendedKeyTags] = tagData
		}
		return fs, m
	}

	putPolicy := &PutTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
		ResourcePolicy: `{"Statement":[{"Effect":"Allow","Principal":"writer","Action":"s3tables:*","Resource":"*"}]}`,
	}
	putMaintenance := &PutTableMaintenanceConfigurationRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
		Type: MaintenanceTypeIcebergCompaction, Value: &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled},
	}

	t.Run("matching tag", func(t *testing.T) {
		fs, m := seed(t, map[string]string{"protected": "true"})
		require.Error(t, runPolicyOp(t, m, fs, "PutTablePolicy", putPolicy, policyDenyWriter))
		require.Error(t, runPolicyOp(t, m, fs, "PutTableMaintenanceConfiguration", putMaintenance, policyDenyWriter))
	})

	t.Run("other tag value", func(t *testing.T) {
		fs, m := seed(t, map[string]string{"protected": "false"})
		require.NoError(t, runPolicyOp(t, m, fs, "PutTablePolicy", putPolicy, policyDenyWriter))
		require.NoError(t, runPolicyOp(t, m, fs, "PutTableMaintenanceConfiguration", putMaintenance, policyDenyWriter))
	})
}

// The policy being replaced is still the resource's policy: a Deny on
// PutTableBucketPolicy inside the existing bucket policy must veto the write
// even for the bucket owner under the open fallback.
func TestBucketPolicyDenyVetoesPolicyReplacement(t *testing.T) {
	fs := s3tablestest.Start(t)

	bucketMeta, _ := json.Marshal(tableBucketMetadata{Name: policyDenyBucket, OwnerAccountID: DefaultAccountID})
	denyReplace := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:PutTableBucketPolicy","Resource":"*"}]}`
	fs.Put(TablesPath, policyDenyBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
		ExtendedKeyPolicy:      []byte(denyReplace),
	})

	m := NewManager()
	m.SetDefaultAllow(true)

	err := runPolicyOp(t, m, fs, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: policyDenyBucketARN(t),
		ResourcePolicy: `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*"}]}`,
	}, "")
	require.Error(t, err, "existing bucket policy Deny must veto replacing it")

	entry := fs.Get(TablesPath, policyDenyBucket)
	require.NotNil(t, entry)
	assert.Equal(t, denyReplace, string(entry.Extended[ExtendedKeyPolicy]), "bucket policy must be unchanged")
}
