package s3tables

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const nullConditionDeny = `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":null}}}]}`

// startConditionManager seeds startPolicyDenyManager's bucket, namespace and
// table with no table policy and bucketPolicy, if any, stored on the bucket
// the way a release without write-time condition checks could have left it.
func startConditionManager(t *testing.T, bucketPolicy string) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs, m := startPolicyDenyManager(t)
	bucket := fs.Get(TablesPath, policyDenyBucket)
	require.NotNil(t, bucket)
	delete(bucket.Extended, ExtendedKeyPolicy)
	if bucketPolicy != "" {
		bucket.Extended[ExtendedKeyPolicy] = []byte(bucketPolicy)
	}
	table := fs.Get(GetNamespacePath(policyDenyBucket, "ns"), "t")
	require.NotNil(t, table)
	delete(table.Extended, ExtendedKeyPolicy)
	return fs, m
}

func requireS3TablesError(t *testing.T, err error, code string) *S3TablesError {
	t.Helper()
	var s3err *S3TablesError
	require.ErrorAs(t, err, &s3err)
	assert.Equal(t, code, s3err.Type, s3err.Message)
	return s3err
}

// A condition the evaluator cannot compare is refused at write time, on the
// bucket and on the table, and nothing is stored.
func TestPutPolicyRejectsUnusableCondition(t *testing.T) {
	fs, m := startConditionManager(t, "")
	bucketARN, ns, name := deniedTableReq(t)

	err := runPolicyOp(t, m, fs, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: nullConditionDeny,
	}, DefaultAccountID)
	s3err := requireS3TablesError(t, err, ErrCodeInvalidRequest)
	assert.Contains(t, s3err.Message, `"aws:ResourceTag/env"`)
	_, stored := fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the bucket policy must not be written")

	err = runPolicyOp(t, m, fs, "PutTablePolicy", &PutTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name, ResourcePolicy: nullConditionDeny,
	}, DefaultAccountID)
	s3err = requireS3TablesError(t, err, ErrCodeInvalidRequest)
	assert.Contains(t, s3err.Message, `"aws:ResourceTag/env"`)
	_, stored = fs.Get(GetNamespacePath(policyDenyBucket, ns), name).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the table policy must not be written")

	// A string the numeric evaluator would skip is refused like a null one.
	unreadable := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":{"NumericLessThan":{"aws:RequestTag/priority":"low"}}}]}`
	err = runPolicyOp(t, m, fs, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: unreadable,
	}, DefaultAccountID)
	s3err = requireS3TablesError(t, err, ErrCodeInvalidRequest)
	assert.Contains(t, s3err.Message, "NumericLessThan")
	_, stored = fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the bucket policy must not be written")

	valid := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`
	require.NoError(t, runPolicyOp(t, m, fs, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: valid,
	}, DefaultAccountID))
	assert.Equal(t, valid, string(fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]))
}

// The typed decode reads a null Condition as none, so an Allow carrying one is
// refused on the bucket and on the table, and nothing is stored.
func TestPutPolicyRejectsNullAllowCondition(t *testing.T) {
	fs, m := startConditionManager(t, "")
	bucketARN, ns, name := deniedTableReq(t)
	allowNull := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":null}]}`

	requireS3TablesError(t, runPolicyOp(t, m, fs, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: allowNull,
	}, DefaultAccountID), ErrCodeInvalidRequest)
	_, stored := fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the bucket policy must not be written")

	requireS3TablesError(t, runPolicyOp(t, m, fs, "PutTablePolicy", &PutTablePolicyRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name, ResourcePolicy: allowNull,
	}, DefaultAccountID), ErrCodeInvalidRequest)
	_, stored = fs.Get(GetNamespacePath(policyDenyBucket, ns), name).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the table policy must not be written")
}

// A keyless operator stored before write-time checks made its Allow grant
// everyone; it is now unusable, so a non-owner is turned away. GetTableBucket
// hides a bucket the caller may not read, so the refusal is NoSuchBucket,
// while GetTableBucketPolicy answers AccessDenied.
func TestStoredKeylessAllowDoesNotGrant(t *testing.T) {
	keylessAllow := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":{"StringEquals":{}}}]}`
	fs, m := startConditionManager(t, keylessAllow)
	bucketARN := policyDenyBucketARN(t)

	requireS3TablesError(t, runPolicyOp(t, m, fs, "GetTableBucket", &GetTableBucketRequest{
		TableBucketARN: bucketARN,
	}, "reader"), ErrCodeNoSuchBucket)
	requireS3TablesError(t, runPolicyOp(t, m, fs, "GetTableBucketPolicy", &GetTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}, "reader"), ErrCodeAccessDenied)
	require.NoError(t, runPolicyOp(t, m, fs, "GetTableBucket", &GetTableBucketRequest{
		TableBucketARN: bucketARN,
	}, DefaultAccountID), "the owner keeps access")
}

// A bucket policy stored before conditions were validated must not cut the
// connection: the owner gets an authorization error, the admin can still
// remove the policy, and the owner is let back in once it is gone.
func TestStoredUnusableConditionPolicyIsRecoverable(t *testing.T) {
	fs, m := startConditionManager(t, nullConditionDeny)
	bucketARN, ns, name := deniedTableReq(t)

	requireS3TablesError(t, runPolicyOp(t, m, fs, "GetTableBucket", &GetTableBucketRequest{
		TableBucketARN: bucketARN,
	}, DefaultAccountID), ErrCodeNoSuchBucket)
	require.Error(t, runPolicyOp(t, m, fs, "GetTable", &GetTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, DefaultAccountID), "the broken bucket Deny must veto table reads")
	requireS3TablesError(t, runPolicyOp(t, m, fs, "DeleteTableBucketPolicy", &DeleteTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}, DefaultAccountID), ErrCodeAccessDenied)
	assert.Equal(t, nullConditionDeny, string(fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]))

	require.NoError(t, runPolicyOp(t, m, fs, "DeleteTableBucketPolicy", &DeleteTableBucketPolicyRequest{
		TableBucketARN: bucketARN,
	}, s3_constants.AccountAdminId), "the admin must be able to remove the broken policy")
	_, stored := fs.Get(TablesPath, policyDenyBucket).Extended[ExtendedKeyPolicy]
	assert.False(t, stored, "the broken policy must be gone")

	require.NoError(t, runPolicyOp(t, m, fs, "GetTableBucket", &GetTableBucketRequest{
		TableBucketARN: bucketARN,
	}, DefaultAccountID))
	require.NoError(t, runPolicyOp(t, m, fs, "GetTable", &GetTableRequest{
		TableBucketARN: bucketARN, Namespace: []string{ns}, Name: name,
	}, DefaultAccountID))
}
