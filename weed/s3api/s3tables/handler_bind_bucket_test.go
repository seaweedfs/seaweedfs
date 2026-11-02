package s3tables

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recreatedBucket is the record of the test bucket deleted and created again:
// another owner, and a creation time of its own.
func recreatedBucket(t *testing.T) map[string][]byte {
	t.Helper()
	meta, err := json.Marshal(tableBucketMetadata{
		Name:           renameTestBucket,
		OwnerAccountID: "111122223333",
		CreatedAt:      time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	return map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    meta,
	}
}

// bucketWriteOp is a write to the test bucket, with the attributes the bucket
// carries for it besides its record.
type bucketWriteOp struct {
	operation string
	input     func(t *testing.T) interface{}
	extra     map[string][]byte
}

func bucketWriteOps() []bucketWriteOp {
	return []bucketWriteOp{
		{operation: "PutTableBucketPolicy", input: func(t *testing.T) interface{} {
			return &PutTableBucketPolicyRequest{TableBucketARN: mustBucketARN(t), ResourcePolicy: bindTestPolicy}
		}},
		{operation: "DeleteTableBucketPolicy", input: func(t *testing.T) interface{} {
			return &DeleteTableBucketPolicyRequest{TableBucketARN: mustBucketARN(t)}
		}, extra: map[string][]byte{ExtendedKeyPolicy: []byte(bindTestPolicy)}},
		{operation: "TagResource", input: func(t *testing.T) interface{} {
			return &TagResourceRequest{ResourceARN: mustBucketARN(t), Tags: map[string]string{"k": "v"}}
		}},
		{operation: "UntagResource", input: func(t *testing.T) interface{} {
			return &UntagResourceRequest{ResourceARN: mustBucketARN(t), TagKeys: []string{"k"}}
		}, extra: map[string][]byte{ExtendedKeyTags: []byte(`{"k":"v","keep":"1"}`)}},
		{operation: "PutTableBucketMaintenanceConfiguration", input: func(t *testing.T) interface{} {
			return &PutTableBucketMaintenanceConfigurationRequest{
				TableBucketARN: mustBucketARN(t),
				Type:           MaintenanceTypeIcebergUnreferencedFileRemoval,
				Value:          &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled},
			}
		}},
	}
}

// A write to a bucket's policy, tags or maintenance settings is authorized
// against the bucket it read. If that bucket is deleted and another is created under the same name
// before the write, the values it compares can match the new bucket's just as
// well. The write checks the bucket by its creation time and is refused, and
// the new bucket is kept as it was created.
func TestBucketWritesSpareARecreatedBucket(t *testing.T) {
	for _, op := range bucketWriteOps() {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			extended := extendedOf(t, fs, TablesPath, renameTestBucket)
			recreated := recreatedBucket(t)
			for key, value := range op.extra {
				extended[key] = value
				recreated[key] = value
			}
			fs.Put(TablesPath, renameTestBucket, extended)
			fs.BeforeUpdate = func() { fs.Put(TablesPath, renameTestBucket, recreated) }

			code, resp := runHandler(t, m, fs.Client, op.operation, op.input(t))

			assert.Equal(t, http.StatusConflict, code, "%+v", resp)
			assert.Equal(t, ErrCodeConflict, resp.Type, "%+v", resp)
			assert.Equal(t, recreated, fs.Get(TablesPath, renameTestBucket).Extended, "the new bucket is left as it was created")
		})
	}
}

// The same writes still land on the bucket they were authorized against.
func TestBucketWritesLandOnTheAuthorizedBucket(t *testing.T) {
	for _, op := range bucketWriteOps() {
		t.Run(op.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			extended := extendedOf(t, fs, TablesPath, renameTestBucket)
			for key, value := range op.extra {
				extended[key] = value
			}
			fs.Put(TablesPath, renameTestBucket, extended)

			code, resp := runHandler(t, m, fs.Client, op.operation, op.input(t))

			assert.Equal(t, http.StatusOK, code, "%+v", resp)
		})
	}
}
