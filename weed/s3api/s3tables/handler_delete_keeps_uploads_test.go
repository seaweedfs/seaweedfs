package s3tables

import (
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Files a record did not write for itself stay when it is deleted: anything
// but metadata under a view, and files already under a table's path when the
// table was created. The files a table wrote after its create still go.
func TestDeleteKeepsFilesOlderThanTheRecord(t *testing.T) {
	older := time.Now().Add(-time.Hour)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	upload := func(fs *s3tablestest.MemFiler, name string, crtime time.Time) {
		if fs.Get(nsPath, name) == nil {
			fs.Put(nsPath, name, nil)
		}
		fs.Put(path.Join(nsPath, name), "data", nil)
		fs.PutFile(path.Join(nsPath, name, "data"), "file.parquet", crtime)
	}

	t.Run("view", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateView", &CreateViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v1"})
		upload(fs, "v1", time.Now())

		requireOK(t, m, fs, "DeleteView", &DeleteViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v1"})

		assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "v1")+"/data", "file.parquet"))
		require.NotNil(t, fs.Get(nsPath, "v1"))
		assert.Empty(t, fs.Get(nsPath, "v1").Extended[ExtendedKeyMetadata])
	})

	t.Run("colocated table", func(t *testing.T) {
		fs, m := startRenameManager(t)
		upload(fs, "t2", older)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg})

		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2"})

		assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "t2")+"/data", "file.parquet"))
		require.NotNil(t, fs.Get(nsPath, "t2"))
		assert.Empty(t, fs.Get(nsPath, "t2").Extended[ExtendedKeyMetadata])
	})

	t.Run("table located over other files", func(t *testing.T) {
		fs, m := startRenameManager(t)
		upload(fs, "other", older)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/other/metadata/v1.metadata.json")})

		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2"})

		assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "other")+"/data", "file.parquet"))
		require.NotNil(t, fs.Get(nsPath, "t2"))
		assert.Empty(t, fs.Get(nsPath, "t2").Extended[ExtendedKeyMetadata])
	})

	t.Run("table purges its own files", func(t *testing.T) {
		fs, m := startRenameManager(t)
		tablePath := GetTablePath(renameTestBucket, "ns", "t2")
		fs.PutFile(tablePath+"/metadata", "v1.metadata.json", older)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/t2/metadata/v1.metadata.json")})
		fs.PutFile(tablePath+"/data", "file.parquet", time.Now().Add(time.Second))

		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2"})

		assert.Nil(t, fs.Get(nsPath, "t2"))
		assert.Nil(t, fs.Get(tablePath+"/data", "file.parquet"))
	})
}
