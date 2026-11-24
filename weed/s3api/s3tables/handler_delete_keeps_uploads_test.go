package s3tables

import (
	"context"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
		upload(fs, "v1", time.Now().Add(time.Minute))

		requireOK(t, m, fs, "DeleteView", &DeleteViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v1"})

		assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "v1")+"/data", "file.parquet"))
		require.NotNil(t, fs.Get(nsPath, "v1"))
		assert.Empty(t, fs.Get(nsPath, "v1").Extended[ExtendedKeyMetadata])

		status, resp := runHandler(t, m, fs.Client, "CreateView", &CreateViewRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "v1"})
		assert.Equal(t, http.StatusConflict, status)
		assert.Equal(t, "v1 already exists and is not a table or view", resp.Message)
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

	t.Run("table purges a file from its own second", func(t *testing.T) {
		fs, m := startRenameManager(t)
		requireOK(t, m, fs, "CreateTable", &CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg,
			MetadataLocation: renameLocation("ns/t2/metadata/v1.metadata.json")})
		upload(fs, "t2", time.Now())

		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2"})

		assert.Nil(t, fs.Get(nsPath, "t2"))
	})

	t.Run("staged table purges files written before its commit", func(t *testing.T) {
		fs, m := startRenameManager(t)
		stagedAt := time.Now().Add(-time.Minute)
		upload(fs, "t2", stagedAt.Add(30*time.Second))
		require.NoError(t, m.Execute(WithTableStagedAt(context.Background(), stagedAt), NewManagerClient(fs.Client), "CreateTable",
			&CreateTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2", Format: FormatIceberg,
				MetadataLocation: renameLocation("ns/t2/metadata/v1.metadata.json")}, nil, "admin"))

		requireOK(t, m, fs, "DeleteTable", &DeleteTableRequest{TableBucketARN: mustBucketARN(t), Namespace: []string{"ns"}, Name: "t2"})

		assert.Nil(t, fs.Get(nsPath, "t2"))
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

// An S3 object stored on a directory entry, below the walked path or on it,
// counts like a file.
func TestDirHoldsFilesCountsDirectoryObjects(t *testing.T) {
	older := time.Now().Add(-time.Hour)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	dirObject := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, IsDirectory: true, Content: []byte("x"),
			Attributes: &filer_pb.FuseAttributes{Mime: "text/plain", Crtime: older.Unix()}}
	}
	fs, _ := startRenameManager(t)
	ctx := context.Background()

	fs.Put(nsPath, "below", nil)
	_, err := fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: path.Join(nsPath, "below"), Entry: dirObject("checkpoint")})
	require.NoError(t, err)
	holds, err := DirHoldsFiles(ctx, fs.Client, path.Join(nsPath, "below"), time.Now())
	require.NoError(t, err)
	assert.True(t, holds, "object on a directory under the path")

	_, err = fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: nsPath, Entry: dirObject("on")})
	require.NoError(t, err)
	holds, err = DirHoldsFiles(ctx, fs.Client, path.Join(nsPath, "on"), time.Now())
	require.NoError(t, err)
	assert.True(t, holds, "object on the path itself")

	fs.Put(nsPath, "plain", nil)
	fs.Put(path.Join(nsPath, "plain"), "sub", nil)
	holds, err = DirHoldsFiles(ctx, fs.Client, path.Join(nsPath, "plain"), time.Now())
	require.NoError(t, err)
	assert.False(t, holds, "plain directories hold nothing")
}
