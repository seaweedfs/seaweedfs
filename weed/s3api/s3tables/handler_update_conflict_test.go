package s3tables

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// claimRecord puts a delete claim on an existing catalog entry, the state a
// record is in while another operation owns it.
func claimRecord(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) {
	t.Helper()
	entry := fs.Get(dir, name)
	require.NotNil(t, entry)
	claimed := map[string][]byte{}
	for k, v := range entry.Extended {
		claimed[k] = v
	}
	claimed[ExtendedKeyDeleting] = []byte("delete")
	fs.Put(dir, name, claimed)
}

// A write landing on a record another operation claimed is an expected
// conflict, not a server fault: the caller must see 409.
func TestUpdateViewClaimedRecordConflicts(t *testing.T) {
	fs, m := startRenameManager(t)
	seedView(t, fs, "v")
	claimRecord(t, fs, GetNamespacePath(renameTestBucket, "ns"), "v")

	status, resp := runHandler(t, m, fs.Client, "UpdateView", &UpdateViewRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "v",
		MetadataLocation: "s3://" + renameTestBucket + "/ns/v/metadata/v2.metadata.json",
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
}

func TestTagResourceClaimedRecordConflicts(t *testing.T) {
	fs, m := startRenameManager(t)
	claimRecord(t, fs, GetNamespacePath(renameTestBucket, "ns"), "t")

	tableARN := fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s/table/ns/t", DefaultRegion, DefaultAccountID, renameTestBucket)
	status, resp := runHandler(t, m, fs.Client, "TagResource", &TagResourceRequest{
		ResourceARN: tableARN,
		Tags:        map[string]string{"k": "v"},
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
}

func TestUntagResourceClaimedRecordConflicts(t *testing.T) {
	fs, m := startRenameManager(t)
	claimRecord(t, fs, GetNamespacePath(renameTestBucket, "ns"), "t")

	tableARN := fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s/table/ns/t", DefaultRegion, DefaultAccountID, renameTestBucket)
	status, resp := runHandler(t, m, fs.Client, "UntagResource", &UntagResourceRequest{
		ResourceARN: tableARN,
		TagKeys:     []string{"k"},
	})
	assert.Equal(t, http.StatusConflict, status, "%+v", resp)
	assert.Equal(t, ErrCodeConflict, resp.Type)
}
