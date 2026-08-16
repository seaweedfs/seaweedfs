package s3tables

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func updateTableRequest(t *testing.T, metadataLocation string, version int) *UpdateTableRequest {
	t.Helper()
	return &UpdateTableRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "t",
		MetadataVersion:  version,
		MetadataLocation: metadataLocation,
	}
}

// A second commit that reads the same base must not overwrite the first one's
// metadata pointer: the snapshot the first commit published would be lost.
func TestUpdateTableRejectsLostUpdate(t *testing.T) {
	fs, m := startRenameManager(t)

	winnerLocation := "s3://" + renameTestBucket + "/ns/t/metadata/v4-winner.metadata.json"
	fs.beforeUpdate = func() {
		winner, err := json.Marshal(tableMetadataInternal{
			Name:             "t",
			Namespace:        "ns",
			Format:           "ICEBERG",
			OwnerAccountID:   DefaultAccountID,
			MetadataVersion:  4,
			MetadataLocation: winnerLocation,
			VersionToken:     generateVersionToken(),
		})
		require.NoError(t, err)
		entry := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t")
		require.NotNil(t, entry)
		entry.Extended[ExtendedKeyMetadata] = winner
	}

	loserLocation := "s3://" + renameTestBucket + "/ns/t/metadata/v4-loser.metadata.json"
	err := m.Execute(context.Background(), NewManagerClient(fs.client), "UpdateTable",
		updateTableRequest(t, loserLocation, 4), nil, "")

	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeConflict, s3Err.Type)

	got, err := runGetTable(t, m, fs, "ns", "t")
	require.NoError(t, err)
	assert.Equal(t, winnerLocation, got.MetadataLocation, "the first commit must survive")
}

// The write replaces the whole entry, so a policy landing in the same window
// must not be reverted by the commit's stale copy of it.
func TestUpdateTableRejectsWhenAnotherAttributeChanges(t *testing.T) {
	fs, m := startRenameManager(t)

	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	fs.beforeUpdate = func() {
		entry := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t")
		require.NotNil(t, entry)
		entry.Extended[ExtendedKeyPolicy] = policy
	}

	err := m.Execute(context.Background(), NewManagerClient(fs.client), "UpdateTable",
		updateTableRequest(t, "s3://"+renameTestBucket+"/ns/t/metadata/v4.metadata.json", 4), nil, "")

	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeConflict, s3Err.Type)

	entry := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t")
	require.NotNil(t, entry)
	assert.Equal(t, policy, entry.Extended[ExtendedKeyPolicy], "the concurrent policy write must survive")
}

func TestUpdateTableAppliesWithoutContention(t *testing.T) {
	fs, m := startRenameManager(t)

	location := "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json"
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.client), "UpdateTable",
		updateTableRequest(t, location, 4), nil, ""))

	got, err := runGetTable(t, m, fs, "ns", "t")
	require.NoError(t, err)
	assert.Equal(t, location, got.MetadataLocation)
	assert.Equal(t, 4, got.MetadataVersion)
}
