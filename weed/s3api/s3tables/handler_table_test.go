package s3tables

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataVersionFromLocation(t *testing.T) {
	cases := []struct {
		location string
		want     int
	}{
		{"s3://bucket/ns/tbl/metadata/v1.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/v7.metadata.json", 7},
		{"v42.metadata.json", 42},
		{"s3://bucket/ns/tbl/metadata/00003-9f1c2b3a-4d5e-6f70-8192-a3b4c5d6e7f8.metadata.json", 3},
		{"00012-abcdef.metadata.json", 12},
		{"s3://bucket/ns/tbl/metadata/00000-abc.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/v0.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/garbage.metadata.json", 1},
		{"", 1},
	}
	for _, c := range cases {
		if got := metadataVersionFromLocation(c.location); got != c.want {
			t.Errorf("metadataVersionFromLocation(%q) = %d, want %d", c.location, got, c.want)
		}
	}
}

// A registration keeps the metadata it was given in the record, the way
// CreateTable does, so GetTable hands it back alongside the location.
func TestRegisterTableStoresMetadata(t *testing.T) {
	fs, m := startRenameManager(t)

	const tableUUID = "6f1d0c5e-7a4b-4d2e-9c3f-1a2b3c4d5e6f"
	location := "s3://" + renameTestBucket + "/ns/reg/metadata/00004-6f1d0c5e.metadata.json"
	metadata := &TableMetadata{
		Iceberg:      &IcebergMetadata{TableUUID: tableUUID},
		FullMetadata: json.RawMessage(`{"format-version":2,"table-uuid":"` + tableUUID + `"}`),
	}

	status, resp := runHandler(t, m, fs.Client, "RegisterTable", &RegisterTableRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "reg",
		MetadataLocation: location,
		Metadata:         metadata,
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	got, err := runGetTable(t, m, fs, "ns", "reg")
	require.NoError(t, err)
	assert.Equal(t, location, got.MetadataLocation)
	assert.Equal(t, 4, got.MetadataVersion)
	require.NotNil(t, got.Metadata, "the registered metadata must be kept with the record")
	require.NotNil(t, got.Metadata.Iceberg)
	assert.Equal(t, tableUUID, got.Metadata.Iceberg.TableUUID)
	assert.JSONEq(t, string(metadata.FullMetadata), string(got.Metadata.FullMetadata))
}

// UpdateTable drops the record's stored metadata copy when asked, leaving the
// table tracked by its location — a commit whose metadata outgrew the request
// limit must not leave the stale copy shadowing the new file.
func TestUpdateTableClearsFullMetadata(t *testing.T) {
	fs, m := startRenameManager(t)

	const tableUUID = "6f1d0c5e-7a4b-4d2e-9c3f-1a2b3c4d5e6f"
	location := "s3://" + renameTestBucket + "/ns/reg/metadata/00004-6f1d0c5e.metadata.json"
	status, resp := runHandler(t, m, fs.Client, "RegisterTable", &RegisterTableRequest{
		TableBucketARN:   mustBucketARN(t),
		Namespace:        []string{"ns"},
		Name:             "reg",
		MetadataLocation: location,
		Metadata: &TableMetadata{
			Iceberg:      &IcebergMetadata{TableUUID: tableUUID},
			FullMetadata: json.RawMessage(`{"format-version":2,"table-uuid":"` + tableUUID + `"}`),
		},
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	newLocation := "s3://" + renameTestBucket + "/ns/reg/metadata/00005-6f1d0c5e.metadata.json"
	status, resp = runHandler(t, m, fs.Client, "UpdateTable", &UpdateTableRequest{
		TableBucketARN:    mustBucketARN(t),
		Namespace:         []string{"ns"},
		Name:              "reg",
		Metadata:          &TableMetadata{Iceberg: &IcebergMetadata{TableUUID: tableUUID}},
		MetadataVersion:   5,
		MetadataLocation:  newLocation,
		ClearFullMetadata: true,
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	got, err := runGetTable(t, m, fs, "ns", "reg")
	require.NoError(t, err)
	assert.Equal(t, newLocation, got.MetadataLocation)
	assert.Equal(t, 5, got.MetadataVersion)
	require.NotNil(t, got.Metadata)
	require.NotNil(t, got.Metadata.Iceberg)
	assert.Equal(t, tableUUID, got.Metadata.Iceberg.TableUUID)
	assert.Empty(t, got.Metadata.FullMetadata, "the stored copy must be dropped, not shadow the location")
}
