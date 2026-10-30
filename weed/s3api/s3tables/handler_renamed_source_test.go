package s3tables

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extendedOf copies the attributes held on an entry, so a later write to the
// entry shows up as a difference.
func extendedOf(t *testing.T, fs *s3tablestest.MemFiler, dir, name string) map[string][]byte {
	t.Helper()
	entry := fs.Get(dir, name)
	require.NotNil(t, entry)
	copied := make(map[string][]byte, len(entry.Extended))
	for key, value := range entry.Extended {
		copied[key] = append([]byte(nil), value...)
	}
	return copied
}

// A rename leaves the source directory in place to hold the data under it, with
// its catalog attributes cleared. That name is no longer a table, so every table
// operation on it answers NoSuchTable and writes nothing to the directory.
func TestRenamedAwayNameIsNoSuchTable(t *testing.T) {
	fs, m := startRenameManager(t)
	requireOK(t, m, fs, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns2"},
	})
	requireOK(t, m, fs, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns2"},
		DestName:        "t",
	})
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	before := extendedOf(t, fs, nsPath, "t")

	bucketARN := mustBucketARN(t)
	ops := []struct {
		operation string
		input     interface{}
	}{
		{"UpdateTable", &UpdateTableRequest{
			TableBucketARN:   bucketARN,
			Namespace:        []string{"ns"},
			Name:             "t",
			MetadataVersion:  4,
			MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json",
		}},
		{"DeleteTable", deleteTableRequest(t, "t")},
		{"PutTablePolicy", &PutTablePolicyRequest{
			TableBucketARN: bucketARN,
			Namespace:      []string{"ns"},
			Name:           "t",
			ResourcePolicy: `{"Statement":[{"Effect":"Allow","Principal":"writer","Action":"s3tables:*","Resource":"*"}]}`,
		}},
		{"GetTablePolicy", &GetTablePolicyRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t"}},
		{"DeleteTablePolicy", &DeleteTablePolicyRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t"}},
		{"PutTableMaintenanceConfiguration", &PutTableMaintenanceConfigurationRequest{
			TableBucketARN: bucketARN,
			Namespace:      []string{"ns"},
			Name:           "t",
			Type:           MaintenanceTypeIcebergCompaction,
			Value:          &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled},
		}},
		{"GetTableMaintenanceConfiguration", &GetTableMaintenanceConfigurationRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t"}},
		{"GetTableMaintenanceJobStatus", &GetTableMaintenanceJobStatusRequest{TableBucketARN: bucketARN, Namespace: []string{"ns"}, Name: "t"}},
	}
	for _, op := range ops {
		t.Run(op.operation, func(t *testing.T) {
			status, resp := runHandler(t, m, fs.Client, op.operation, op.input)
			assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchTable, resp.Type, "%+v", resp)
		})
	}

	assert.Equal(t, before, extendedOf(t, fs, nsPath, "t"), "the renamed-away directory must not be written to")
	status, resp := runHandler(t, m, fs.Client, "GetTable", &GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      []string{"ns2"},
		Name:           "t",
	})
	assert.Equal(t, http.StatusOK, status, "the renamed table must still resolve: %+v", resp)
}
