package app

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/stretchr/testify/require"
)

func TestS3TablesTablesMarksTruncatedList(t *testing.T) {
	data := dash.S3TablesTablesData{
		BucketARN: "arn:aws:s3tables:us-east-1:admin:bucket/analytics",
		Namespace: "sales",
		Tables:    []s3tables.TableSummary{{Name: "orders"}},
	}
	var out strings.Builder
	require.NoError(t, S3TablesTables(data).Render(t.Context(), &out))
	require.NotContains(t, out.String(), "select all covers only these")
	require.Contains(t, out.String(), `class="form-check-input s3tables-select-table" type="checkbox" value="orders"`)

	data.TotalTables, data.Truncated = 1, true
	out.Reset()
	require.NoError(t, S3TablesTables(data).Render(t.Context(), &out))
	require.Contains(t, out.String(), "showing the first 1; select all covers only these")
}
