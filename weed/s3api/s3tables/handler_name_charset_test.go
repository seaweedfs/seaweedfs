package s3tables

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNameValidationAllowsHyphens(t *testing.T) {
	// Hyphen interior is accepted (Iceberg REST clients use names like these).
	require.NoError(t, validateNamespacePart("rest-integration-test"))
	if _, err := validateTableName("test-create-table"); err != nil {
		t.Fatalf("hyphenated table name rejected: %v", err)
	}

	// A leading or trailing hyphen on a namespace is still rejected (must start
	// and end with a letter or digit), as are characters outside the charset.
	require.Error(t, validateNamespacePart("-leading"))
	require.Error(t, validateNamespacePart("trailing-"))
	require.Error(t, validateNamespacePart("Upper"))

	// Table names must still start with a letter or digit and stay in charset.
	if _, err := validateTableName("-leading"); err == nil {
		t.Fatal("table name with leading hyphen should be rejected")
	}
	if _, err := validateTableName("Upper"); err == nil {
		t.Fatal("uppercase table name should be rejected")
	}
}

func TestParseTableFromARNAllowsHyphens(t *testing.T) {
	bucket, ns, table, err := parseTableFromARN("arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket/table/rest-integration-test/test-create-table")
	require.NoError(t, err)
	require.Equal(t, "my-bucket", bucket)
	require.Equal(t, "rest-integration-test", ns)
	require.Equal(t, "test-create-table", table)
}
