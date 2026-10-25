package s3tables

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/require"
)

const formatTestBucket = "formats"

// bucketWithFormat lays down a table bucket holding one namespace, declared as
// the given format. An empty format is a bucket from before the declaration
// existed.
func bucketWithFormat(t *testing.T, format string) (*s3tablestest.MemFiler, *Manager) {
	t.Helper()
	fs := s3tablestest.Start(t)
	m := NewManager()

	bucketMeta, _ := json.Marshal(tableBucketMetadata{
		Name:           formatTestBucket,
		OwnerAccountID: DefaultAccountID,
		Format:         format,
	})
	fs.Put(TablesPath, formatTestBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
	})

	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(formatTestBucket), "ns", map[string][]byte{ExtendedKeyMetadata: nsMeta})

	return fs, m
}

func createTableOfFormat(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, name, format string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "CreateTable", &CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:::bucket/" + formatTestBucket,
		Namespace:      []string{"ns"},
		Name:           name,
		Format:         format,
	}, nil, "")
}

// The declaration is the point: a bucket that says it holds Lance cannot be
// handed an Iceberg table, because the catalog serving it would never show one.
func TestCreateTableRefusesAForeignFormat(t *testing.T) {
	fs, m := bucketWithFormat(t, FormatLance)

	require.NoError(t, createTableOfFormat(t, m, fs, "vectors", FormatLance))

	err := createTableOfFormat(t, m, fs, "events", FormatIceberg)
	require.Error(t, err, "an Iceberg table in a Lance bucket should be refused")
	require.Contains(t, err.Error(), "holds LANCE")
}

func TestCreateTableRefusesLanceInAnIcebergBucket(t *testing.T) {
	fs, m := bucketWithFormat(t, FormatIceberg)

	err := createTableOfFormat(t, m, fs, "vectors", FormatLance)
	require.Error(t, err)
	require.Contains(t, err.Error(), "holds ICEBERG")
}

// A bucket made before formats were declared keeps taking anything. Nothing is
// migrated, so nothing that worked stops working.
func TestUndeclaredBucketAcceptsEitherFormat(t *testing.T) {
	fs, m := bucketWithFormat(t, "")

	require.NoError(t, createTableOfFormat(t, m, fs, "events", FormatIceberg))
	require.NoError(t, createTableOfFormat(t, m, fs, "vectors", FormatLance))
}

// A view is Iceberg metadata, so it has no meaning in a bucket of another format.
func TestCreateViewRefusedInALanceBucket(t *testing.T) {
	fs, m := bucketWithFormat(t, FormatLance)

	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "CreateView", &CreateViewRequest{
		TableBucketARN: "arn:aws:s3tables:::bucket/" + formatTestBucket,
		Namespace:      []string{"ns"},
		Name:           "v",
	}, nil, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot hold views")
}

func TestNormalizeFormat(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"ICEBERG", FormatIceberg, true},
		{"iceberg", FormatIceberg, true},
		{" Lance ", FormatLance, true},
		{"delta", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := NormalizeFormat(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("NormalizeFormat(%q) = %q,%v; want %q,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}
