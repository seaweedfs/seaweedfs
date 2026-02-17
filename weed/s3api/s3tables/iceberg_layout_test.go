package s3tables

import (
	"testing"
)

func TestIcebergLayoutValidator_ValidateFilePath(t *testing.T) {
	v := NewIcebergLayoutValidator()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Valid metadata files
		{"valid metadata v1", "metadata/v1.metadata.json", false},
		{"valid metadata v123", "metadata/v123.metadata.json", false},
		{"valid snapshot manifest", "metadata/snap-123-1-abc12345-1234-5678-9abc-def012345678.avro", false},
		{"valid manifest file", "metadata/abc12345-1234-5678-9abc-def012345678-m0.avro", false},
		{"valid general manifest", "metadata/abc12345-1234-5678-9abc-def012345678.avro", false},
		{"valid version hint", "metadata/version-hint.text", false},
		{"valid uuid metadata", "metadata/abc12345-1234-5678-9abc-def012345678.metadata.json", false},
		{"valid trino stats", "metadata/20260208_212535_00007_bn4hb-d3599c32-1709-4b94-b6b2-1957b6d6db04.stats", false},

		// Valid data files
		{"valid parquet file", "data/file.parquet", false},
		{"valid orc file", "data/file.orc", false},
		{"valid avro data file", "data/file.avro", false},
		{"valid parquet with path", "data/00000-0-abc12345.parquet", false},

		// Valid partitioned data
		{"valid partitioned parquet", "data/year=2024/file.parquet", false},
		{"valid multi-partition", "data/year=2024/month=01/file.parquet", false},
		{"valid bucket subdirectory", "data/bucket0/file.parquet", false},

		// Directories only
		{"metadata directory bare", "metadata", true},
		{"data directory bare", "data", true},
		{"metadata directory with slash", "metadata/", false},
		{"data directory with slash", "data/", false},

		// Invalid paths
		{"empty path", "", true},
		{"invalid top dir", "invalid/file.parquet", true},
		{"root file", "file.parquet", true},
		{"invalid metadata file", "metadata/random.txt", true},
		{"nested metadata directory", "metadata/nested/v1.metadata.json", true},
		{"nested metadata directory no file", "metadata/nested/", true},
		{"metadata subdir no slash", "metadata/nested", true},
		{"invalid data file", "data/file.csv", true},
		{"invalid data file json", "data/file.json", true},

		// Partition/subdirectory without trailing slashes
		{"partition directory no slash", "data/year=2024", false},
		{"data subdirectory no slash", "data/my_subdir", false},
		{"multi-level partition", "data/event_date=2025-01-01/hour=00/file.parquet", false},
		{"multi-level partition directory", "data/event_date=2025-01-01/hour=00/", false},
		{"multi-level partition directory no slash", "data/event_date=2025-01-01/hour=00", false},

		// Double slashes
		{"data double slash", "data//file.parquet", true},
		{"data redundant slash", "data/year=2024//file.parquet", true},
		{"metadata redundant slash", "metadata//v1.metadata.json", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.ValidateFilePath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestIcebergLayoutValidator_PartitionPaths(t *testing.T) {
	v := NewIcebergLayoutValidator()

	validPaths := []string{
		"data/year=2024/file.parquet",
		"data/date=2024-01-15/file.parquet",
		"data/category=electronics/file.parquet",
		"data/user_id=12345/file.parquet",
		"data/region=us-east-1/file.parquet",
		"data/year=2024/month=01/day=15/file.parquet",
	}

	for _, path := range validPaths {
		if err := v.ValidateFilePath(path); err != nil {
			t.Errorf("ValidateFilePath(%q) should be valid, got error: %v", path, err)
		}
	}
}

func TestTableBucketFileValidator_ValidateTableBucketUpload(t *testing.T) {
	v := NewTableBucketFileValidator()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Non-table bucket paths should pass (no validation)
		{"regular bucket path", "/buckets/mybucket/file.txt", false},
		{"filer path", "/home/user/file.txt", false},

		// Table bucket structure paths (creating directories)
		{"table bucket root", "/buckets/mybucket", false},
		{"namespace dir", "/buckets/mybucket/myns", false},
		{"table dir", "/buckets/mybucket/myns/mytable", false},
		{"table dir trailing slash", "/buckets/mybucket/myns/mytable/", false},

		// Valid table bucket file uploads
		{"valid parquet upload", "/buckets/mybucket/myns/mytable/data/file.parquet", false},
		{"valid metadata upload", "/buckets/mybucket/myns/mytable/metadata/v1.metadata.json", false},
		{"valid trino stats upload", "/buckets/mybucket/myns/mytable/metadata/20260208_212535_00007_bn4hb-d3599c32-1709-4b94-b6b2-1957b6d6db04.stats", false},
		{"valid partitioned data", "/buckets/mybucket/myns/mytable/data/year=2024/file.parquet", false},

		// Invalid table bucket file uploads
		{"invalid file type", "/buckets/mybucket/myns/mytable/data/file.csv", true},
		{"invalid top-level dir", "/buckets/mybucket/myns/mytable/invalid/file.parquet", true},
		{"root file in table", "/buckets/mybucket/myns/mytable/file.parquet", true},

		// Empty segment cases
		{"empty bucket", "/buckets//myns/mytable/data/file.parquet", true},
		{"empty namespace", "/buckets/mybucket//mytable/data/file.parquet", true},
		{"empty table", "/buckets/mybucket/myns//data/file.parquet", true},
		{"empty bucket dir", "/buckets//", true},
		{"empty namespace dir", "/buckets/mybucket//", true},
		{"table double slash bypass", "/buckets/mybucket/myns/mytable//data/file.parquet", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.ValidateTableBucketUpload(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableBucketUpload(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestIsTableBucketPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/buckets/mybucket", true},
		{"/buckets/mybucket/ns/table/data/file.parquet", true},
		{"/home/user/file.txt", false},
		{"buckets/mybucket", false}, // missing leading slash
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := IsTableBucketPath(tt.path); got != tt.want {
				t.Errorf("IsTableBucketPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestGetTableInfoFromPath(t *testing.T) {
	tests := []struct {
		path          string
		wantBucket    string
		wantNamespace string
		wantTable     string
	}{
		{"/buckets/mybucket/myns/mytable/data/file.parquet", "mybucket", "myns", "mytable"},
		{"/buckets/mybucket/myns/mytable", "mybucket", "myns", "mytable"},
		{"/buckets/mybucket/myns", "mybucket", "myns", ""},
		{"/buckets/mybucket", "mybucket", "", ""},
		{"/home/user/file.txt", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			bucket, namespace, table := GetTableInfoFromPath(tt.path)
			if bucket != tt.wantBucket || namespace != tt.wantNamespace || table != tt.wantTable {
				t.Errorf("GetTableInfoFromPath(%q) = (%q, %q, %q), want (%q, %q, %q)",
					tt.path, bucket, namespace, table, tt.wantBucket, tt.wantNamespace, tt.wantTable)
			}
		})
	}
}
