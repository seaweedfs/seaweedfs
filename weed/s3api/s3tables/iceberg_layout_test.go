package s3tables

import "testing"

// TestIcebergLayoutValidator_AcceptsRealWorldManifestNames pins down the
// filename patterns we must accept across Iceberg engines. The original strict
// regex only covered Iceberg-internal naming (`{uuid}-m{n}.avro`,
// `snap-{n}-{n}-{uuid}.avro`) and rejected real manifests written by Flink and
// other writers, causing 403s during INSERT commits. The catch-all entries
// added alongside this test must keep these names valid.
func TestIcebergLayoutValidator_AcceptsRealWorldManifestNames(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{
			"flink-style manifest (job-id + checkpoint + operator-id + counter)",
			"metadata/02678a59b3d6b460ba392851d77155fc-1-cbc357ccb763df2852fee8c4fc7d55f2-00001.avro",
		},
		{
			"spark-style manifest with two uuids and -m suffix",
			"metadata/00000000-0000-0000-0000-000000000000-m0.avro",
		},
		{
			"snapshot manifest list with iceberg-internal naming",
			"metadata/snap-7234891234567890123-1-82e3eec4-3aee-414f-a444-94c03c641d20.avro",
		},
		{
			"versioned table metadata",
			"metadata/v1.metadata.json",
		},
		{
			"uuid-named metadata json (newer iceberg)",
			"metadata/82e3eec4-3aee-414f-a444-94c03c641d20.metadata.json",
		},
		{
			"flink-style metadata json with dashes and digits",
			"metadata/02678a59-1-cbc357cc.metadata.json",
		},
		{
			"version hint",
			"metadata/version-hint.text",
		},
		{
			"trino/iceberg stats file",
			"metadata/table.stats",
		},
	}

	v := NewIcebergLayoutValidator()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := v.ValidateFilePath(tc.path); err != nil {
				t.Errorf("expected %q to be accepted, got error: %v", tc.path, err)
			}
		})
	}
}

// TestIcebergLayoutValidator_RejectsClearlyBadMetadataNames guards the
// catch-all from being too permissive — paths that look like attempts to
// escape the metadata layout or have forbidden file types must still fail.
func TestIcebergLayoutValidator_RejectsClearlyBadMetadataNames(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{"random extension", "metadata/random-file.txt"},
		{"executable masquerading as avro path", "metadata/evil.sh"},
		{"subdirectory under metadata is not allowed", "metadata/sub/file.avro"},
		{"top-level dir other than metadata or data", "garbage/file.avro"},
	}

	v := NewIcebergLayoutValidator()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := v.ValidateFilePath(tc.path); err == nil {
				t.Errorf("expected %q to be rejected, but it passed validation", tc.path)
			}
		})
	}
}

// TestIcebergLayoutValidator_AcceptsRealWorldDataFiles is a sanity check that
// the patterns most engines actually emit for data files still pass.
func TestIcebergLayoutValidator_AcceptsRealWorldDataFiles(t *testing.T) {
	cases := []string{
		"data/00000-0-ede83b82-08e1-40cd-af8a-6d83680a5194-00001.parquet",
		"data/part-00000.parquet",
		"data/some-file.orc",
		"data/year=2026/month=05/00000-0-uuid.parquet",
	}
	v := NewIcebergLayoutValidator()
	for _, p := range cases {
		t.Run(p, func(t *testing.T) {
			if err := v.ValidateFilePath(p); err != nil {
				t.Errorf("expected %q to be accepted, got error: %v", p, err)
			}
		})
	}
}
