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
		{
			"puffin statistics file",
			"metadata/20260928-1-0199a3c2-7d1e-7c55-9a0e-3b1f2c4d5e6f.puffin",
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
		{"catch-all anchors must reject trailing extension", "metadata/file.avro.txt"},
		{"puffin with a trailing extension", "metadata/stats.puffin.txt"},
		{"puffin in a subdirectory under metadata", "metadata/sub/stats.puffin"},
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
		"data/00000-2-c56b1e21-828c-40ad-8f7b-739c6cb82dcf-00001-deletes.puffin",
		"data/82e3eec4-3aee-414f-a444-94c03c641d20-deletes.puffin",
		"data/year=2026/month=05/00000-2-c56b1e21-828c-40ad-8f7b-739c6cb82dcf-00001-deletes.puffin",
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

// A Lance dataset writes fragments into data/ and keeps its own bookkeeping in
// underscore-prefixed directories. The validator runs on the S3 door, where the
// table's format is not in hand, so it has to admit both formats' layouts.
func TestValidateFilePathAcceptsLanceLayout(t *testing.T) {
	v := NewIcebergLayoutValidator()
	allowed := []string{
		"data/01111110101110001101011164cadc43919eace9c608107bd9.lance",
		"_versions/1.manifest",
		"_versions/9223372036854775806.manifest",
		"_transactions/0-ddb27ab7-2e5c-42c7-b4bf-265d8a3ff636.txn",
		"_indices/85814508-ed9a-41f2-b939-2050bb7a0ed5-fts/index.idx",
		"_deletions/_deletions-1.arrow",
		".lance-reserved",
		".lance-deregistered",
	}
	for _, path := range allowed {
		if err := v.ValidateFilePath(path); err != nil {
			t.Errorf("ValidateFilePath(%q) = %v, want nil", path, err)
		}
	}

	rejected := []string{
		"_versions/../../escape",
		"_versions//empty",
		"notadir/file.lance",
		"random.txt",
	}
	for _, path := range rejected {
		if err := v.ValidateFilePath(path); err == nil {
			t.Errorf("ValidateFilePath(%q) = nil, want an error", path)
		}
	}
}

// Statistics files are referenced from the table metadata and kept by orphan
// cleanup; the upload door has to admit them in the table's metadata/, and
// only there.
func TestTableBucketFileValidator_PuffinStatisticsOnlyInTableMetadata(t *testing.T) {
	v := NewTableBucketFileValidator()
	accepted := []string{
		TablesPath + "/cat/ns/t/metadata/stats-1.puffin",
		TablesPath + "/cat/ns/t/metadata/table.stats",
		TablesPath + "/cat/ns/t/data/0199a3c2-deletes.puffin",
	}
	for _, p := range accepted {
		if err := v.ValidateTableBucketUpload(p); err != nil {
			t.Errorf("ValidateTableBucketUpload(%q) = %v, want nil", p, err)
		}
	}
	rejected := []string{
		TablesPath + "/cat/ns/t/stats-1.puffin",
		TablesPath + "/cat/ns/t/other/stats-1.puffin",
		TablesPath + "/cat/ns/t/metadata/sub/stats-1.puffin",
		TablesPath + "/cat/ns/t/metadata/notes.txt",
	}
	for _, p := range rejected {
		if err := v.ValidateTableBucketUpload(p); err == nil {
			t.Errorf("ValidateTableBucketUpload(%q) = nil, want an error", p)
		}
	}
}
