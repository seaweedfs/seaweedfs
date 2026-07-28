package s3tables

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// writeManifestList builds a manifest list holding one data and one delete
// manifest, the shape DuckDB produces for a v2 table that has been updated.
func writeManifestList(t *testing.T, version int) []byte {
	t.Helper()

	dataManifest := iceberg.NewManifestFile(version, "s3://bucket/ns/tbl/metadata/data-m0.avro", 1024, 0, 7).
		SequenceNum(3, 3).
		Content(iceberg.ManifestContentData).
		AddedFiles(1).
		AddedRows(10).
		Build()
	files := []iceberg.ManifestFile{dataManifest}
	if version > 1 {
		// Delete manifests only exist from v2 onwards.
		files = append(files, iceberg.NewManifestFile(version, "s3://bucket/ns/tbl/metadata/deletes-m0.avro", 512, 0, 7).
			SequenceNum(3, 3).
			Content(iceberg.ManifestContentDeletes).
			AddedFiles(1).
			AddedRows(1).
			Build())
	}

	var buf bytes.Buffer
	seqNum := int64(3)
	if err := iceberg.WriteManifestList(version, &buf, 7, nil, &seqNum, 0, files); err != nil {
		t.Fatalf("write v%d manifest list: %v", version, err)
	}
	return buf.Bytes()
}

// stripFormatVersion rewrites an Avro header without its "format-version"
// entry, reproducing what DuckDB writes: a manifest list carrying no Iceberg
// header metadata at all.
func stripFormatVersion(t *testing.T, data []byte) []byte {
	t.Helper()

	metadata, terminator, err := readAvroFileMetadata(data)
	if err != nil {
		t.Fatalf("read avro header: %v", err)
	}
	if _, ok := metadata[formatVersionKey]; !ok {
		t.Fatal("fixture has no format-version to strip")
	}

	var entries []byte
	count := 0
	for key, value := range metadata {
		if key == formatVersionKey {
			continue
		}
		entries = appendAvroBytes(appendAvroBytes(entries, []byte(key)), value)
		count++
	}

	stripped := append([]byte{}, avroFileMagic...)
	stripped = appendAvroLong(stripped, int64(count))
	stripped = append(stripped, entries...)
	// Everything from the closing zero count on — sync marker and data
	// blocks included — is unaffected by the header rewrite.
	return append(stripped, data[terminator:]...)
}

// DuckDB writes manifest lists with no Avro header metadata, so iceberg-go
// falls back to v1 and every v2 manifest below it fails to parse.
func TestReadManifestListWithoutFormatVersion(t *testing.T) {
	stripped := stripFormatVersion(t, writeManifestList(t, 2))

	// Baseline: iceberg-go alone reads the list as v1, which both mislabels
	// the delete manifest and makes ReadManifest reject the v2 manifests.
	unpatched, err := iceberg.ReadManifestList(bytes.NewReader(stripped))
	if err != nil {
		t.Fatalf("iceberg.ReadManifestList: %v", err)
	}
	if got := unpatched[0].Version(); got != 1 {
		t.Fatalf("expected iceberg-go to default to v1, got v%d", got)
	}

	manifests, err := ReadManifestList(stripped)
	if err != nil {
		t.Fatalf("ReadManifestList: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(manifests))
	}
	for _, mf := range manifests {
		if mf.Version() != 2 {
			t.Errorf("manifest %s: version = %d, want 2", mf.FilePath(), mf.Version())
		}
		if mf.SequenceNum() != 3 || mf.MinSequenceNum() != 3 {
			t.Errorf("manifest %s: sequence numbers = %d/%d, want 3/3", mf.FilePath(), mf.SequenceNum(), mf.MinSequenceNum())
		}
	}
	if got := manifests[0].ManifestContent(); got != iceberg.ManifestContentData {
		t.Errorf("first manifest content = %s, want data", got)
	}
	if got := manifests[1].ManifestContent(); got != iceberg.ManifestContentDeletes {
		t.Errorf("second manifest content = %s, want deletes", got)
	}
}

func TestReadManifestListKeepsWrittenFormatVersion(t *testing.T) {
	for _, version := range []int{1, 2} {
		manifests, err := ReadManifestList(writeManifestList(t, version))
		if err != nil {
			t.Fatalf("v%d: ReadManifestList: %v", version, err)
		}
		if got := manifests[0].Version(); got != version {
			t.Errorf("v%d: version = %d", version, got)
		}
	}
}

// A v1 manifest list without the header entry must stay v1: its record schema
// has none of the fields v2 added.
func TestReadManifestListWithoutFormatVersionStaysV1(t *testing.T) {
	manifests, err := ReadManifestList(stripFormatVersion(t, writeManifestList(t, 1)))
	if err != nil {
		t.Fatalf("ReadManifestList: %v", err)
	}
	if got := manifests[0].Version(); got != 1 {
		t.Errorf("version = %d, want 1", got)
	}
}

// Unparseable input is handed to iceberg-go untouched so it keeps reporting
// the underlying problem rather than a header-rewriting one.
func TestReadManifestListRejectsNonAvro(t *testing.T) {
	for name, data := range map[string][]byte{
		"empty":           nil,
		"not avro":        []byte("this is not an avro file"),
		"truncated magic": avroFileMagic[:3],
		"truncated header": func() []byte {
			list := writeManifestList(t, 2)
			return list[:len(avroFileMagic)+2]
		}(),
	} {
		if _, err := ReadManifestList(data); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestManifestListFormatVersion(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   int
	}{
		{"v1 fields", `{"fields":[{"name":"manifest_path"},{"name":"added_snapshot_id"}]}`, 1},
		{"v2 adds content", `{"fields":[{"name":"manifest_path"},{"name":"content"}]}`, 2},
		{"v2 adds sequence numbers", `{"fields":[{"name":"sequence_number"},{"name":"min_sequence_number"}]}`, 2},
		{"v3 adds first_row_id", `{"fields":[{"name":"content"},{"name":"first_row_id"}]}`, 3},
		{"v3 field ordering", `{"fields":[{"name":"first_row_id"},{"name":"content"}]}`, 3},
		{"unparseable schema", `not json`, 1},
		{"missing schema", ``, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := manifestListFormatVersion([]byte(tc.schema)); got != tc.want {
				t.Errorf("manifestListFormatVersion() = %d, want %d", got, tc.want)
			}
		})
	}
}
