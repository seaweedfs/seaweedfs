package iceberg

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"

	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Files of one Iceberg table may spell its schema differently in Parquet:
// a column added by schema evolution, or the same schema written REQUIRED
// by one engine and OPTIONAL by another (PyIceberg writes REQUIRED, DuckDB
// OPTIONAL). The bin-pack merge joins such files by Iceberg field id,
// reading a column a file lacks as null, and refuses files whose same-name
// columns carry different field ids (a column dropped and re-added).

type narrowSchemaRow struct { // id REQUIRED id=1, name OPTIONAL id=2
	ID   int64  `parquet:"id,id(1)"`
	Name string `parquet:"name,id(2),optional"`
}

type wideSchemaRow struct { // plus extra OPTIONAL id=3
	ID    int64  `parquet:"id,id(1)"`
	Name  string `parquet:"name,id(2),optional"`
	Extra string `parquet:"extra,id(3),optional"`
}

type reorderedSchemaRow struct { // the wide schema's columns in another order
	Extra string `parquet:"extra,id(3),optional"`
	ID    int64  `parquet:"id,id(1)"`
	Name  string `parquet:"name,id(2),optional"`
}

type optionalIDSchemaRow struct { // id OPTIONAL id=1
	ID   int64  `parquet:"id,id(1),optional"`
	Name string `parquet:"name,id(2),optional"`
}

type readdedNameSchemaRow struct { // name re-added under field id 3
	ID   int64  `parquet:"id,id(1)"`
	Name string `parquet:"name,id(3),optional"`
}

type nullableExtraSchemaRow struct { // the wide schema, extra writable as null
	ID    int64   `parquet:"id,id(1)"`
	Name  string  `parquet:"name,id(2),optional"`
	Extra *string `parquet:"extra,id(3),optional"`
}

func newAddedColumnSchema() *iceberg.Schema { // newTestSchema plus extra id=3
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "name", Required: false},
		iceberg.NestedField{ID: 3, Type: iceberg.PrimitiveTypes.String, Name: "extra", Required: false},
	)
}

func schemaFileEntry(t *testing.T, path string, recordCount int64) iceberg.ManifestEntry {
	t.Helper()
	dfb, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, map[int]any{}, nil, nil, recordCount, 1024)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	snapID := int64(1)
	return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build())
}

func mergeNamedFiles(t *testing.T, client filer_pb.SeaweedFilerClient, names []string, counts map[string]int64,
	eqGroups []equalityDeleteGroup, schema *iceberg.Schema, lineage bool) ([]byte, int64, error) {
	t.Helper()
	var entries []iceberg.ManifestEntry
	for _, name := range names {
		entries = append(entries, schemaFileEntry(t, "data/"+name, counts[name]))
	}
	return mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl",
		entries, nil, eqGroups, schema, 0, lineage)
}

// mergedValues returns the merged file's schema and each row's leaf values
// in column order (nil for nulls).
func mergedValues(t *testing.T, data []byte) (*parquet.Schema, [][]any) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open merged: %v", err)
	}
	var out [][]any
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 16)
		for {
			n, err := rows.ReadRows(buf)
			for _, r := range buf[:n] {
				vals := make([]any, len(r))
				for i, v := range r {
					if v.IsNull() {
						continue
					}
					if v.Kind() == parquet.Int64 {
						vals[i] = v.Int64()
					} else {
						vals[i] = v.String()
					}
				}
				out = append(out, vals)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("read merged rows: %v", err)
			}
		}
		rows.Close()
	}
	return f.Schema(), out
}

func checkMergedRows(t *testing.T, data []byte, want [][]any) *parquet.Schema {
	t.Helper()
	schema, got := mergedValues(t, data)
	if len(got) != len(want) {
		t.Fatalf("merged rows = %v, want %v", got, want)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d = %v, want %v", i, got[i], want[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %v, want %v (row %v)", i, j, got[i][j], want[i][j], got[i])
			}
		}
	}
	return schema
}

// A column added to the table after some files were written: the merge
// joins the files by field id and reads the new column as null for the
// old file's rows, whichever file leads the bin.
func TestMergeParquetFilesAddedColumn(t *testing.T) {
	put := func(fs *fakeFilerServer) map[string]int64 {
		putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}, {2, "b"}, {3, "c"}})
		putDataFileRows(t, fs, "wide.parquet", []wideSchemaRow{{4, "d", "e4"}, {5, "e", "e5"}})
		return map[string]int64{"narrow.parquet": 3, "wide.parquet": 2}
	}
	checkFields := func(t *testing.T, schema *parquet.Schema) {
		fields := schema.Fields()
		if len(fields) != 3 {
			t.Fatalf("merged fields = %d, want 3", len(fields))
		}
		names := []string{fields[0].Name(), fields[1].Name(), fields[2].Name()}
		if names[0] != "id" || names[1] != "name" || names[2] != "extra" {
			t.Fatalf("merged fields = %v, want [id name extra]", names)
		}
		if fields[2].ID() != 3 {
			t.Fatalf("extra field id = %d, want 3", fields[2].ID())
		}
	}

	t.Run("narrow first", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		counts := put(fs)
		merged, count, err := mergeNamedFiles(t, client,
			[]string{"narrow.parquet", "wide.parquet"}, counts, nil, nil, false)
		if err != nil {
			t.Fatalf("mergeParquetFiles: %v", err)
		}
		if count != 5 {
			t.Fatalf("merged %d rows, want 5", count)
		}
		checkFields(t, checkMergedRows(t, merged, [][]any{
			{int64(1), "a", nil},
			{int64(2), "b", nil},
			{int64(3), "c", nil},
			{int64(4), "d", "e4"},
			{int64(5), "e", "e5"},
		}))
	})

	t.Run("wide first", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		counts := put(fs)
		merged, count, err := mergeNamedFiles(t, client,
			[]string{"wide.parquet", "narrow.parquet"}, counts, nil, nil, false)
		if err != nil {
			t.Fatalf("mergeParquetFiles: %v", err)
		}
		if count != 5 {
			t.Fatalf("merged %d rows, want 5", count)
		}
		checkFields(t, checkMergedRows(t, merged, [][]any{
			{int64(4), "d", "e4"},
			{int64(5), "e", "e5"},
			{int64(1), "a", nil},
			{int64(2), "b", nil},
			{int64(3), "c", nil},
		}))
	})
}

// The same Iceberg schema written REQUIRED by one engine and OPTIONAL by
// another merges; the writer keeps the wider, optional, repetition.
func TestMergeParquetFilesRequiredVsOptional(t *testing.T) {
	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "required.parquet", []narrowSchemaRow{{1, "a"}, {2, "b"}})
	putDataFileRows(t, fs, "optional.parquet", []optionalIDSchemaRow{{3, "c"}, {4, "d"}})

	merged, count, err := mergeNamedFiles(t, client,
		[]string{"required.parquet", "optional.parquet"},
		map[string]int64{"required.parquet": 2, "optional.parquet": 2}, nil, nil, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 4 {
		t.Fatalf("merged %d rows, want 4", count)
	}
	schema := checkMergedRows(t, merged, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
		{int64(4), "d"},
	})
	if !schema.Fields()[0].Optional() {
		t.Error("merged id column is REQUIRED, want OPTIONAL")
	}
}

// A column dropped and re-added under the same name and type has a new
// field id; merging its file with an old one would file the new column's
// values under the dropped field id, so the bin is refused.
func TestMergeParquetFilesReaddedColumn(t *testing.T) {
	for _, names := range [][]string{
		{"narrow.parquet", "re-added.parquet"},
		{"re-added.parquet", "narrow.parquet"},
	} {
		fs, client := startFakeFiler(t)
		putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}})
		putDataFileRows(t, fs, "re-added.parquet", []readdedNameSchemaRow{{2, "new-2"}})

		_, _, err := mergeNamedFiles(t, client, names,
			map[string]int64{"narrow.parquet": 1, "re-added.parquet": 1}, nil, nil, false)
		if err == nil || !strings.Contains(err.Error(), "cannot merge files with different schemas") {
			t.Errorf("merge of %v: %v, want schema mismatch", names, err)
		}
	}
}

type disjointColumnsRow struct { // id id=1, extra id=3: no name
	ID    int64  `parquet:"id,id(1)"`
	Extra string `parquet:"extra,id(3),optional"`
}

type requiredExtraSchemaRow struct { // extra REQUIRED id=3
	ID    int64  `parquet:"id,id(1)"`
	Name  string `parquet:"name,id(2),optional"`
	Extra string `parquet:"extra,id(3)"`
}

// Differences field-id matching does not cover stay refused: a rename, a
// type change, files without field ids; and a control pair still compacts.
func TestMergeParquetFilesSchemaRefusals(t *testing.T) {
	type renamedSchemaRow struct {
		ID    int64  `parquet:"id,id(1)"`
		Other string `parquet:"other,id(2),optional"`
	}
	type widenedSchemaRow struct {
		ID   int32  `parquet:"id,id(1)"`
		Name string `parquet:"name,id(2),optional"`
	}
	type noIDExtraRow struct {
		ID    int64  `parquet:"id"`
		Name  string `parquet:"name,optional"`
		Extra string `parquet:"extra,optional"`
	}

	cases := []struct {
		name      string
		writeFile func(fs *fakeFilerServer, name string)
	}{
		{"rename", func(fs *fakeFilerServer, name string) {
			putDataFileRows(t, fs, name, []renamedSchemaRow{{5, "e"}})
		}},
		{"type change", func(fs *fakeFilerServer, name string) {
			putDataFileRows(t, fs, name, []widenedSchemaRow{{5, "e"}})
		}},
		{"no field ids", func(fs *fakeFilerServer, name string) {
			putDataFileRows(t, fs, name, []noIDExtraRow{{5, "e", "x"}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, client := startFakeFiler(t)
			putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}})
			tc.writeFile(fs, "other.parquet")

			_, _, err := mergeNamedFiles(t, client,
				[]string{"narrow.parquet", "other.parquet"},
				map[string]int64{"narrow.parquet": 1, "other.parquet": 1}, nil, nil, false)
			if err == nil || !strings.Contains(err.Error(), "cannot merge files with different schemas") {
				t.Errorf("merge: %v, want schema mismatch", err)
			}
		})
	}

	t.Run("control", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		putDataFileRows(t, fs, "a.parquet", []narrowSchemaRow{{1, "a"}})
		putDataFileRows(t, fs, "b.parquet", []narrowSchemaRow{{2, "b"}})
		if _, _, err := mergeNamedFiles(t, client, []string{"a.parquet", "b.parquet"},
			map[string]int64{"a.parquet": 1, "b.parquet": 1}, nil, nil, false); err != nil {
			t.Errorf("control merge: %v", err)
		}
	})
}

// An equality-delete column can sit at a different position in an older
// file: it is resolved on each file's own schema.
func TestMergeParquetFilesEvolvedSchemaEqualityDelete(t *testing.T) {
	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}, {2, "b"}, {3, "c"}})
	putDataFileRows(t, fs, "reordered.parquet", []reorderedSchemaRow{
		{"e4", 4, "d"}, {"e5", 5, "e"},
	})

	// An equality delete on name (field id 2) for "b" (index 1 in the
	// narrow file) and "e" (index 2 in the reordered file).
	type eqDeleteRow struct {
		Name string `parquet:"name"`
	}
	var eqBuf bytes.Buffer
	eqWriter := parquet.NewWriter(&eqBuf, parquet.SchemaOf(new(eqDeleteRow)))
	for _, r := range []eqDeleteRow{{"b"}, {"e"}} {
		if err := eqWriter.Write(&r); err != nil {
			t.Fatalf("write eq delete: %v", err)
		}
	}
	if err := eqWriter.Close(); err != nil {
		t.Fatalf("close eq writer: %v", err)
	}
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", "eq-del.parquet", &filer_pb.Entry{
		Name: "eq-del.parquet", Content: eqBuf.Bytes(),
	})

	eqDfb, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"data/eq-del.parquet", iceberg.ParquetFile, map[int]any{}, nil, nil, 2, int64(eqBuf.Len()))
	if err != nil {
		t.Fatalf("build eq delete: %v", err)
	}
	eqDfb.EqualityFieldIDs([]int{2})
	snapID := int64(1)
	eqEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, eqDfb.Build())

	eqGroups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl",
		[]iceberg.ManifestEntry{eqEntry}, newTestSchema())
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}

	merged, count, err := mergeNamedFiles(t, client,
		[]string{"narrow.parquet", "reordered.parquet"},
		map[string]int64{"narrow.parquet": 3, "reordered.parquet": 2}, eqGroups, newTestSchema(), false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 3 {
		t.Fatalf("merged %d rows, want 3", count)
	}
	// The union schema (id, name, extra) is the writer's.
	checkMergedRows(t, merged, [][]any{
		{int64(1), "a", nil},
		{int64(3), "c", nil},
		{int64(4), "d", "e4"},
	})
}

// File pairs where neither schema covers the other merge under the union
// writer schema: a REQUIRED/OPTIONAL difference plus an added column, a
// column each side lacks the other's of, and an added REQUIRED column
// (demoted to OPTIONAL: the files without it read null).
func TestMergeParquetFilesUnionWriterSchema(t *testing.T) {
	t.Run("added column and repetition differ", func(t *testing.T) {
		for _, names := range [][]string{
			{"wide.parquet", "optional.parquet"},
			{"optional.parquet", "wide.parquet"},
		} {
			fs, client := startFakeFiler(t)
			putDataFileRows(t, fs, "wide.parquet", []wideSchemaRow{{4, "d", "e4"}, {5, "e", "e5"}})
			putDataFileRows(t, fs, "optional.parquet", []optionalIDSchemaRow{{1, "a"}, {2, "b"}})

			merged, count, err := mergeNamedFiles(t, client, names,
				map[string]int64{"wide.parquet": 2, "optional.parquet": 2}, nil, nil, false)
			if err != nil {
				t.Fatalf("merge of %v: %v", names, err)
			}
			if count != 4 {
				t.Fatalf("merged %d rows, want 4", count)
			}
			var want [][]any
			if names[0] == "wide.parquet" {
				want = [][]any{{int64(4), "d", "e4"}, {int64(5), "e", "e5"}, {int64(1), "a", nil}, {int64(2), "b", nil}}
			} else {
				want = [][]any{{int64(1), "a", nil}, {int64(2), "b", nil}, {int64(4), "d", "e4"}, {int64(5), "e", "e5"}}
			}
			schema := checkMergedRows(t, merged, want)
			if !schema.Fields()[0].Optional() {
				t.Errorf("merged id column is REQUIRED, want OPTIONAL")
			}
		}
	})

	t.Run("disjoint columns", func(t *testing.T) {
		for _, names := range [][]string{
			{"narrow.parquet", "disjoint.parquet"},
			{"disjoint.parquet", "narrow.parquet"},
		} {
			fs, client := startFakeFiler(t)
			putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}})
			putDataFileRows(t, fs, "disjoint.parquet", []disjointColumnsRow{{5, "e"}})

			merged, count, err := mergeNamedFiles(t, client, names,
				map[string]int64{"narrow.parquet": 1, "disjoint.parquet": 1}, nil, nil, false)
			if err != nil {
				t.Fatalf("merge of %v: %v", names, err)
			}
			if count != 2 {
				t.Fatalf("merged %d rows, want 2", count)
			}
			var want [][]any
			if names[0] == "narrow.parquet" { // union (id, name, extra)
				want = [][]any{{int64(1), "a", nil}, {int64(5), nil, "e"}}
			} else { // union (id, extra, name)
				want = [][]any{{int64(5), "e", nil}, {int64(1), nil, "a"}}
			}
			checkMergedRows(t, merged, want)
		}
	})

	t.Run("added required column", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}})
		putDataFileRows(t, fs, "required-extra.parquet", []requiredExtraSchemaRow{{5, "e", "x"}})

		merged, count, err := mergeNamedFiles(t, client,
			[]string{"narrow.parquet", "required-extra.parquet"},
			map[string]int64{"narrow.parquet": 1, "required-extra.parquet": 1}, nil, nil, false)
		if err != nil {
			t.Fatalf("mergeParquetFiles: %v", err)
		}
		if count != 2 {
			t.Fatalf("merged %d rows, want 2", count)
		}
		schema := checkMergedRows(t, merged, [][]any{
			{int64(1), "a", nil},
			{int64(5), "e", "x"},
		})
		if !schema.Fields()[2].Optional() {
			t.Error("merged extra column is REQUIRED, want OPTIONAL")
		}
	})
}

// An equality delete naming a column added after some files were written:
// the files that lack it read the delete key's column as null, so a real
// key does not match their rows and a null key deletes them all.
func TestMergeParquetFilesEqualityDeleteAddedColumn(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	put := func(fs *fakeFilerServer) map[string]int64 {
		putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}, {2, "b"}, {3, "c"}})
		putDataFileRows(t, fs, "wide.parquet", []nullableExtraSchemaRow{{4, "d", strPtr("e4")}, {5, "e", nil}})
		return map[string]int64{"narrow.parquet": 3, "wide.parquet": 2}
	}
	eqGroups := func(keys ...parquet.Value) []equalityDeleteGroup {
		set := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			set[buildEqualityKey(parquet.Row{k}, []int{0})] = struct{}{}
		}
		return []equalityDeleteGroup{{FieldIDs: []int{3}, Keys: set}}
	}
	schema := newAddedColumnSchema()

	t.Run("key matches no null", func(t *testing.T) {
		for _, names := range [][]string{
			{"narrow.parquet", "wide.parquet"},
			{"wide.parquet", "narrow.parquet"},
		} {
			fs, client := startFakeFiler(t)
			counts := put(fs)
			merged, count, err := mergeNamedFiles(t, client, names, counts,
				eqGroups(parquet.ValueOf("e4")), schema, false)
			if err != nil {
				t.Fatalf("merge of %v: %v", names, err)
			}
			if count != 4 {
				t.Fatalf("merged %d rows, want 4", count)
			}
			var want [][]any
			if names[0] == "narrow.parquet" {
				want = [][]any{{int64(1), "a", nil}, {int64(2), "b", nil}, {int64(3), "c", nil}, {int64(5), "e", nil}}
			} else {
				want = [][]any{{int64(5), "e", nil}, {int64(1), "a", nil}, {int64(2), "b", nil}, {int64(3), "c", nil}}
			}
			checkMergedRows(t, merged, want)
		}
	})

	t.Run("null key deletes nulls", func(t *testing.T) {
		for _, names := range [][]string{
			{"narrow.parquet", "wide.parquet"},
			{"wide.parquet", "narrow.parquet"},
		} {
			fs, client := startFakeFiler(t)
			counts := put(fs)
			merged, count, err := mergeNamedFiles(t, client, names, counts,
				eqGroups(parquet.NullValue()), schema, false)
			if err != nil {
				t.Fatalf("merge of %v: %v", names, err)
			}
			if count != 1 {
				t.Fatalf("merged %d rows, want 1", count)
			}
			checkMergedRows(t, merged, [][]any{{int64(4), "d", "e4"}})
		}
	})
}

// A column renamed after its files were written still carries its field id,
// so an equality delete on it resolves the column by id and removes only the
// rows it names — the old name looking absent must not turn every key null.
func TestMergeParquetFilesEqualityDeleteRenamedColumn(t *testing.T) {
	// The table renamed name (field id 2) to label; every file still spells
	// the old name.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "label", Required: false},
	)
	type eqDeleteRow struct {
		Name string `parquet:"name,id(2)"`
	}

	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "a.parquet", []narrowSchemaRow{{1, "a"}, {2, "b"}, {3, "c"}})
	putDataFileRows(t, fs, "b.parquet", []narrowSchemaRow{{4, "d"}})

	var eqBuf bytes.Buffer
	eqWriter := parquet.NewWriter(&eqBuf, parquet.SchemaOf(new(eqDeleteRow)))
	if err := eqWriter.Write(&eqDeleteRow{"b"}); err != nil {
		t.Fatalf("write eq delete: %v", err)
	}
	if err := eqWriter.Close(); err != nil {
		t.Fatalf("close eq writer: %v", err)
	}
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", "eq-del.parquet", &filer_pb.Entry{
		Name: "eq-del.parquet", Content: eqBuf.Bytes(),
	})

	eqDfb, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"data/eq-del.parquet", iceberg.ParquetFile, map[int]any{}, nil, nil, 1, int64(eqBuf.Len()))
	if err != nil {
		t.Fatalf("build eq delete: %v", err)
	}
	eqDfb.EqualityFieldIDs([]int{2})
	snapID := int64(1)
	eqEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, eqDfb.Build())

	eqGroups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl",
		[]iceberg.ManifestEntry{eqEntry}, schema)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}

	merged, count, err := mergeNamedFiles(t, client,
		[]string{"a.parquet", "b.parquet"},
		map[string]int64{"a.parquet": 3, "b.parquet": 1}, eqGroups, schema, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 3 {
		t.Fatalf("merged %d rows, want 3", count)
	}
	checkMergedRows(t, merged, [][]any{
		{int64(1), "a"},
		{int64(3), "c"},
		{int64(4), "d"},
	})
}

// An equality-delete file that lacks its equality column is refused rather
// than read as null keys: the spec requires equality deletes to carry the
// columns they name, so the missing-column fallback is for data files only.
func TestEqualityDeleteFileMissingColumn(t *testing.T) {
	fs, client := startFakeFiler(t)
	type otherRow struct {
		Other string `parquet:"other,id(9)"`
	}
	var eqBuf bytes.Buffer
	eqWriter := parquet.NewWriter(&eqBuf, parquet.SchemaOf(new(otherRow)))
	if err := eqWriter.Write(&otherRow{"x"}); err != nil {
		t.Fatalf("write eq delete: %v", err)
	}
	if err := eqWriter.Close(); err != nil {
		t.Fatalf("close eq writer: %v", err)
	}
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", "eq-del.parquet", &filer_pb.Entry{
		Name: "eq-del.parquet", Content: eqBuf.Bytes(),
	})

	eqDfb, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"data/eq-del.parquet", iceberg.ParquetFile, map[int]any{}, nil, nil, 1, int64(eqBuf.Len()))
	if err != nil {
		t.Fatalf("build eq delete: %v", err)
	}
	eqDfb.EqualityFieldIDs([]int{2})
	snapID := int64(1)
	eqEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, eqDfb.Build())

	_, err = collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl",
		[]iceberg.ManifestEntry{eqEntry}, newAddedColumnSchema())
	if err == nil {
		t.Fatal("collectEqualityDeletes: want an error for a missing equality column")
	}
}

// The v3 merge matches files on the first file's columns; the re-added
// column is refused there too.
func TestMergeParquetFilesV3ReaddedColumn(t *testing.T) {
	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "narrow.parquet", []narrowSchemaRow{{1, "a"}})
	putDataFileRows(t, fs, "re-added.parquet", []readdedNameSchemaRow{{2, "new-2"}})

	_, _, err := mergeNamedFiles(t, client,
		[]string{"narrow.parquet", "re-added.parquet"},
		map[string]int64{"narrow.parquet": 1, "re-added.parquet": 1}, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "cannot merge files with different schemas") {
		t.Fatalf("merge: %v, want schema mismatch", err)
	}
}
