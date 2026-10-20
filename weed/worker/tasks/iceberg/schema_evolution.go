package iceberg

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// The bin-pack merge of a v1/v2 table whose data files spell its schema
// differently. A bin used to take the first file's Parquet schema as the
// writer's and refuse every file whose schema was not structurally identical
// ("cannot merge files with different schemas"). That refused:
//   - schema evolution: a column added to the table after some files were
//     written;
//   - one Iceberg schema written REQUIRED by one engine and OPTIONAL by another.
//
// A file whose flat columns are covered by the writer's schema, matched by
// Iceberg field id, is converted into it:
//   - the same field id carries the same name and type;
//   - the writer's column may be optional where the file's is required, never
//     the reverse;
//   - a writer column the file lacks must be optional, and reads as null.
//
// A file that covers the writer's schema instead (a wider, newer one) restarts
// the bin with that file's schema, so no column is ever dropped. Anything else
// (a rename, a type change, a column only one side has both ways, nested
// columns, no field ids) is refused as before.

// flatColumnsByFieldID is the schema's columns by Iceberg field id when every
// column is a top-level, non-repeated leaf with a field id of its own; ok is
// false otherwise.
func flatColumnsByFieldID(s *parquet.Schema) (map[int]parquet.Field, bool) {
	if s == nil {
		return nil, false
	}
	out := make(map[int]parquet.Field)
	for _, f := range s.Fields() {
		if !f.Leaf() || f.Repeated() || f.ID() <= 0 {
			return nil, false
		}
		if _, dup := out[f.ID()]; dup {
			return nil, false
		}
		out[f.ID()] = f
	}
	return out, true
}

// schemaCovers reports whether every row of a file written in source converts
// to target without losing or inventing a value (the rules above).
func schemaCovers(target, source *parquet.Schema) bool {
	targetCols, ok := flatColumnsByFieldID(target)
	if !ok {
		return false
	}
	sourceCols, ok := flatColumnsByFieldID(source)
	if !ok {
		return false
	}
	for id, s := range sourceCols {
		t, ok := targetCols[id]
		if !ok || t.Name() != s.Name() || !parquet.EqualTypes(t.Type(), s.Type()) {
			return false
		}
		if t.Required() && !s.Required() {
			return false
		}
	}
	for id, t := range targetCols {
		if _, ok := sourceCols[id]; !ok && !t.Optional() {
			return false
		}
	}
	return true
}

// widerSchemaError stops a merge that met a file whose schema covers the
// writer's: the bin is merged again with it.
type widerSchemaError struct {
	schema *parquet.Schema
	path   string
}

func (e *widerSchemaError) Error() string {
	return fmt.Sprintf("schema of %s is wider than the bin's writer schema", e.path)
}

// evolvedFileConversion is how a file of schema source joins a merge written
// in target: nil when they are equal, a conversion when target covers it, a
// *widerSchemaError when it covers target, else the merge's mismatch error.
func evolvedFileConversion(target, source *parquet.Schema, path string) (parquet.Conversion, error) {
	if schemasEqual(target, source) {
		return nil, nil
	}
	if schemaCovers(target, source) {
		conv, err := parquet.Convert(target, source)
		if err != nil {
			return nil, fmt.Errorf("schema of %s: %w", path, err)
		}
		return conv, nil
	}
	if schemaCovers(source, target) {
		return nil, &widerSchemaError{schema: source, path: path}
	}
	return nil, fmt.Errorf("schema mismatch in %s: cannot merge files with different schemas", path)
}
