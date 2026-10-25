package iceberg

import (
	"fmt"
	"reflect"

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
// The writer's schema is now the union of the bin's files' schemas, matched
// by Iceberg field id and computed before any row is written (schemaUnion),
// so every file converts into it in a single pass:
//   - the same field id carries the same name and type;
//   - the writer's column may be optional where the file's is required, never
//     the reverse;
//   - a writer column the file lacks must be optional, and reads as null.
//
// Anything else (a rename, a type change, the same name under different field
// ids, nested columns, no field ids) is refused as before.

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

// schemaUnion folds a bin's file schemas into the writer schema the merge
// needs: every field id any file carries, in the order first seen, under the
// name and type all of its files agree on. A column is REQUIRED only when
// every file has it and spells it so; a file that lacks it or spells it
// optional reads and writes nulls there. A shared field id with a different
// name or type, or the same name under different field ids (a column dropped
// and re-added), is a conflict the merge refuses.
type schemaUnion struct {
	fields []unionField
	byID   map[int]int
	byName map[string]int
	folds  int
}

type unionField struct {
	field        parquet.Field // the field as first seen
	required     bool          // required in every file that carries it
	presentInAll bool          // present in every file folded so far
}

func (u *schemaUnion) fold(s *parquet.Schema, path string) error {
	cols, ok := flatColumnsByFieldID(s)
	if !ok {
		return fmt.Errorf("schema of %s does not key flat columns by field id", path)
	}
	if u.byID == nil {
		u.byID, u.byName = make(map[int]int), make(map[string]int)
	}
	for _, f := range s.Fields() {
		id, name := f.ID(), f.Name()
		if j, ok := u.byID[id]; ok {
			e := &u.fields[j]
			if e.field.Name() != name || !parquet.EqualTypes(e.field.Type(), f.Type()) {
				return fmt.Errorf("field id %d is %q in one file but %q in %s", id, e.field.Name(), name, path)
			}
			e.required = e.required && f.Required()
			continue
		}
		if _, ok := u.byName[name]; ok {
			return fmt.Errorf("column %q carries different field ids in %s", name, path)
		}
		u.byID[id], u.byName[name] = len(u.fields), id
		// A field folded in after the first schema is absent from every
		// file seen before it.
		u.fields = append(u.fields, unionField{field: f, required: f.Required(), presentInAll: u.folds == 0})
	}
	for i := range u.fields {
		if _, ok := cols[u.fields[i].field.ID()]; !ok {
			u.fields[i].presentInAll = false
		}
	}
	u.folds++
	return nil
}

// schema is the union as a writer schema; name and group give the root node
// (their Go type is never read: the merge writes parquet.Row values).
func (u *schemaUnion) schema(name string, group parquet.Node) *parquet.Schema {
	fields := make([]parquet.Field, len(u.fields))
	for i, e := range u.fields {
		fields[i] = e.field
		if !e.field.Optional() && !(e.required && e.presentInAll) {
			fields[i] = unionOptionalField{Node: parquet.Optional(e.field), name: e.field.Name()}
		}
	}
	return parquet.NewSchema(name, &orderedGroup{Node: group, fields: fields})
}

// unionOptionalField spells a field optional in the union schema when the
// file it came from wrote it required.
type unionOptionalField struct {
	parquet.Node
	name string
}

func (f unionOptionalField) Name() string                           { return f.name }
func (f unionOptionalField) Value(base reflect.Value) reflect.Value { return reflect.Value{} }

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
