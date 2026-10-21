package iceberg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// A GEOMETRY column whose Iceberg type has the default CRS (OGC:CRS84) is
// written with no CRS in the Parquet logical type, as the input files from
// most writers declare it. parquet-go spells the default out when it writes
// such a column, even when the column it read declared none. Both mean the
// same, but a reader may then see the file column as a different type from
// the table column (DuckDB casts it and no longer prunes row groups by their
// statistics). An explicit CRS is left as it is.
//
// The footer is changed after the file is written rather than by giving the
// writer another column type: parquet-go refuses to convert between two
// GEOMETRY types that both mean the default, and its sorting writer
// converts. Only the footer's schema elements change; the data, the page
// indexes and every offset into the file stay byte for byte.

var parquetMagic = []byte("PAR1")

// withDefaultGeometryCRSUnset returns data with the CRS of every GEOMETRY
// column whose Iceberg type has the default CRS left unset in the footer, and
// whether anything changed. A file that needs no change comes back as it was;
// so does one whose footer cannot be read (encrypted, not Parquet), with an
// error. A changed file is written into data's buffer when it fits, so the
// caller uses the result in place of data.
func withDefaultGeometryCRSUnset(data []byte, schema *iceberg.Schema) ([]byte, bool, error) {
	if schema == nil || !hasDefaultCRSGeometry(schema) {
		return data, false, nil
	}
	n := len(data)
	if n < 12 || !bytes.Equal(data[n-4:], parquetMagic) || !bytes.Equal(data[:4], parquetMagic) {
		return data, false, fmt.Errorf("not a plaintext parquet file")
	}
	footerLen := int(binary.LittleEndian.Uint32(data[n-8 : n-4]))
	footerStart := n - 8 - footerLen
	if footerLen <= 0 || footerStart < 4 {
		return data, false, fmt.Errorf("footer length %d out of range", footerLen)
	}
	var protocol thrift.CompactProtocol
	var md format.FileMetaData
	if err := thrift.Unmarshal(&protocol, data[footerStart:n-8], &md); err != nil {
		return data, false, fmt.Errorf("decode footer: %w", err)
	}
	changed := false
	for i, path := range schemaElementPaths(md.Schema) {
		el := &md.Schema[i]
		g, ok := el.LogicalType.Value.(*format.GeometryType)
		if !ok || g.CRS != iceberg.DefaultGeoCRS {
			continue
		}
		var field iceberg.NestedField
		if el.FieldID > 0 {
			field, ok = schema.FindFieldByID(int(el.FieldID))
		} else {
			field, ok = schema.FindFieldByName(strings.Join(path, "."))
		}
		if !ok {
			continue
		}
		if gt, isGeo := field.Type.(iceberg.GeometryType); !isGeo || gt.CRS() != iceberg.DefaultGeoCRS {
			continue
		}
		el.LogicalType = format.LogicalType{Value: &format.GeometryType{}}
		changed = true
	}
	if !changed {
		return data, false, nil
	}
	footer, err := thrift.Marshal(&protocol, &md)
	if err != nil {
		return data, false, fmt.Errorf("encode footer: %w", err)
	}
	// The new footer is written over the old one in data's own buffer when it
	// fits, as it does when a CRS is removed: the file is as large as a
	// compaction target, and copying it would double the memory it takes.
	out := data[:footerStart]
	if cap(data) < footerStart+len(footer)+8 {
		out = make([]byte, footerStart, footerStart+len(footer)+8)
		copy(out, data[:footerStart])
	}
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, parquetMagic...)
	return out, true, nil
}

func hasDefaultCRSGeometry(schema *iceberg.Schema) bool {
	for _, f := range schema.Fields() {
		if typeHasDefaultCRSGeometry(f.Type) {
			return true
		}
	}
	return false
}

// typeHasDefaultCRSGeometry reports whether the type is, or nests, a geometry
// with the default CRS.
func typeHasDefaultCRSGeometry(t iceberg.Type) bool {
	switch v := t.(type) {
	case iceberg.GeometryType:
		return v.CRS() == iceberg.DefaultGeoCRS
	case *iceberg.StructType:
		for _, f := range v.FieldList {
			if typeHasDefaultCRSGeometry(f.Type) {
				return true
			}
		}
	case *iceberg.ListType:
		return typeHasDefaultCRSGeometry(v.Element)
	case *iceberg.MapType:
		return typeHasDefaultCRSGeometry(v.KeyType) || typeHasDefaultCRSGeometry(v.ValueType)
	}
	return false
}

// schemaElementPaths is each flattened schema element's Iceberg field path
// from the root (the root's is empty): the footer lists the tree depth first,
// every group followed by its num_children children. The repeated group of a
// LIST or MAP is not an Iceberg field, so it takes its parent's path; under it
// a list's element is named "element" and a map's two children "key" and
// "value", as Iceberg names them, whatever the writer called them.
func schemaElementPaths(elements []format.SchemaElement) [][]string {
	paths := make([][]string, len(elements))
	var walk func(i int, path []string) int
	// children walks the n children of a group whose own path is path,
	// naming the k-th by name(k) when name is set.
	children := func(next, n int, path []string, name func(k int, el *format.SchemaElement) string) int {
		for c := 0; c < n && next < len(elements); c++ {
			child := path
			if name != nil {
				child = append(append([]string{}, path...), name(c, &elements[next]))
			} else {
				child = append(append([]string{}, path...), elements[next].Name)
			}
			next = walk(next, child)
		}
		return next
	}
	walk = func(i int, path []string) int {
		if i >= len(elements) {
			return i
		}
		paths[i] = path
		el := &elements[i]
		n := int(el.NumChildren.V)
		if n != 1 || i+1 >= len(elements) {
			return children(i+1, n, path, nil)
		}
		repeated := &elements[i+1]
		if rt, ok := repeated.RepetitionType.Get(); !ok || rt != format.Repeated {
			return children(i+1, n, path, nil)
		}
		switch {
		case isListAnnotated(el):
			// A repeated group of one field wraps the element, unless its
			// name marks the legacy form where the group is the element.
			if rn := int(repeated.NumChildren.V); rn == 1 && repeated.Name != "array" && repeated.Name != el.Name+"_tuple" {
				paths[i+1] = path
				return children(i+2, 1, path, func(int, *format.SchemaElement) string { return "element" })
			}
			return children(i+1, 1, path, func(int, *format.SchemaElement) string { return "element" })
		case isMapAnnotated(el):
			if int(repeated.NumChildren.V) == 2 {
				paths[i+1] = path
				return children(i+2, 2, path, func(k int, _ *format.SchemaElement) string {
					if k == 0 {
						return "key"
					}
					return "value"
				})
			}
		}
		return children(i+1, n, path, nil)
	}
	walk(0, nil)
	return paths
}

func isListAnnotated(el *format.SchemaElement) bool {
	if _, ok := el.LogicalType.Value.(*format.ListType); ok {
		return true
	}
	ct, ok := el.ConvertedType.Get()
	return ok && ct == deprecated.List
}

func isMapAnnotated(el *format.SchemaElement) bool {
	if _, ok := el.LogicalType.Value.(*format.MapType); ok {
		return true
	}
	ct, ok := el.ConvertedType.Get()
	return ok && (ct == deprecated.Map || ct == deprecated.MapKeyValue)
}
