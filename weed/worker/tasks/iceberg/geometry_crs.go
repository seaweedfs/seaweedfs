package iceberg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
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
// error.
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
	out := make([]byte, 0, footerStart+len(footer)+8)
	out = append(out, data[:footerStart]...)
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

// schemaElementPaths is each flattened schema element's path from the root
// (the root's is empty): the footer lists the tree depth first, every group
// followed by its num_children children.
func schemaElementPaths(elements []format.SchemaElement) [][]string {
	paths := make([][]string, len(elements))
	var walk func(i int, parent []string) int
	walk = func(i int, parent []string) int {
		if i >= len(elements) {
			return i
		}
		var path []string
		if i > 0 {
			path = append(append([]string{}, parent...), elements[i].Name)
		}
		paths[i] = path
		next := i + 1
		children := int(elements[i].NumChildren.V)
		for c := 0; c < children && next < len(elements); c++ {
			next = walk(next, path)
		}
		return next
	}
	walk(0, nil)
	return paths
}
