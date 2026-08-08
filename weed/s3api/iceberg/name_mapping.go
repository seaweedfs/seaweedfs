package iceberg

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// nameMappingFromSchema derives the spec's field-id-to-names mapping from a
// table schema, mirroring Java's MappingUtil.create. List elements and map
// keys/values use the canonical "element"/"key"/"value" names.
func nameMappingFromSchema(schema *iceberg.Schema) iceberg.NameMapping {
	return mappedFieldsOf(schema.Fields())
}

func mappedFieldsOf(fields []iceberg.NestedField) []iceberg.MappedField {
	mapped := make([]iceberg.MappedField, 0, len(fields))
	for _, field := range fields {
		id := field.ID
		mapped = append(mapped, iceberg.MappedField{
			Names:   []string{field.Name},
			FieldID: &id,
			Fields:  mappedFieldsOfType(field.Type),
		})
	}
	return mapped
}

func mappedFieldsOfType(typ iceberg.Type) []iceberg.MappedField {
	switch t := typ.(type) {
	case *iceberg.StructType:
		return mappedFieldsOf(t.FieldList)
	case *iceberg.ListType:
		return mappedFieldsOf([]iceberg.NestedField{t.ElementField()})
	case *iceberg.MapType:
		return mappedFieldsOf([]iceberg.NestedField{t.KeyField(), t.ValueField()})
	default:
		return nil
	}
}

// ensureDefaultNameMapping sets schema.name-mapping.default so engines can
// read data files written without parquet field ids by falling back to
// name-based column resolution. Existing mappings are left untouched.
func ensureDefaultNameMapping(props iceberg.Properties, schema *iceberg.Schema) iceberg.Properties {
	if schema == nil || len(schema.Fields()) == 0 {
		return props
	}
	if _, ok := props[table.DefaultNameMappingKey]; ok {
		return props
	}
	mappingJSON, err := json.Marshal(nameMappingFromSchema(schema))
	if err != nil {
		return props
	}
	if props == nil {
		props = make(iceberg.Properties)
	}
	props[table.DefaultNameMappingKey] = string(mappingJSON)
	return props
}
