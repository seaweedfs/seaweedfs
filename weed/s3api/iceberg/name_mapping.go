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

// refreshDefaultNameMapping keeps schema.name-mapping.default in sync with
// the current schema across commits, so schema evolution does not strand
// readers of field-id-less data files on the creation-time mapping. Only
// mappings the catalog generated itself are replaced: a stored mapping that
// differs from the one derived from the pre-commit schema was set by the user
// and is left alone. Returns the (possibly patched) serialized metadata.
func refreshDefaultNameMapping(raw []byte, base, updated table.Metadata) []byte {
	if updated == nil {
		return raw
	}
	updatedSchema := updated.CurrentSchema()
	if updatedSchema == nil || len(updatedSchema.Fields()) == 0 {
		return raw
	}
	wantJSON, err := json.Marshal(nameMappingFromSchema(updatedSchema))
	if err != nil {
		return raw
	}
	want := string(wantJSON)

	existing := updated.Properties()[table.DefaultNameMappingKey]
	if existing == want {
		return raw
	}
	if existing != "" {
		if base == nil {
			return raw
		}
		baseSchema := base.CurrentSchema()
		if baseSchema == nil {
			return raw
		}
		baseJSON, err := json.Marshal(nameMappingFromSchema(baseSchema))
		if err != nil || existing != string(baseJSON) {
			return raw
		}
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return raw
	}
	properties := map[string]json.RawMessage{}
	if rawProperties, ok := metadata["properties"]; ok {
		if err := json.Unmarshal(rawProperties, &properties); err != nil {
			return raw
		}
	}
	valueJSON, err := json.Marshal(want)
	if err != nil {
		return raw
	}
	properties[table.DefaultNameMappingKey] = valueJSON
	propertiesJSON, err := json.Marshal(properties)
	if err != nil {
		return raw
	}
	metadata["properties"] = propertiesJSON
	patched, err := json.Marshal(metadata)
	if err != nil {
		return raw
	}
	return patched
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
