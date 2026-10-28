package iceberg

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// Where a delete file applies, decided on typed partition values. Partition maps come from manifests written by other
// engines, and one value can arrive in different Go shapes (a uuid as uuid.UUID, [16]byte or []byte; an int as int32 or
// int64; a null as nil or as a missing field), so comparing them as JSON strings misses equal partitions, and a delete
// that misses its partition is skipped: its deleted rows come back in the merged file for good. Every value is converted
// through the type the spec gives its field; a value that cannot be converted, a spec the table does not list, or a
// partitioned spec with no partition values makes the scope undecidable, and an undecidable scope fails closed: a
// delete file whose scope cannot be decided stops the compaction, and a bin whose own partition cannot be typed while a
// delete of its spec exists is not compacted.

// deleteScope is where a delete file may apply: every partition (global), or the data files of specID whose typed
// partition key is partKey.
type deleteScope struct {
	global  bool
	specID  int32
	partKey string
}

// covers reports whether the scope reaches a bin of specID whose typed partition key is partKey.
func (s deleteScope) covers(specID int32, partKey string) bool {
	return s.global || (specID == s.specID && partKey == s.partKey)
}

// key is the scope as a map key, its parts separated by NUL so no two scopes collide.
func (s deleteScope) key() string {
	if s.global {
		return "global"
	}
	return fmt.Sprintf("%d\x00%s", s.specID, s.partKey)
}

// deleteFileScope is the scope of a delete file. A spec with no fields or only void ones is global; any other spec
// needs a partition value for each of its fields, typed.
func deleteFileScope(df iceberg.DataFile, specs map[int]iceberg.PartitionSpec, schema *iceberg.Schema) (deleteScope, error) {
	spec, ok := specs[int(df.SpecID())]
	if !ok {
		return deleteScope{}, fmt.Errorf("delete file %s names partition spec %d, which the table does not list", df.FilePath(), df.SpecID())
	}
	if spec.IsUnpartitioned() {
		return deleteScope{global: true}, nil
	}
	if len(df.Partition()) == 0 {
		return deleteScope{}, fmt.Errorf("delete file %s has no partition values under partitioned spec %d", df.FilePath(), df.SpecID())
	}
	key, err := typedPartitionKey(spec, schema, df.Partition())
	if err != nil {
		return deleteScope{}, fmt.Errorf("delete file %s: %w", df.FilePath(), err)
	}
	return deleteScope{specID: df.SpecID(), partKey: key}, nil
}

// binTypedPartitionKey is the typed partition key of a bin: every file of a bin shares its spec and partition.
func binTypedPartitionKey(bin compactionBin, specs map[int]iceberg.PartitionSpec, schema *iceberg.Schema) (string, error) {
	spec, ok := specs[int(bin.SpecID)]
	if !ok {
		return "", fmt.Errorf("partition spec %d is not listed by the table", bin.SpecID)
	}
	if spec.IsUnpartitioned() {
		return "", nil
	}
	return typedPartitionKey(spec, schema, bin.Partition)
}

// eqDeleteGroupsForBin narrows the equality delete groups to those that apply to bin. When a group of the bin's spec
// exists and the bin's own partition cannot be typed, it cannot be told whether the group applies: an error, and the
// bin is not compacted.
func eqDeleteGroupsForBin(groups []equalityDeleteGroup, bin compactionBin, specs map[int]iceberg.PartitionSpec, schema *iceberg.Schema) ([]equalityDeleteGroup, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	binKey, keyErr := binTypedPartitionKey(bin, specs, schema)
	var applicable []equalityDeleteGroup
	for _, g := range groups {
		if g.Scope.global {
			applicable = append(applicable, g)
			continue
		}
		if g.Scope.specID != bin.SpecID {
			continue
		}
		if keyErr != nil {
			return nil, fmt.Errorf("equality deletes of spec %d may apply, but the bin's partition cannot be compared: %w", bin.SpecID, keyErr)
		}
		if g.Scope.partKey == binKey {
			applicable = append(applicable, g)
		}
	}
	return applicable, nil
}

// typedPartitionKey encodes a partition map field by field in the spec's order, each value converted through the
// type the spec gives its field. A field missing from the map is null, as a null value is.
func typedPartitionKey(spec iceberg.PartitionSpec, schema *iceberg.Schema, partition map[int]any) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("no table schema to type the partition values against")
	}
	fields := spec.PartitionType(schema).FieldList
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		v, err := typedPartitionValue(f.Type, partition[f.ID])
		if err != nil {
			return "", fmt.Errorf("partition field %d (%s): %w", f.ID, f.Name, err)
		}
		parts = append(parts, strconv.Itoa(f.ID)+"="+v)
	}
	return strings.Join(parts, "\x00"), nil
}

// literalAny unwraps an iceberg literal to its Go value.
type literalAny interface{ Any() any }

func typedPartitionValue(typ iceberg.Type, v any) (string, error) {
	switch v.(type) {
	case iceberg.AboveMaxLiteral, iceberg.BelowMinLiteral:
		return "", fmt.Errorf("an out-of-range literal %v", v)
	}
	if lit, ok := v.(literalAny); ok {
		v = lit.Any()
	}
	if v == nil {
		return "null", nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "null", nil
		}
		v = rv.Elem().Interface()
	}
	switch t := typ.(type) {
	case iceberg.UnknownType:
		return "null", nil // a dropped source field: its values are null by definition
	case iceberg.BooleanType:
		if b, ok := v.(bool); ok {
			return strconv.FormatBool(b), nil
		}
	case iceberg.Int32Type, iceberg.Int64Type, iceberg.DateType, iceberg.TimeType, iceberg.TimestampType,
		iceberg.TimestampTzType, iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		if i, ok := integerValue(v); ok {
			return "i" + strconv.FormatInt(i, 10), nil
		}
	case iceberg.Float32Type, iceberg.Float64Type:
		var f float64
		switch x := v.(type) {
		case float32:
			f = float64(x)
		case float64:
			f = x
		default:
			return "", fmt.Errorf("value %v (%T) does not convert to %s", v, v, typ)
		}
		switch {
		case math.IsNaN(f):
			return "fNaN", nil
		case f == 0:
			f = 0 // -0 and +0 are one partition value
		}
		return "f" + strconv.FormatUint(math.Float64bits(f), 16), nil
	case iceberg.StringType:
		switch s := v.(type) {
		case string:
			return strconv.Quote(s), nil
		case []byte:
			return strconv.Quote(string(s)), nil
		}
	case iceberg.UUIDType:
		switch u := v.(type) {
		case uuid.UUID:
			return "u" + u.String(), nil
		case string:
			if p, err := uuid.Parse(u); err == nil {
				return "u" + p.String(), nil
			}
		default:
			if b, ok := byteValue(v); ok && len(b) == 16 {
				return "u" + uuid.UUID(b).String(), nil
			}
		}
	case iceberg.FixedType, iceberg.BinaryType:
		if s, ok := v.(string); ok {
			return "b" + hex.EncodeToString([]byte(s)), nil
		}
		if b, ok := byteValue(v); ok {
			return "b" + hex.EncodeToString(b), nil
		}
	case iceberg.DecimalType:
		if d, ok := v.(iceberg.Decimal); ok {
			if d.Scale != t.Scale() {
				return "", fmt.Errorf("decimal of scale %d under type %s", d.Scale, t)
			}
			return "d" + d.Val.ToString(int32(t.Scale())), nil
		}
	default:
		return "", fmt.Errorf("partition values of type %s are not compared", typ)
	}
	return "", fmt.Errorf("value %v (%T) does not convert to %s", v, v, typ)
}

// integerValue reads any Go integer kind (iceberg.Date, Time and Timestamp included) as int64.
func integerValue(v any) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(rv.Uint()), true
	case reflect.Uint64:
		if u := rv.Uint(); u <= math.MaxInt64 {
			return int64(u), true
		}
	}
	return 0, false
}

// byteValue reads a []byte or a byte array of any length ([16]byte, uuid.UUID) as bytes.
func byteValue(v any) ([]byte, bool) {
	if b, ok := v.([]byte); ok {
		return b, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)
		return b, true
	}
	return nil, false
}
