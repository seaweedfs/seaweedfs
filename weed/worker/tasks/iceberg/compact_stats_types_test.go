package iceberg

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

// The bound converter covers the types engines write besides the plain
// numbers and strings: a compacted file whose DECIMAL, TIME, UUID or
// millisecond-timestamp column has no bounds can never be skipped on it.
func TestBoundLiteralTypes(t *testing.T) {
	u := uuid.MustParse("0b6a4f7e-6c1d-4c7a-9a52-2c51c0d6a9f1")
	cases := []struct {
		name    string
		v       parquet.Value
		pt      parquet.Type
		it      iceberg.Type
		roundUp bool
		want    iceberg.Literal // nil = no bound
	}{
		{"decimal(18,2) as INT64", parquet.Int64Value(-12345), parquet.Decimal(2, 18, parquet.Int64Type).Type(), iceberg.DecimalTypeOf(18, 2), false,
			iceberg.DecimalLiteral(iceberg.Decimal{Val: decimal128.FromI64(-12345), Scale: 2})},
		{"decimal(9,2) as INT32", parquet.Int32Value(705), parquet.Decimal(2, 9, parquet.Int32Type).Type(), iceberg.DecimalTypeOf(9, 2), true,
			iceberg.DecimalLiteral(iceberg.Decimal{Val: decimal128.FromI64(705), Scale: 2})},
		{"decimal at another scale: none", parquet.Int64Value(1), parquet.Decimal(3, 18, parquet.Int64Type).Type(), iceberg.DecimalTypeOf(18, 2), false, nil},
		{"decimal(38,2) as bytes: none", parquet.FixedLenByteArrayValue(make([]byte, 16)), parquet.Decimal(2, 38, parquet.FixedLenByteArrayType(16)).Type(), iceberg.DecimalTypeOf(38, 2), false, nil},
		{"time (us)", parquet.Int64Value(3_600_000_000), parquet.Time(parquet.Microsecond).Type(), iceberg.PrimitiveTypes.Time, false,
			iceberg.TimeLiteral(3_600_000_000)},
		{"time (ms) is INT32", parquet.Int32Value(3_600_000), parquet.Time(parquet.Millisecond).Type(), iceberg.PrimitiveTypes.Time, false,
			iceberg.TimeLiteral(3_600_000_000)},
		{"timestamp ms, lower", parquet.Int64Value(1_700_000_000_123), parquet.Timestamp(parquet.Millisecond).Type(), iceberg.PrimitiveTypes.Timestamp, false,
			iceberg.TimestampLiteral(1_700_000_000_123_000)},
		{"timestamp ns to us, upper rounds up", parquet.Int64Value(1_700_000_000_000_000_001), parquet.Timestamp(parquet.Nanosecond).Type(), iceberg.PrimitiveTypes.TimestampTz, true,
			iceberg.TimestampLiteral(1_700_000_000_000_001)},
		{"timestamp ns to us, lower rounds down", parquet.Int64Value(1_700_000_000_000_000_001), parquet.Timestamp(parquet.Nanosecond).Type(), iceberg.PrimitiveTypes.TimestampTz, false,
			iceberg.TimestampLiteral(1_700_000_000_000_000)},
		{"negative timestamp ns to us, lower rounds down", parquet.Int64Value(-1_500), parquet.Timestamp(parquet.Nanosecond).Type(), iceberg.PrimitiveTypes.TimestampTz, false,
			iceberg.TimestampLiteral(-2)},
		{"negative timestamp ns to us, upper rounds up", parquet.Int64Value(-1_500), parquet.Timestamp(parquet.Nanosecond).Type(), iceberg.PrimitiveTypes.TimestampTz, true,
			iceberg.TimestampLiteral(-1)},
		{"timestamptz ns us rescale", parquet.Int64Value(1_700_000_000_123), parquet.Timestamp(parquet.Microsecond).Type(), iceberg.TimestampTzNsType{}, false,
			iceberg.TimestampNsLiteral(1_700_000_000_123_000)},
		{"timestamp ms to us overflows: none", parquet.Int64Value(1 << 62), parquet.Timestamp(parquet.Millisecond).Type(), iceberg.PrimitiveTypes.Timestamp, false, nil},
		{"uuid", parquet.FixedLenByteArrayValue(u[:]), parquet.UUID().Type(), iceberg.PrimitiveTypes.UUID, false, iceberg.UUIDLiteral(u)},
		{"fixed[4]", parquet.FixedLenByteArrayValue([]byte{1, 2, 3, 4}), parquet.FixedLenByteArrayType(4), iceberg.FixedTypeOf(4), false,
			iceberg.FixedLiteral([]byte{1, 2, 3, 4})},
		{"fixed of another length: none", parquet.FixedLenByteArrayValue([]byte{1, 2, 3}), parquet.FixedLenByteArrayType(3), iceberg.FixedTypeOf(4), false, nil},
		{"double from FLOAT", parquet.FloatValue(1.5), parquet.FloatType, iceberg.PrimitiveTypes.Float64, false, iceberg.Float64Literal(1.5)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := boundLiteral(c.v, c.pt, c.it, c.roundUp)
			if c.want == nil {
				if ok {
					t.Fatalf("want no bound, got %v", got)
				}
				return
			}
			if !ok || !got.Equals(c.want) {
				t.Fatalf("got %v (ok %v), want %v", got, ok, c.want)
			}
		})
	}
}

// A file written with a DECIMAL(18,2) stored as INT64, a millisecond
// timestamp, a nanosecond timestamp read as a microsecond timestamptz, a
// TIME, a UUID and a FIXED[4] column, in two row groups, gets lower and upper
// bounds for each of them.
func TestCollectColumnStatsBoundsMoreTypes(t *testing.T) {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "price", Type: iceberg.DecimalTypeOf(18, 2), Required: true},
		iceberg.NestedField{ID: 2, Name: "ts_ms", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 3, Name: "ts_ns", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 4, Name: "tod", Type: iceberg.PrimitiveTypes.Time, Required: true},
		iceberg.NestedField{ID: 5, Name: "id", Type: iceberg.PrimitiveTypes.UUID, Required: true},
		iceberg.NestedField{ID: 6, Name: "code", Type: iceberg.FixedTypeOf(4), Required: true},
	)
	schema := parquet.NewSchema("table", parquet.Group{
		"price": parquet.FieldID(parquet.Decimal(2, 18, parquet.Int64Type), 1),
		"ts_ms": parquet.FieldID(parquet.Timestamp(parquet.Millisecond), 2),
		"ts_ns": parquet.FieldID(parquet.Timestamp(parquet.Nanosecond), 3),
		"tod":   parquet.FieldID(parquet.Time(parquet.Microsecond), 4),
		"id":    parquet.FieldID(parquet.UUID(), 5),
		"code":  parquet.FieldID(parquet.Leaf(parquet.FixedLenByteArrayType(4)), 6),
	})
	col := map[string]int{}
	for i, p := range schema.Columns() {
		col[strings.Join(p, ".")] = i
	}
	u1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	u2 := uuid.MustParse("ffffffff-0000-0000-0000-000000000002")
	type row struct {
		price, tsMS, tsNS, tod int64
		id                     uuid.UUID
		code                   []byte
	}
	groups := [][]row{
		{{-12345, 1_700_000_000_123, -1_500, 3_600_000_000, u2, []byte{9, 9, 9, 9}}},
		{{67890, 1_700_000_001_000, 2_500, 7_200_000_000, u1, []byte{1, 2, 3, 4}}},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[any](&buf, schema)
	for _, g := range groups {
		for _, r := range g {
			pr := make(parquet.Row, len(col))
			pr[col["price"]] = parquet.Int64Value(r.price).Level(0, 0, col["price"])
			pr[col["ts_ms"]] = parquet.Int64Value(r.tsMS).Level(0, 0, col["ts_ms"])
			pr[col["ts_ns"]] = parquet.Int64Value(r.tsNS).Level(0, 0, col["ts_ns"])
			pr[col["tod"]] = parquet.Int64Value(r.tod).Level(0, 0, col["tod"])
			pr[col["id"]] = parquet.FixedLenByteArrayValue(r.id[:]).Level(0, 0, col["id"])
			pr[col["code"]] = parquet.FixedLenByteArrayValue(r.code).Level(0, 0, col["code"])
			if _, err := w.WriteRows([]parquet.Row{pr}); err != nil {
				t.Fatalf("write row: %v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	stats, err := collectColumnStats(buf.Bytes(), icebergSchema)
	if err != nil {
		t.Fatal(err)
	}
	dec := func(v int64) iceberg.Literal {
		return iceberg.DecimalLiteral(iceberg.Decimal{Val: decimal128.FromI64(v), Scale: 2})
	}
	want := [][2]iceberg.Literal{ // by field id, from 1
		{dec(-12345), dec(67890)},
		{iceberg.TimestampLiteral(1_700_000_000_123_000), iceberg.TimestampLiteral(1_700_000_001_000_000)},
		{iceberg.TimestampLiteral(-2), iceberg.TimestampLiteral(3)}, // rounded outwards
		{iceberg.TimeLiteral(3_600_000_000), iceberg.TimeLiteral(7_200_000_000)},
		{iceberg.UUIDLiteral(u1), iceberg.UUIDLiteral(u2)},
		{iceberg.FixedLiteral([]byte{1, 2, 3, 4}), iceberg.FixedLiteral([]byte{9, 9, 9, 9})},
	}
	for i, w := range want {
		id := i + 1
		name := icebergSchema.Field(i).Name
		for j, got := range [][]byte{stats.lower[id], stats.upper[id]} {
			kind := [2]string{"lower", "upper"}[j]
			exp, err := w[j].MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Errorf("%s: no %s bound", name, kind)
			} else if !bytes.Equal(got, exp) {
				t.Errorf("%s: %s bound %x, want %x", name, kind, got, exp)
			}
		}
	}
}
