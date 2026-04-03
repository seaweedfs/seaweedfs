package sqltypes

import (
	"strconv"
)

var (
	// NULL represents the NULL value.
	NULL = Value{}
	// DontEscape tells you if a character should not be escaped.
	DontEscape = byte(255)
	nullstr    = []byte("null")
)

type Value struct {
	typ Type
	val []byte
}

// MakeTrusted makes a new Value based on the type.
// This function should only be used if you know the value
// and type conform to the rules. Every place this function is
// called, a comment is needed that explains why it's justified.
// Exceptions: The current package and mysql package do not need
// comments. Other packages can also use the function to create
// VarBinary or VarChar values.
func MakeTrusted(typ Type, val []byte) Value {

	if typ == Null {
		return NULL
	}

	return Value{typ: typ, val: val}
}

// NewInt64 builds an Int64 Value.
func NewInt64(v int64) Value {
	return MakeTrusted(Int64, strconv.AppendInt(nil, v, 10))
}

// NewInt32 builds an Int64 Value.
func NewInt32(v int32) Value {
	return MakeTrusted(Int32, strconv.AppendInt(nil, int64(v), 10))
}

// NewFloat32 builds an Float64 Value.
func NewFloat32(v float32) Value {
	return MakeTrusted(Float32, strconv.AppendFloat(nil, float64(v), 'f', -1, 64))
}

// NewFloat64 builds an Float64 Value.
func NewFloat64(v float64) Value {
	return MakeTrusted(Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

// NewVarChar builds a VarChar Value.
func NewVarChar(v string) Value {
	return MakeTrusted(VarChar, []byte(v))
}

// NewVarBinary builds a VarBinary Value.
// The input is a string because it's the most common use case.
func NewVarBinary(v string) Value {
	return MakeTrusted(VarBinary, []byte(v))
}

// MakeString makes a VarBinary Value.
func MakeString(val []byte) Value {
	return MakeTrusted(VarBinary, val)
}

// Type returns the type of Value.
func (v Value) Type() Type {
	return v.typ
}

// Raw returns the raw bytes. All types are currently implemented as []byte.
// You should avoid using this function. If you do, you should treat the
// bytes as read-only.
func (v Value) Raw() []byte {
	return v.val
}

// Len returns the length.
func (v Value) Len() int {
	return len(v.val)
}

// Values represents the array of Value.
type Values []Value

// String returns the raw value as a string.
func (v Value) String() string {
	return BytesToString(v.val)
}

// ToNative converts Value to a native go type.
// This does not work for sqltypes.Tuple. The function
// panics if there are inconsistencies.
func (v Value) ToNative() interface{} {
	var out interface{}
	var err error
	switch {
	case v.typ == Null:
		// no-op
	case IsSigned(v.typ):
		out, err = v.ParseInt64()
	case IsUnsigned(v.typ):
		out, err = v.ParseUint64()
	case IsFloat(v.typ):
		out, err = v.ParseFloat64()
	default:
		out = v.val
	}
	if err != nil {
		panic(err)
	}
	return out
}

// ParseInt64 will parse a Value into an int64. It does
// not check the type.
func (v Value) ParseInt64() (val int64, err error) {
	return strconv.ParseInt(v.String(), 10, 64)
}

// ParseUint64 will parse a Value into a uint64. It does
// not check the type.
func (v Value) ParseUint64() (val uint64, err error) {
	return strconv.ParseUint(v.String(), 10, 64)
}

// ParseFloat64 will parse a Value into an float64. It does
// not check the type.
func (v Value) ParseFloat64() (val float64, err error) {
	return strconv.ParseFloat(v.String(), 64)
}

// IsNull returns true if Value is null.
func (v Value) IsNull() bool {
	return v.typ == Null
}

// IsIntegral returns true if Value is an integral.
func (v Value) IsIntegral() bool {
	return IsIntegral(v.typ)
}

// IsSigned returns true if Value is a signed integral.
func (v Value) IsSigned() bool {
	return IsSigned(v.typ)
}

// IsUnsigned returns true if Value is an unsigned integral.
func (v Value) IsUnsigned() bool {
	return IsUnsigned(v.typ)
}

// IsFloat returns true if Value is a float.
func (v Value) IsFloat() bool {
	return IsFloat(v.typ)
}

// IsQuoted returns true if Value must be SQL-quoted.
func (v Value) IsQuoted() bool {
	return IsQuoted(v.typ)
}

// IsText returns true if Value is a collatable text.
func (v Value) IsText() bool {
	return IsText(v.typ)
}

// IsBinary returns true if Value is binary.
func (v Value) IsBinary() bool {
	return IsBinary(v.typ)
}

// IsTemporal returns true if Value is time type.
func (v Value) IsTemporal() bool {
	return IsTemporal(v.typ)
}

// ToString returns the value as MySQL would return it as string.
// If the value is not convertible like in the case of Expression, it returns nil.
func (v Value) ToString() string {
	return BytesToString(v.val)
}
