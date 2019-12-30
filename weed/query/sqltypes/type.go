package sqltypes

// These bit flags can be used to query on the
// common properties of types.
const (
	flagIsIntegral = int(Flag_ISINTEGRAL)
	flagIsUnsigned = int(Flag_ISUNSIGNED)
	flagIsFloat    = int(Flag_ISFLOAT)
	flagIsQuoted   = int(Flag_ISQUOTED)
	flagIsText     = int(Flag_ISTEXT)
	flagIsBinary   = int(Flag_ISBINARY)
)

// IsIntegral returns true if Type is an integral
// (signed/unsigned) that can be represented using
// up to 64 binary bits.
// If you have a Value object, use its member function.
func IsIntegral(t Type) bool {
	return int(t)&flagIsIntegral == flagIsIntegral
}

// IsSigned returns true if Type is a signed integral.
// If you have a Value object, use its member function.
func IsSigned(t Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral
}

// IsUnsigned returns true if Type is an unsigned integral.
// Caution: this is not the same as !IsSigned.
// If you have a Value object, use its member function.
func IsUnsigned(t Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsFloat returns true is Type is a floating point.
// If you have a Value object, use its member function.
func IsFloat(t Type) bool {
	return int(t)&flagIsFloat == flagIsFloat
}

// IsQuoted returns true if Type is a quoted text or binary.
// If you have a Value object, use its member function.
func IsQuoted(t Type) bool {
	return (int(t)&flagIsQuoted == flagIsQuoted) && t != Bit
}

// IsText returns true if Type is a text.
// If you have a Value object, use its member function.
func IsText(t Type) bool {
	return int(t)&flagIsText == flagIsText
}

// IsBinary returns true if Type is a binary.
// If you have a Value object, use its member function.
func IsBinary(t Type) bool {
	return int(t)&flagIsBinary == flagIsBinary
}

// isNumber returns true if the type is any type of number.
func isNumber(t Type) bool {
	return IsIntegral(t) || IsFloat(t) || t == Decimal
}

// IsTemporal returns true if Value is time type.
func IsTemporal(t Type) bool {
	switch t {
	case Timestamp, Date, Time, Datetime:
		return true
	}
	return false
}

// Vitess data types. These are idiomatically
// named synonyms for the Type values.
const (
	Null      = Type_NULL_TYPE
	Int8      = Type_INT8
	Uint8     = Type_UINT8
	Int16     = Type_INT16
	Uint16    = Type_UINT16
	Int32     = Type_INT32
	Uint32    = Type_UINT32
	Int64     = Type_INT64
	Uint64    = Type_UINT64
	Float32   = Type_FLOAT32
	Float64   = Type_FLOAT64
	Timestamp = Type_TIMESTAMP
	Date      = Type_DATE
	Time      = Type_TIME
	Datetime  = Type_DATETIME
	Year      = Type_YEAR
	Decimal   = Type_DECIMAL
	Text      = Type_TEXT
	Blob      = Type_BLOB
	VarChar   = Type_VARCHAR
	VarBinary = Type_VARBINARY
	Char      = Type_CHAR
	Binary    = Type_BINARY
	Bit       = Type_BIT
	TypeJSON  = Type_JSON
)
