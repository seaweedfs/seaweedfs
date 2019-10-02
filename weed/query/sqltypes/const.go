package sqltypes

// copied from vitness

// Flag allows us to qualify types by their common properties.
type Flag int32

const (
	Flag_NONE       Flag = 0
	Flag_ISINTEGRAL Flag = 256
	Flag_ISUNSIGNED Flag = 512
	Flag_ISFLOAT    Flag = 1024
	Flag_ISQUOTED   Flag = 2048
	Flag_ISTEXT     Flag = 4096
	Flag_ISBINARY   Flag = 8192
)

var Flag_name = map[int32]string{
	0:    "NONE",
	256:  "ISINTEGRAL",
	512:  "ISUNSIGNED",
	1024: "ISFLOAT",
	2048: "ISQUOTED",
	4096: "ISTEXT",
	8192: "ISBINARY",
}
var Flag_value = map[string]int32{
	"NONE":       0,
	"ISINTEGRAL": 256,
	"ISUNSIGNED": 512,
	"ISFLOAT":    1024,
	"ISQUOTED":   2048,
	"ISTEXT":     4096,
	"ISBINARY":   8192,
}

// Type defines the various supported data types in bind vars
// and query results.
type Type int32

const (
	// NULL_TYPE specifies a NULL type.
	Type_NULL_TYPE Type = 0
	// INT8 specifies a TINYINT type.
	// Properties: 1, IsNumber.
	Type_INT8 Type = 257
	// UINT8 specifies a TINYINT UNSIGNED type.
	// Properties: 2, IsNumber, IsUnsigned.
	Type_UINT8 Type = 770
	// INT16 specifies a SMALLINT type.
	// Properties: 3, IsNumber.
	Type_INT16 Type = 259
	// UINT16 specifies a SMALLINT UNSIGNED type.
	// Properties: 4, IsNumber, IsUnsigned.
	Type_UINT16 Type = 772
	// INT24 specifies a MEDIUMINT type.
	// Properties: 5, IsNumber.
	Type_INT32 Type = 263
	// UINT32 specifies a INTEGER UNSIGNED type.
	// Properties: 8, IsNumber, IsUnsigned.
	Type_UINT32 Type = 776
	// INT64 specifies a BIGINT type.
	// Properties: 9, IsNumber.
	Type_INT64 Type = 265
	// UINT64 specifies a BIGINT UNSIGNED type.
	// Properties: 10, IsNumber, IsUnsigned.
	Type_UINT64 Type = 778
	// FLOAT32 specifies a FLOAT type.
	// Properties: 11, IsFloat.
	Type_FLOAT32 Type = 1035
	// FLOAT64 specifies a DOUBLE or REAL type.
	// Properties: 12, IsFloat.
	Type_FLOAT64 Type = 1036
	// TIMESTAMP specifies a TIMESTAMP type.
	// Properties: 13, IsQuoted.
	Type_TIMESTAMP Type = 2061
	// DATE specifies a DATE type.
	// Properties: 14, IsQuoted.
	Type_DATE Type = 2062
	// TIME specifies a TIME type.
	// Properties: 15, IsQuoted.
	Type_TIME Type = 2063
	// DATETIME specifies a DATETIME type.
	// Properties: 16, IsQuoted.
	Type_DATETIME Type = 2064
	// YEAR specifies a YEAR type.
	// Properties: 17, IsNumber, IsUnsigned.
	Type_YEAR Type = 785
	// DECIMAL specifies a DECIMAL or NUMERIC type.
	// Properties: 18, None.
	Type_DECIMAL Type = 18
	// TEXT specifies a TEXT type.
	// Properties: 19, IsQuoted, IsText.
	Type_TEXT Type = 6163
	// BLOB specifies a BLOB type.
	// Properties: 20, IsQuoted, IsBinary.
	Type_BLOB Type = 10260
	// VARCHAR specifies a VARCHAR type.
	// Properties: 21, IsQuoted, IsText.
	Type_VARCHAR Type = 6165
	// VARBINARY specifies a VARBINARY type.
	// Properties: 22, IsQuoted, IsBinary.
	Type_VARBINARY Type = 10262
	// CHAR specifies a CHAR type.
	// Properties: 23, IsQuoted, IsText.
	Type_CHAR Type = 6167
	// BINARY specifies a BINARY type.
	// Properties: 24, IsQuoted, IsBinary.
	Type_BINARY Type = 10264
	// BIT specifies a BIT type.
	// Properties: 25, IsQuoted.
	Type_BIT Type = 2073
	// JSON specifies a JSON type.
	// Properties: 30, IsQuoted.
	Type_JSON Type = 2078
)

// BindVariable represents a single bind variable in a Query.
type BindVariable struct {
	Type  Type   `protobuf:"varint,1,opt,name=type,enum=query.Type" json:"type,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	// values are set if type is TUPLE.
	Values []*Value `protobuf:"bytes,3,rep,name=values" json:"values,omitempty"`
}
