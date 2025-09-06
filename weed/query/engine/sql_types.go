package engine

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// convertSQLTypeToMQ converts SQL column types to MQ schema field types
// Assumptions:
// 1. Standard SQL types map to MQ scalar types
// 2. Unsupported types result in errors
// 3. Default sizes are used for variable-length types
func (e *SQLEngine) convertSQLTypeToMQ(sqlType TypeRef) (*schema_pb.Type, error) {
	typeName := strings.ToUpper(sqlType.Type)

	switch typeName {
	case "BOOLEAN", "BOOL":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BOOL}}, nil

	case "TINYINT", "SMALLINT", "INT", "INTEGER", "MEDIUMINT":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}, nil

	case "BIGINT":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, nil

	case "FLOAT", "REAL":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_FLOAT}}, nil

	case "DOUBLE", "DOUBLE PRECISION":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}, nil

	case "CHAR", "VARCHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, nil

	case "BINARY", "VARBINARY", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES}}, nil

	case "JSON":
		// JSON stored as string for now
		// TODO: Implement proper JSON type support
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, nil

	case "TIMESTAMP", "DATETIME":
		// Store as BIGINT (Unix timestamp in nanoseconds)
		return &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, nil

	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", typeName)
	}
}

// convertMQTypeToSQL converts MQ schema field types back to SQL column types
// This is the reverse of convertSQLTypeToMQ for display purposes
func (e *SQLEngine) convertMQTypeToSQL(fieldType *schema_pb.Type) string {
	switch t := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch t.ScalarType {
		case schema_pb.ScalarType_BOOL:
			return "BOOLEAN"
		case schema_pb.ScalarType_INT32:
			return "INT"
		case schema_pb.ScalarType_INT64:
			return "BIGINT"
		case schema_pb.ScalarType_FLOAT:
			return "FLOAT"
		case schema_pb.ScalarType_DOUBLE:
			return "DOUBLE"
		case schema_pb.ScalarType_BYTES:
			return "VARBINARY"
		case schema_pb.ScalarType_STRING:
			return "VARCHAR(255)"
		default:
			return "UNKNOWN"
		}
	case *schema_pb.Type_ListType:
		return "TEXT" // Lists serialized as JSON
	case *schema_pb.Type_RecordType:
		return "TEXT" // Nested records serialized as JSON
	default:
		return "UNKNOWN"
	}
}
