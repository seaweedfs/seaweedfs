package example

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type MyRecord struct {
	Key    []byte
	Field1 []byte
	Field2 string
	Field3 int32
	Field4 int64
	Field5 float32
	Field6 float64
	Field7 bool
}

func MyRecordType() *schema_pb.RecordType {
	return schema.RecordTypeBegin().
		WithField("key", schema.TypeBytes).
		WithField("field1", schema.TypeBytes).
		WithField("field2", schema.TypeString).
		WithField("field3", schema.TypeInt32).
		WithField("field4", schema.TypeInt64).
		WithField("field5", schema.TypeFloat).
		WithField("field6", schema.TypeDouble).
		WithField("field7", schema.TypeBoolean).
		RecordTypeEnd()
}

func (r *MyRecord) ToRecordValue() *schema_pb.RecordValue {
	return schema.RecordBegin().
		SetBytes("key", r.Key).
		SetBytes("field1", r.Field1).
		SetString("field2", r.Field2).
		SetInt32("field3", r.Field3).
		SetInt64("field4", r.Field4).
		SetFloat("field5", r.Field5).
		SetDouble("field6", r.Field6).
		SetBool("field7", r.Field7).
		RecordEnd()
}

func FromRecordValue(recordValue *schema_pb.RecordValue) *MyRecord {
	return &MyRecord{
		Key:    recordValue.Fields["key"].GetBytesValue(),
		Field1: recordValue.Fields["field1"].GetBytesValue(),
		Field2: recordValue.Fields["field2"].GetStringValue(),
		Field3: recordValue.Fields["field3"].GetInt32Value(),
		Field4: recordValue.Fields["field4"].GetInt64Value(),
		Field5: recordValue.Fields["field5"].GetFloatValue(),
		Field6: recordValue.Fields["field6"].GetDoubleValue(),
		Field7: recordValue.Fields["field7"].GetBoolValue(),
	}
}
