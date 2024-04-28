package schema

import "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"

// RecordValueBuilder helps in constructing RecordValue protobuf messages
type RecordValueBuilder struct {
	recordValue *schema_pb.RecordValue
}

// NewRecordValueBuilder creates a new RecordValueBuilder instance
func NewRecordValueBuilder() *RecordValueBuilder {
	return &RecordValueBuilder{recordValue: &schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}}
}

// Build returns the constructed RecordValue message
func (rvb *RecordValueBuilder) Build() *schema_pb.RecordValue {
	return rvb.recordValue
}

func (rvb *RecordValueBuilder) SetBoolValue(key string, value bool) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetIntValue(key string, value int32) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetLongValue(key string, value int64) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetFloatValue(key string, value float32) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetDoubleValue(key string, value float64) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetBytesValue(key string, value []byte) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetStringValue(key string, value string) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) SetRecordValue(key string, value *RecordValueBuilder) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: value.Build()}}
	return rvb
}

func (rvb *RecordValueBuilder) addListValue(key string, values []*schema_pb.Value) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_ListValue{ListValue: &schema_pb.ListValue{Values: values}}}
	return rvb
}

func (rvb *RecordValueBuilder) SetBoolListValue(key string, values ...bool) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetIntListValue(key string, values ...int32) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetLongListValue(key string, values ...int64) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetFloatListValue(key string, values ...float32) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetDoubleListValue(key string, values ...float64) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetBytesListValue(key string, values ...[]byte) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetStringListValue(key string, values ...string) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) SetRecordListValue(key string, values ...*RecordValueBuilder) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: v.Build()}})
	}
	return rvb.addListValue(key, listValues)
}
