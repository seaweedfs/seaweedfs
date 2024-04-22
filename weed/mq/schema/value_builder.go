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

func (rvb *RecordValueBuilder) AddBoolValue(key string, value bool) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddIntValue(key string, value int32) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddLongValue(key string, value int64) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddFloatValue(key string, value float32) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddDoubleValue(key string, value float64) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddBytesValue(key string, value []byte) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddStringValue(key string, value string) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: value}}
	return rvb
}
func (rvb *RecordValueBuilder) AddRecordValue(key string, value *RecordValueBuilder) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: value.Build()}}
	return rvb
}

func (rvb *RecordValueBuilder) addListValue(key string, values []*schema_pb.Value) *RecordValueBuilder {
	rvb.recordValue.Fields[key] = &schema_pb.Value{Kind: &schema_pb.Value_ListValue{ListValue: &schema_pb.ListValue{Values: values}}}
	return rvb
}

func (rvb *RecordValueBuilder) AddBoolListValue(key string, values ...bool) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddIntListValue(key string, values ...int32) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddLongListValue(key string, values ...int64) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddFloatListValue(key string, values ...float32) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddDoubleListValue(key string, values ...float64) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddBytesListValue(key string, values ...[]byte) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddStringListValue(key string, values ...string) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v}})
	}
	return rvb.addListValue(key, listValues)
}
func (rvb *RecordValueBuilder) AddRecordListValue(key string, values ...*RecordValueBuilder) *RecordValueBuilder {
	var listValues []*schema_pb.Value
	for _, v := range values {
		listValues = append(listValues, &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: v.Build()}})
	}
	return rvb.addListValue(key, listValues)
}
