package schema

import (
	"fmt"
	"testing"
)

func TestSchemaBuilder(t *testing.T) {
	rtb := NewRecordTypeBuilder()
	rtb.AddStringField("ID").
		AddLongField("CreatedAt").
		AddLongField("ModifiedAt").
		AddStringField("User")
	recordType := rtb.Build()
	fmt.Printf("RecordType: %v\n", recordType)

	recordType2 := NewRecordTypeBuilder().
		AddLongField("ID").
		AddLongField("CreatedAt").
		AddRecordField("Person", NewRecordTypeBuilder().
			AddStringField("Name").
			AddListField("emails", TypeString)).Build()

	fmt.Printf("RecordType2: %v\n", recordType2)

}
