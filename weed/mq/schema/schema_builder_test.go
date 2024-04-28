package schema

import (
	"fmt"
	"testing"
)

func TestSchemaBuilder(t *testing.T) {
	rtb := NewRecordTypeBuilder()
	rtb.SetStringField("ID").
		SetLongField("CreatedAt").
		SetLongField("ModifiedAt").
		SetStringField("User")
	recordType := rtb.Build()
	fmt.Printf("RecordType: %v\n", recordType)

}
