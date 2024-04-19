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

}
