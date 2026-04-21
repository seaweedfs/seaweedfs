package iceberg

import (
	"testing"
	"unsafe"
)

// skipIfEqualityDeleteAvroUnsupported skips the test when running on a 32-bit
// architecture. The upstream github.com/apache/iceberg-go library declares
// manifestEntry.EqualityIDs as *[]int, but the Iceberg Avro schema declares
// equality_ids as a long array. hamba/avro rejects []int for Avro long when
// int is 32-bit (386, arm, mips), causing these tests to fail through no
// fault of seaweedfs. Tracked upstream; remove this guard once iceberg-go
// switches the field to []int32/[]int64.
func skipIfEqualityDeleteAvroUnsupported(t *testing.T) {
	t.Helper()
	if unsafe.Sizeof(int(0)) < 8 {
		t.Skip("equality-delete Avro decoding is broken on 32-bit platforms " +
			"because github.com/apache/iceberg-go declares EqualityIDs as *[]int " +
			"but the Iceberg schema requires Avro long")
	}
}
