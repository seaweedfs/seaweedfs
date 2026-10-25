package foundationdb

import (
	"strings"
	"testing"
)

func TestFDBValueSizeLimitConstant(t *testing.T) {
	if FDB_VALUE_SIZE_LIMIT != 100*1000 {
		t.Fatalf("FDB_VALUE_SIZE_LIMIT = %d, want 100000", FDB_VALUE_SIZE_LIMIT)
	}
}

func TestErrIfValueTooLarge(t *testing.T) {
	if err := errIfValueTooLarge("/p", make([]byte, FDB_VALUE_SIZE_LIMIT)); err != nil {
		t.Fatalf("100000-byte value should pass, got %v", err)
	}
	err := errIfValueTooLarge("/buckets/b/o", make([]byte, FDB_VALUE_SIZE_LIMIT+1))
	if err == nil {
		t.Fatal("100001-byte value should fail")
	}
	msg := err.Error()
	if !strings.Contains(msg, "value size limit") {
		t.Fatalf("error should name value size limit, got %q", msg)
	}
	if strings.Contains(msg, "transaction size limit") {
		t.Fatalf("error must not mention transaction size limit, got %q", msg)
	}
	if !strings.Contains(msg, "100001") || !strings.Contains(msg, "100000") {
		t.Fatalf("error should include both sizes, got %q", msg)
	}
}
