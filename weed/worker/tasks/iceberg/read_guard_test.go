package iceberg

import (
	"errors"
	"strings"
	"testing"
)

func TestGuardReadTurnsAPanicIntoAnError(t *testing.T) {
	v, err := guardRead("data/x.parquet", func() (int, error) {
		var s []int
		return s[3], nil // index out of range
	})
	if v != 0 || err == nil || !strings.Contains(err.Error(), "data/x.parquet") || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("v = %d, err = %v: want a zero value and an error naming the file", v, err)
	}

	want := errors.New("plain error")
	if _, err := guardRead("data/y.parquet", func() (int, error) { return 0, want }); err != want {
		t.Fatalf("err = %v, want the function's own error unchanged", err)
	}
}
