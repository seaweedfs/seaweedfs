package util

import (
	"errors"
	"testing"
)

func TestRandomUint64ReturnsFullRandomValue(t *testing.T) {
	useRandomRead(t, func(p []byte) (int, error) {
		copy(p, []byte{0x80, 0, 0, 0, 0, 0, 0, 1})
		return len(p), nil
	})

	const want uint64 = 0x8000000000000001
	if got := RandomUint64(); got != want {
		t.Fatalf("RandomUint64() = %d, want %d", got, want)
	}
}

func TestRandomUint64PanicsOnRandomReadError(t *testing.T) {
	useRandomRead(t, func([]byte) (int, error) {
		return 0, errors.New("random read failed")
	})

	defer func() {
		if recover() == nil {
			t.Fatal("RandomUint64() did not panic on random read error")
		}
	}()

	RandomUint64()
}

func useRandomRead(t *testing.T, read func([]byte) (int, error)) {
	t.Helper()

	original := randomRead
	randomRead = read
	t.Cleanup(func() {
		randomRead = original
	})
}

func TestByteParsing(t *testing.T) {
	tests := []struct {
		in  string
		exp uint64
	}{
		{"42", 42},
		{"42MB", 42000000},
		{"42MiB", 44040192},
		{"42mb", 42000000},
		{"42mib", 44040192},
		{"42MIB", 44040192},
		{"42 MB", 42000000},
		{"42 MiB", 44040192},
		{"42 mb", 42000000},
		{"42 mib", 44040192},
		{"42 MIB", 44040192},
		{"42.5MB", 42500000},
		{"42.5MiB", 44564480},
		{"42.5 MB", 42500000},
		{"42.5 MiB", 44564480},
		// No need to say B
		{"42M", 42000000},
		{"42Mi", 44040192},
		{"42m", 42000000},
		{"42mi", 44040192},
		{"42MI", 44040192},
		{"42 M", 42000000},
		{"42 Mi", 44040192},
		{"42 m", 42000000},
		{"42 mi", 44040192},
		{"42 MI", 44040192},
		{"42.5M", 42500000},
		{"42.5Mi", 44564480},
		{"42.5 M", 42500000},
		{"42.5 Mi", 44564480},
		// Bug #42
		{"1,005.03 MB", 1005030000},
		// Large testing, breaks when too much larger than
		// this.
		{"12.5 EB", uint64(12.5 * float64(EByte))},
		{"12.5 E", uint64(12.5 * float64(EByte))},
		{"12.5 EiB", uint64(12.5 * float64(EiByte))},
	}

	for _, p := range tests {
		got, err := ParseBytes(p.in)
		if err != nil {
			t.Errorf("Couldn't parse %v: %v", p.in, err)
		}
		if got != p.exp {
			t.Errorf("Expected %v for %v, got %v",
				p.exp, p.in, got)
		}
	}
}
