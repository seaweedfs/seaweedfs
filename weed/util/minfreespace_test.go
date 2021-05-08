package util

import "testing"

func TestParseMinFreeSpace(t *testing.T) {
	tests := []struct {
		in    string
		ok    bool
		value *MinFreeSpace
	}{
		{in: "42", ok: true, value: &MinFreeSpace{Type: AsPercent, Percent: 42, Raw: "42"}},
		{in: "-1", ok: false, value: nil},
		{in: "101", ok: false, value: nil},
		{in: "100B", ok: false, value: nil},
		{in: "100Ki", ok: true, value: &MinFreeSpace{Type: AsBytes, Bytes: 100 * 1024, Raw: "100Ki"}},
		{in: "100GiB", ok: true, value: &MinFreeSpace{Type: AsBytes, Bytes: 100 * 1024 * 1024 * 1024, Raw: "100GiB"}},
		{in: "42M", ok: true, value: &MinFreeSpace{Type: AsBytes, Bytes: 42 * 1000 * 1000, Raw: "42M"}},
	}

	for _, p := range tests {
		got, err := ParseMinFreeSpace(p.in)
		if p.ok != (err == nil) {
			t.Errorf("failed to test %v", p.in)
		}
		if p.ok && err == nil && *got != *p.value {
			t.Errorf("failed to test %v", p.in)
		}
	}
}
