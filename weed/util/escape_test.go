package util

import "testing"

func TestUrlPathEscape(t *testing.T) {
	tests := []struct {
		in    string
		value string
	}{
		{"/data/file", "/data/file"},
		{"/data/test file", "/data/test%20file"},
		{"/data/test file проверка", "/data/test%20file%20%D0%BF%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0"},
	}
	for _, p := range tests {
		got := UrlPathEscape(p.in)
		if got != p.value {
			t.Errorf("failed to test: got %v, want %v", got, p.value)
		}
	}
}
func TestUrlPathUnescape(t *testing.T) {
	tests := []struct {
		in    string
		value string
	}{
		{"/data/file", "/data/file"},
		{"/data/test%20file", "/data/test file"},
		{"/data/test%20file%20%D0%BF%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0", "/data/test file проверка"},
	}
	for _, p := range tests {
		got, err := UrlPathUnescape(p.in)
		if err != nil {
			t.Errorf("got unexcpected error: %s", err.Error())
		}
		if got != p.value {
			t.Errorf("failed to test: got %v, want %v", got, p.value)
		}
	}
}
