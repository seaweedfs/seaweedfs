package s3api

import (
	"net/url"
	"reflect"
	"testing"
)

func TestEscapeFilerPath(t *testing.T) {
	cases := map[string]string{
		"/buckets/b/a/b.txt":    "/buckets/b/a/b.txt",
		"/buckets/b/a b/ключ\n": "/buckets/b/a%20b/%D0%BA%D0%BB%D1%8E%D1%87%0A",
		"/buckets/b/100%+?#":    "/buckets/b/100%25+%3F%23",
	}
	for in, want := range cases {
		got := escapeFilerPath(in)
		if got != want {
			t.Errorf("escapeFilerPath(%q) = %q, want %q", in, got, want)
		}
		back, err := url.PathUnescape(got)
		if err != nil || back != in {
			t.Errorf("url.PathUnescape(%q) = %q, %v; want %q", got, back, err, in)
		}
	}
}

func TestContentLocationPairMap(t *testing.T) {
	if got := contentLocationPairMap(""); got != nil {
		t.Errorf("contentLocationPairMap(\"\") = %v, want nil", got)
	}
	want := map[string]string{"Seaweed-Content-Location": "/buckets/b/k"}
	if got := contentLocationPairMap("/buckets/b/k"); !reflect.DeepEqual(got, want) {
		t.Errorf("contentLocationPairMap(\"/buckets/b/k\") = %v, want %v", got, want)
	}
}
