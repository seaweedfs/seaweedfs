package s3api

import (
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/gorilla/mux"
)

func TestObjectContentLocation(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	partReq := mux.SetURLVars(httptest.NewRequest("PUT", "/b/dir/key?partNumber=1&uploadId=u", nil),
		map[string]string{"bucket": "b", "object": "dir/key"})

	if got := s3a.objectContentLocation(partReq, "b", "obj"); got != "/buckets/b/obj" {
		t.Errorf("explicit object: got %q, want %q", got, "/buckets/b/obj")
	}
	if got := s3a.objectContentLocation(partReq, "b", ""); got != "/buckets/b/dir/key" {
		t.Errorf("UploadPart (object from mux vars): got %q, want %q", got, "/buckets/b/dir/key")
	}
	if got := s3a.objectContentLocation(httptest.NewRequest("POST", "/b", nil), "b", ""); got != "" {
		t.Errorf("no object anywhere: got %q, want empty", got)
	}
}

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
