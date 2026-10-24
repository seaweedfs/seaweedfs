package s3api

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

// S3 clients such as minio-java parse LastModified with a fixed-width
// "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" pattern, so trailing zeros in the
// fractional seconds must not be trimmed.
func TestXMLTimestampsHaveFixedMilliseconds(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{time.Date(2026, 9, 29, 20, 30, 4, 560_000_000, time.UTC), "2026-09-29T20:30:04.560Z"},
		{time.Date(2026, 9, 29, 20, 30, 4, 500_000_000, time.UTC), "2026-09-29T20:30:04.500Z"},
		{time.Date(2026, 9, 29, 20, 30, 4, 0, time.UTC), "2026-09-29T20:30:04.000Z"},
		{time.Date(2026, 9, 29, 20, 30, 4, 123_456_789, time.UTC), "2026-09-29T20:30:04.123Z"},
		{time.Date(2026, 9, 29, 14, 30, 4, 560_000_000, time.FixedZone("MDT", -6*3600)), "2026-09-29T20:30:04.560Z"},
	}
	for _, c := range cases {
		for name, v := range map[string]any{
			// by value, the way the handlers pass them to writeSuccessResponseXML
			"CopyObjectResult":       CopyObjectResult{ETag: "e", LastModified: c.in},
			"CopyPartResult":         CopyPartResult{ETag: "e", LastModified: c.in},
			"CopyObjectResult (ptr)": &CopyObjectResult{ETag: "e", LastModified: c.in},
			"CopyPartResult (ptr)":   &CopyPartResult{ETag: "e", LastModified: c.in},
		} {
			out, err := xml.Marshal(v)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if !strings.Contains(string(out), "<LastModified>"+c.want+"</LastModified>") {
				t.Errorf("%s(%v): got %s, want LastModified %s", name, c.in, out, c.want)
			}
		}
	}
}

func TestXMLTimestampRoundTrip(t *testing.T) {
	in := CopyObjectResult{ETag: "e", LastModified: time.Date(2026, 9, 29, 20, 30, 4, 560_000_000, time.UTC)}
	out, err := xml.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	var back CopyObjectResult
	if err := xml.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if !back.LastModified.Equal(in.LastModified) {
		t.Fatalf("round trip: got %v, want %v", back.LastModified, in.LastModified)
	}
}
