package weed_server

import "testing"

func TestParseRange(t *testing.T) {
	const size = int64(10)

	tests := []struct {
		name      string
		rangeSpec string
		wantErr   bool
		wantStart int64
		wantLen   int64
	}{
		{name: "start within bounds", rangeSpec: "bytes=0-", wantStart: 0, wantLen: 10},
		{name: "start at last valid byte", rangeSpec: "bytes=9-", wantStart: 9, wantLen: 1},
		{name: "start equal to size is not satisfiable", rangeSpec: "bytes=10-", wantErr: true},
		{name: "start past size is not satisfiable", rangeSpec: "bytes=11-", wantErr: true},
		{name: "start with explicit end within bounds", rangeSpec: "bytes=2-5", wantStart: 2, wantLen: 4},
		{name: "end clamped to the last byte", rangeSpec: "bytes=2-100", wantStart: 2, wantLen: 8},
		{name: "suffix range within bounds", rangeSpec: "bytes=-3", wantStart: 7, wantLen: 3},
		{name: "suffix range larger than size is clamped, not rejected", rangeSpec: "bytes=-100", wantStart: 0, wantLen: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges, err := parseRange(tt.rangeSpec, size)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseRange(%q, %d) = %+v, want an error", tt.rangeSpec, size, ranges)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRange(%q, %d) returned unexpected error: %v", tt.rangeSpec, size, err)
			}
			if len(ranges) != 1 {
				t.Fatalf("parseRange(%q, %d) = %d ranges, want 1", tt.rangeSpec, size, len(ranges))
			}
			if ranges[0].start != tt.wantStart || ranges[0].length != tt.wantLen {
				t.Fatalf("parseRange(%q, %d) = {start:%d length:%d}, want {start:%d length:%d}",
					tt.rangeSpec, size, ranges[0].start, ranges[0].length, tt.wantStart, tt.wantLen)
			}
		})
	}
}

func TestParseRangeOnEmptyFile(t *testing.T) {
	// A zero-length file has no valid byte offsets at all, not even 0, so any
	// range request against it must be rejected rather than satisfied with an
	// empty range.
	if _, err := parseRange("bytes=0-", 0); err == nil {
		t.Fatal("parseRange(\"bytes=0-\", 0) = nil error, want an error")
	}
}
