package needle

import (
	"bytes"
	"testing"
)

// TestEagerPreGrow is a regression guard for the volume-side amplifier in
// https://github.com/seaweedfs/seaweedfs/issues/6541.
//
// ParseUpload reads the multipart part body via bytes.Buffer.ReadFrom; if the
// receive buffer arrives with cap=0 (sync.Pool dropped the prior buffer),
// ReadFrom doubles capacity on each grow and pays 2-4x the chunk size in
// cumulative allocations. eagerPreGrow shaves the first few rounds off that
// grow chain by pre-sizing from r.ContentLength.
//
// Two policy invariants the cases below pin:
//
//  1. The cap. We never grow more than maxEagerPreGrow per request, so a
//     misreported Content-Length or a slow/idle client cannot force
//     per-request preallocation up to the configured sizeLimit. This is the
//     review concern that landed during PR review and is what changed the
//     test from a TotalAlloc bound to this structural form.
//  2. The validity gates. Negative / zero / oversized Content-Length and
//     buffers that already have enough capacity should not trigger a Grow.
func TestEagerPreGrow(t *testing.T) {
	const sizeLimit = int64(256 * 1024 * 1024)

	cases := []struct {
		name        string
		startCap    int
		cl          int64
		wantMinCap  int
		wantNoop    bool // post: cap stays at startCap
	}{
		{
			name:       "small content-length grows exactly",
			cl:         1 * 1024 * 1024,
			wantMinCap: 1 * 1024 * 1024,
		},
		{
			name:       "content-length at cap grows to cap",
			cl:         maxEagerPreGrow,
			wantMinCap: maxEagerPreGrow,
		},
		{
			name:       "content-length above cap is clamped to cap",
			cl:         200 * 1024 * 1024,
			wantMinCap: maxEagerPreGrow,
		},
		{
			name:     "content-length above sizeLimit is rejected",
			cl:       sizeLimit + 1,
			wantNoop: true,
		},
		{
			name:     "zero content-length is a no-op",
			cl:       0,
			wantNoop: true,
		},
		{
			name:     "negative content-length (chunked encoding) is a no-op",
			cl:       -1,
			wantNoop: true,
		},
		{
			name:       "buffer with sufficient cap is left alone",
			startCap:   maxEagerPreGrow,
			cl:         1 * 1024 * 1024,
			wantMinCap: maxEagerPreGrow,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bb := &bytes.Buffer{}
			if c.startCap > 0 {
				bb.Grow(c.startCap)
			}
			startCap := bb.Cap()

			eagerPreGrow(bb, c.cl, sizeLimit)

			if c.wantNoop {
				if got := bb.Cap(); got != startCap {
					t.Fatalf("eagerPreGrow(cl=%d, sizeLimit=%d) should not have grown: "+
						"cap=%d, want %d (regression: validity gate removed?)",
						c.cl, sizeLimit, got, startCap)
				}
				return
			}
			if got := bb.Cap(); got < c.wantMinCap {
				t.Fatalf("after eagerPreGrow(cl=%d): cap=%d, want >=%d", c.cl, got, c.wantMinCap)
			}
			// Hard upper bound: never above the cap (regardless of cl/sizeLimit).
			// Note bytes.Buffer.Grow may round capacity up modestly, so allow
			// a generous overshoot but still well below sizeLimit.
			if got := int64(bb.Cap()); got > 2*int64(maxEagerPreGrow) {
				t.Fatalf("after eagerPreGrow(cl=%d): cap=%d exceeds 2*maxEagerPreGrow=%d "+
					"(regression: cap removed?)", c.cl, got, 2*maxEagerPreGrow)
			}
		})
	}
}
