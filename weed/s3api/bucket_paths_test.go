package s3api

import "testing"

func TestNormalizeTableLocationMappingPath(t *testing.T) {
	testCases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "legacy table path maps to bucket root",
			raw:  "/buckets/warehouse/analytics/orders",
			want: "/buckets/warehouse",
		},
		{
			name: "already bucket root",
			raw:  "/buckets/warehouse",
			want: "/buckets/warehouse",
		},
		{
			name: "relative buckets path normalized and reduced",
			raw:  "buckets/warehouse/analytics/orders",
			want: "/buckets/warehouse",
		},
		{
			name: "non buckets path preserved",
			raw:  "/tmp/custom/path",
			want: "/tmp/custom/path",
		},
		{
			name: "empty path",
			raw:  "",
			want: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeTableLocationMappingPath(tc.raw); got != tc.want {
				t.Fatalf("normalizeTableLocationMappingPath(%q)=%q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
