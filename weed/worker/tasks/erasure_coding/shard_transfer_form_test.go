package erasure_coding

import "testing"

func TestShardTransferControlsRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		cap      *int
		parallel bool
		maxText  string
	}{
		{name: "unset", cap: nil, parallel: true, maxText: ""},
		{name: "sequential", cap: intPtr(0), parallel: false, maxText: ""},
		{name: "cap", cap: intPtr(4), parallel: true, maxText: "4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			form := ShardTransferControlsFromCap(tc.cap)
			if form.Parallel != tc.parallel || form.MaxText != tc.maxText {
				t.Fatalf("form = %+v, want parallel=%v max=%q", form, tc.parallel, tc.maxText)
			}
			got := form.Cap()
			if tc.cap == nil {
				if got != nil {
					t.Fatalf("round trip = %d, want unset", *got)
				}
				return
			}
			if got == nil || *got != *tc.cap {
				t.Fatalf("round trip = %v, want %d", got, *tc.cap)
			}
		})
	}
}

func TestShardTransferControlsUncheckedIgnoresMax(t *testing.T) {
	got := (ShardTransferControls{Parallel: false, MaxText: "4"}).Cap()
	if got == nil || *got != 0 {
		t.Fatalf("unchecked with N = %v, want 0", got)
	}
}

func TestShardTransferControlsCheckedEmptyIsUnset(t *testing.T) {
	if got := (ShardTransferControls{Parallel: true, MaxText: "  "}).Cap(); got != nil {
		t.Fatalf("checked empty = %d, want unset", *got)
	}
}
