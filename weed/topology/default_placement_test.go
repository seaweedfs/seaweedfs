package topology

import "testing"

func TestDefaultPlacementStandsOut(t *testing.T) {
	tests := []struct {
		name    string
		dc      string
		rack    string
		dcRacks map[string][]string
		want    bool
	}{
		{"explicit placement", "dc1", "r1", map[string][]string{"dc1": {"r1"}, DefaultDataCenter: {DefaultRack}}, false},
		{"all default", DefaultDataCenter, DefaultRack, map[string][]string{DefaultDataCenter: {DefaultRack}}, false},
		{"default dc next to named dc", DefaultDataCenter, DefaultRack, map[string][]string{"dc1": {"r1", "r2"}, DefaultDataCenter: {DefaultRack}}, true},
		{"default rack next to named rack", "dc1", DefaultRack, map[string][]string{"dc1": {"r1", DefaultRack}}, true},
		{"default rack, racks named only elsewhere", "dc1", DefaultRack, map[string][]string{"dc1": {DefaultRack}, "dc2": {"r1"}}, false},
		{"named dc without racks", "dc1", DefaultRack, map[string][]string{"dc1": {DefaultRack}, "dc2": {DefaultRack}}, false},
	}
	for _, tt := range tests {
		if got := DefaultPlacementStandsOut(tt.dc, tt.rack, tt.dcRacks); got != tt.want {
			t.Errorf("%s: DefaultPlacementStandsOut(%q, %q) = %v, want %v", tt.name, tt.dc, tt.rack, got, tt.want)
		}
	}
}

func TestTopologyDefaultPlacementStandsOut(t *testing.T) {
	topo := NewTopology("weedfs", nil, 32*1024, 5, false)
	topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("r1")
	if topo.DefaultPlacementStandsOut("dc1", "r1") {
		t.Fatalf("named placement reported as default")
	}
	topo.GetOrCreateDataCenter(DefaultDataCenter).GetOrCreateRack(DefaultRack)
	if !topo.DefaultPlacementStandsOut(DefaultDataCenter, DefaultRack) {
		t.Fatalf("default data center next to dc1 not reported")
	}
}
