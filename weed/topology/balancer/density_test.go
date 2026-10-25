package balancer

import (
	"math"
	"sort"
	"testing"
)

func TestVolumeDensity(t *testing.T) {
	const gb = uint64(1) << 30
	// 20 full-ish volumes (~1900 GB) on a disk with a 30 GB limit and Max 1000.
	cap, used := VolumeDensity(1000, 1900*gb, 30*gb)
	if used != 63 { // 1900/30 = 63.33 -> 63
		t.Errorf("usedVolumes = %d, want 63", used)
	}
	if cap != float64(1000-63) {
		t.Errorf("capacity = %v, want %v", cap, float64(1000-63))
	}
	// Unset limit -> 0 used, capacity = maxVolumeCount.
	if c, u := VolumeDensity(10, 1000*gb, 0); u != 0 || c != 10 {
		t.Errorf("unset limit: got cap=%v used=%d, want 10/0", c, u)
	}
}

func TestDensityRatio(t *testing.T) {
	// Empty server: small positive ratio (below any loaded server).
	empty := DensityRatio(1000, 0)
	loaded := DensityRatio(1000, 1)
	if !(empty > 0 && empty < loaded) {
		t.Errorf("empty ratio %v should be >0 and < loaded %v", empty, loaded)
	}
	// Loaded: used/capacity.
	if got := DensityRatio(3, 7); got != 7.0/3.0 {
		t.Errorf("DensityRatio(3,7) = %v, want %v", got, 7.0/3.0)
	}
	// No free capacity (full) or over the slot limit (negative capacity) -> fullest.
	if got := DensityRatio(0, 5); !math.IsInf(got, 1) {
		t.Errorf("DensityRatio(0,5) = %v, want +Inf", got)
	}
	if got := DensityRatio(-4, 20); !math.IsInf(got, 1) {
		t.Errorf("DensityRatio(-4,20) = %v, want +Inf (overfull ranks fullest, not negative)", got)
	}
}

func TestDensityNextRatio(t *testing.T) {
	if got := DensityNextRatio(3, 7); got != 8.0/3.0 {
		t.Errorf("DensityNextRatio(3,7) = %v, want %v", got, 8.0/3.0)
	}
	if got := DensityNextRatio(0, 7); !math.IsInf(got, 1) {
		t.Errorf("DensityNextRatio(0,7) = %v, want +Inf", got)
	}
	if got := DensityNextRatio(-1, 7); !math.IsInf(got, 1) {
		t.Errorf("DensityNextRatio(-1,7) = %v, want +Inf", got)
	}
}

// An overfull server (negative capacity) must sort as the fullest, not the
// emptiest, when consumers rank ascending by DensityRatio.
func TestDensityRatio_OverfullSortsFullest(t *testing.T) {
	// empty (cap 100, used 0), half (cap 5, used 5), overfull (cap -3, used 13).
	type server struct {
		name        string
		capacity    float64
		usedVolumes uint64
	}
	servers := []server{
		{"overfull", -3, 13},
		{"empty", 100, 0},
		{"half", 5, 5},
	}
	sort.Slice(servers, func(i, j int) bool {
		return DensityRatio(servers[i].capacity, servers[i].usedVolumes) <
			DensityRatio(servers[j].capacity, servers[j].usedVolumes)
	})
	if servers[0].name != "empty" {
		t.Errorf("emptiest (ascending first) = %q, want empty", servers[0].name)
	}
	if servers[len(servers)-1].name != "overfull" {
		t.Errorf("fullest (ascending last) = %q, want overfull", servers[len(servers)-1].name)
	}
}
