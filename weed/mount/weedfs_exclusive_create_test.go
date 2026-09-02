package mount

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func wfsWithFilers(addresses ...pb.ServerAddress) *WFS {
	return &WFS{option: &Option{FilerAddresses: addresses}}
}

func TestOwnerFilerAddressSingle(t *testing.T) {
	wfs := wfsWithFilers("filer1:8888")
	if got := wfs.ownerFilerAddress("/any/path"); got != "filer1:8888" {
		t.Fatalf("single filer must own every path, got %v", got)
	}
}

func TestOwnerFilerAddressOrderIndependent(t *testing.T) {
	orderings := [][]pb.ServerAddress{
		{"filer1:8888", "filer2:8888", "filer3:8888"},
		{"filer3:8888", "filer1:8888", "filer2:8888"},
		{"filer2:8888", "filer3:8888", "filer1:8888"},
	}
	for i := 0; i < 100; i++ {
		path := util.FullPath(fmt.Sprintf("/buckets/b/dir-%d/leaf", i))
		owner := wfsWithFilers(orderings[0]...).ownerFilerAddress(path)
		for _, ordering := range orderings[1:] {
			if got := wfsWithFilers(ordering...).ownerFilerAddress(path); got != owner {
				t.Fatalf("path %s: owner %v with ordering %v, but %v with %v",
					path, owner, orderings[0], got, ordering)
			}
		}
	}
}

// Rendezvous hashing: removing a non-owner filer from the list must not move
// a path's owner, so mounts configured with overlapping filer lists agree
// wherever the winning filer appears in both.
func TestOwnerFilerAddressSubsetAgreement(t *testing.T) {
	full := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	for i := 0; i < 100; i++ {
		path := util.FullPath(fmt.Sprintf("/buckets/b/dir-%d/leaf", i))
		owner := wfsWithFilers(full...).ownerFilerAddress(path)
		var subset []pb.ServerAddress
		for _, addr := range full {
			if addr != owner {
				subset = append(subset, addr)
			}
		}
		subset = append(subset, owner)
		if got := wfsWithFilers(subset[1:]...).ownerFilerAddress(path); got != owner {
			t.Fatalf("path %s: owner %v moved to %v after dropping non-owner %v",
				path, owner, got, subset[0])
		}
	}
}

func TestOwnerFilerAddressSpreadsPaths(t *testing.T) {
	wfs := wfsWithFilers("filer1:8888", "filer2:8888", "filer3:8888")
	counts := map[pb.ServerAddress]int{}
	for i := 0; i < 999; i++ {
		counts[wfs.ownerFilerAddress(util.FullPath(fmt.Sprintf("/d/%d", i)))]++
	}
	for _, addr := range wfs.option.FilerAddresses {
		if counts[addr] == 0 {
			t.Fatalf("filer %v owns no paths at all: %v", addr, counts)
		}
	}
}
