package topology

import (
	"testing"
)

func flagNode(ip string) *DataNode {
	dn := NewDataNode(ip)
	dn.Ip, dn.Port = ip, 8080
	return dn
}

func TestLocationFlagsFollowTheirNode(t *testing.T) {
	a, b, c := flagNode("10.0.0.1"), flagNode("10.0.0.2"), flagNode("10.0.0.3")
	list := NewVolumeLocationList()
	for _, dn := range []*DataNode{a, b, c} {
		list.Set(dn)
	}

	list.SetReadOnly(b, true)
	list.SetOversized(c, true)
	if !list.AnyReadOnly() || !list.AnyOversized() {
		t.Fatal("flags did not register")
	}

	// Removing an earlier node shifts the rest down; the flags have to move with
	// them or they end up describing the wrong server.
	list.Remove(a)
	list.SetReadOnly(b, false)
	if list.AnyReadOnly() {
		t.Error("clearing the flag on its node left it set, so it had shifted onto another")
	}
	if !list.AnyOversized() {
		t.Error("removing an unrelated node dropped another node's flag")
	}
	list.SetOversized(c, false)
	if list.AnyOversized() {
		t.Error("clearing the flag on its node left it set")
	}
}

func TestLocationFlagsClearedWithTheirNode(t *testing.T) {
	a, b := flagNode("10.0.0.1"), flagNode("10.0.0.2")
	list := NewVolumeLocationList()
	list.Set(a)
	list.Set(b)
	list.SetReadOnly(a, true)

	list.Remove(a)
	if list.AnyReadOnly() {
		t.Error("a departed node left its read-only marking behind")
	}
}

// A node that shares an address replaces the entry, so it inherits the slot.
func TestLocationFlagsSurviveAReplacedNode(t *testing.T) {
	a, replacement := flagNode("10.0.0.1"), flagNode("10.0.0.1")
	list := NewVolumeLocationList()
	list.Set(a)
	list.SetReadOnly(a, true)

	list.Set(replacement)
	if !list.AnyReadOnly() {
		t.Error("replacing the node at an address dropped what was known about the volume there")
	}
	list.SetReadOnly(replacement, false)
	if list.AnyReadOnly() {
		t.Error("the replacement could not clear the flag it inherited")
	}
}

func TestLocationFlagsIgnoreUntrackableReplicaCounts(t *testing.T) {
	list := NewVolumeLocationList()
	nodes := make([]*DataNode, 0, maxTrackedLocations+2)
	for i := 0; i < maxTrackedLocations+2; i++ {
		dn := NewDataNode("n")
		dn.Ip, dn.Port = "10.0.0.1", 9000+i
		nodes = append(nodes, dn)
		list.Set(dn)
	}

	// Past the tracked width the flag is not recorded rather than landing on
	// some other node's slot.
	list.SetReadOnly(nodes[maxTrackedLocations+1], true)
	if list.AnyReadOnly() {
		t.Error("a flag beyond the tracked width was recorded against another node")
	}
	list.SetReadOnly(nodes[0], true)
	if !list.AnyReadOnly() {
		t.Error("a flag within the tracked width was lost")
	}
}

// Refresh drops stale locations, so it has to rebuild the flags with them.
func TestLocationFlagsRebuiltByRefresh(t *testing.T) {
	stale, fresh, alsoFresh := flagNode("10.0.0.1"), flagNode("10.0.0.2"), flagNode("10.0.0.3")
	stale.LastSeen, fresh.LastSeen, alsoFresh.LastSeen = 100, 500, 500

	list := NewVolumeLocationList()
	for _, dn := range []*DataNode{stale, fresh, alsoFresh} {
		list.Set(dn)
	}
	list.SetReadOnly(alsoFresh, true)

	list.Refresh(400)

	if list.Length() != 2 {
		t.Fatalf("expected the stale location to be dropped, got %d", list.Length())
	}
	list.SetReadOnly(alsoFresh, false)
	if list.AnyReadOnly() {
		t.Error("the flag did not move with its location, so it now describes another server")
	}
}
