package topology

import (
	_ "fmt"
	"strconv"
	"testing"
)

func TestXYZ(t *testing.T) {
	topo, err := NewTopology("topo", "/etc/weed.conf", "/tmp", "test", 234, 5)
	if err != nil {
		t.Error("cannot create new topology:", err)
		t.FailNow()
	}
	for i := 0; i < 5; i++ {
		dc := NewDataCenter("dc" + strconv.Itoa(i))
		dc.activeVolumeCount = i
		dc.maxVolumeCount = 5
		topo.LinkChildNode(dc)
	}
	nl := NewNodeList(topo.Children(), nil)

	picked, ret := nl.RandomlyPickN(1, 0, "")
	if !ret || len(picked) != 1 {
		t.Error("need to randomly pick 1 node")
	}

	picked, ret = nl.RandomlyPickN(1, 0, "dc1")
	if !ret || len(picked) != 1 {
		t.Error("need to randomly pick 1 node")
	}
	if picked[0].Id() != "dc1" {
		t.Error("need to randomly pick 1 dc1 node")
	}

	picked, ret = nl.RandomlyPickN(2, 0, "dc1")
	if !ret || len(picked) != 2 {
		t.Error("need to randomly pick 1 node")
	}
	if picked[0].Id() != "dc1" {
		t.Error("need to randomly pick 2 with one dc1 node")
	}

	picked, ret = nl.RandomlyPickN(4, 0, "")
	if !ret || len(picked) != 4 {
		t.Error("need to randomly pick 4 nodes")
	}

	picked, ret = nl.RandomlyPickN(5, 0, "")
	if !ret || len(picked) != 5 {
		t.Error("need to randomly pick 5 nodes")
	}

	picked, ret = nl.RandomlyPickN(6, 0, "")
	if ret || len(picked) != 0 {
		t.Error("can not randomly pick 6 nodes:", ret, picked)
	}

}
