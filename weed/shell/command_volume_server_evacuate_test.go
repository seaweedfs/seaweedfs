package shell

import (
	"os"
	"testing"
)

func TestEvacuatePrefersLowerVolumeRatio(t *testing.T) {
	volumes := mkByteVolumes(1, 1, 1)
	source := makeByteNode("source", 10, 0, 0, volumes)
	fuller := makeByteNode("fuller", 10, 0, 0, mkByteVolumes(10, 4, 1))
	emptier := makeByteNode("emptier", 10, 0, 0, mkByteVolumes(20, 2, 1))

	// Put the 40%-full server first: truncating 0.2 - 0.4 to int makes the ratios tie.
	moved, err := moveAwayOneNormalVolume(nil, nil, volumes[0], source, []*Node{fuller, emptier}, false)
	if err != nil || !moved {
		t.Fatalf("evacuate: moved=%v, err=%v", moved, err)
	}
	if _, found := emptier.selectedVolumes[volumes[0].Id]; !found {
		t.Fatal("expected evacuation to choose the server with lower occupancy")
	}
}

func TestVolumeServerEvacuate(t *testing.T) {
	targetServer := ""
	volumeRack := "rack1"
	c := commandVolumeServerEvacuate{
		targetServer: &targetServer,
		volumeRack:   &volumeRack,
	}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	if err := c.evacuateNormalVolumes(nil, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}
