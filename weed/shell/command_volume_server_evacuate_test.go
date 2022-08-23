package shell

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

func TestVolumeServerEvacuate(t *testing.T) {
	c := commandVolumeServerEvacuate{}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	ce := &CommandEnv{}
	ce.locker = exclusive_locks.NewExclusiveLocker(nil, "admin")
	if err := c.evacuateNormalVolumes(ce, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}
