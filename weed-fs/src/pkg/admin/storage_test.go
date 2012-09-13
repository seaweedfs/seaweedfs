package admin

import (
  "log"
	"pkg/storage"
	"pkg/topology"
	"testing"
)

func TestXYZ(t *testing.T) {
	dn := topology.NewDataNode("server1")
	dn.Ip = "localhost"
	dn.Port = 8080
	vid, _:= storage.NewVolumeId("5")
	out := AllocateVolume(dn,vid,storage.Copy00)
	log.Println(out)
}
