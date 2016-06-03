package topology

import (
	"fmt"
	"testing"
)

func TestLoadConfiguration(t *testing.T) {

	confContent := `

<?xml version="1.0" encoding="UTF-8" ?>
<Configuration>
  <Topology>
    <DataCenter name="dc1">
      <Rack name="rack1">
        <Ip>192.168.1.1</Ip>
      </Rack>
    </DataCenter>
    <DataCenter name="dc2">
      <Rack name="rack1">
        <Ip>192.168.1.2</Ip>
      </Rack>
      <Rack name="rack2">
        <Ip>192.168.1.3</Ip>
        <Ip>192.168.1.4</Ip>
      </Rack>
    </DataCenter>
  </Topology>
</Configuration>
`
	c, err := NewConfiguration([]byte(confContent))

	fmt.Printf("%s\n", c)
	if err != nil {
		t.Fatalf("unmarshal error:%v", err)
	}

	if len(c.Topo.DataCenters) <= 0 || c.Topo.DataCenters[0].Name != "dc1" {
		t.Fatalf("unmarshal error:%s", c)
	}
}
