package topology

import (
	"encoding/xml"
)

const (
	DefaultDataCenter = "DefaultDataCenter"
	DefaultRack       = "DefaultRack"
)

type rack struct {
	Name string   `xml:"name,attr"`
	Ips  []string `xml:"Ip"`
}
type dataCenter struct {
	Name  string `xml:"name,attr"`
	Racks []rack `xml:"Rack"`
}
type topology struct {
	DataCenters []dataCenter `xml:"DataCenter"`
}
type Configuration struct {
	XMLName xml.Name `xml:"Configuration"`
	Topo    topology `xml:"Topology"`
}

func (c *Configuration) String() string {
	if b, e := xml.MarshalIndent(c, "  ", "  "); e == nil {
		return string(b)
	}
	return ""
}

func (c *Configuration) Locate(ip string, dcName string, rackName string) (dc string, rack string) {
	if dcName == "" {
		dcName = DefaultDataCenter
	}

	if rackName == "" {
		rackName = DefaultRack
	}

	return dcName, rackName
}

// DefaultPlacementStandsOut reports whether a node in dcName/rackName fell back
// to the default data center or rack while other nodes name theirs: the default
// data center next to named ones, or the default rack next to named racks of
// the same data center.
func DefaultPlacementStandsOut(dcName, rackName string, dcRacks map[string][]string) bool {
	if dcName == DefaultDataCenter {
		for dc := range dcRacks {
			if dc != DefaultDataCenter {
				return true
			}
		}
	}
	if rackName == DefaultRack {
		for _, r := range dcRacks[dcName] {
			if r != DefaultRack {
				return true
			}
		}
	}
	return false
}
