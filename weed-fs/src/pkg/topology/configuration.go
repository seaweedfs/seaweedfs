package topology

import (
  "encoding/xml"
)

type rack struct {
  Name string `xml:"name,attr"`
  Ips []string `xml:"Ip"`
}
type dataCenter struct {
  Name string `xml:"name,attr"`
  Racks []rack `xml:"Rack"`
}
type topology struct {
  DataCenters []dataCenter `xml:"DataCenter"`
}
type configuration struct {
	XMLName xml.Name `xml:"Configuration"`
	Topo topology `xml:"Topology"`
}

func NewConfiguration(b []byte) (*configuration, error){
  c := &configuration{}
  err := xml.Unmarshal(b, c)
  return c, err
}

func (c *configuration) String() string{
  if b, e := xml.MarshalIndent(c, "  ", "  "); e==nil {
    return string(b)
  }
  return ""
}
