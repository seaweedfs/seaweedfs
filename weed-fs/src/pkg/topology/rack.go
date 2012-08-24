package topology

import (

)

type Rack struct {
  nodes     map[uint64]*Node
  IpRanges   []string
}
