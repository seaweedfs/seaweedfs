package topology

import (

)


type IpRange struct {
  inclusives   []string
  exclusives   []string
}

func (r *IpRange) Match(ip string) bool {
// TODO
//  for _, exc := range r.exclusives {
//    if exc
//  }
//  for _, inc := range r.inclusives {
//  }
  return true
}