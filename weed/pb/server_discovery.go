package pb

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ServerDiscovery encodes a way to find at least 1 instance of a service,
// and provides utility functions to refresh the instance list
type ServerDiscovery struct {
	list      []ServerAddress
	srvRecord *ServerSrvAddress
}

// RefreshBySrvIfAvailable performs a DNS SRV lookup and updates list with the results
// of the lookup
func (sd *ServerDiscovery) RefreshBySrvIfAvailable() {
	if sd.srvRecord == nil {
		return
	}
	newList, err := sd.srvRecord.LookUp()
	if err != nil {
		glog.V(0).Infof("failed to lookup SRV for %s: %v", *sd.srvRecord, err)
	}
	if newList == nil || len(newList) == 0 {
		glog.V(0).Infof("looked up SRV for %s, but found no well-formed names", *sd.srvRecord)
		return
	}
	sd.list = newList
}

// GetInstances returns a copy of the latest known list of addresses
// call RefreshBySrvIfAvailable prior to this in order to get a more up-to-date view
func (sd *ServerDiscovery) GetInstances() (addresses []ServerAddress) {
	for _, a := range sd.list {
		addresses = append(addresses, a)
	}
	return addresses
}
