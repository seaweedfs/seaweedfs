package pb

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ServerDiscovery encodes a way to find at least 1 instance of a service,
// and provides utility functions to refresh the instance list
type ServerDiscovery struct {
	list      []ServerAddress
	srvRecord *ServerSrvAddress
}

func NewServiceDiscoveryFromMap(m map[string]ServerAddress) (sd *ServerDiscovery) {
	sd = &ServerDiscovery{}
	for _, s := range m {
		sd.list = append(sd.list, s)
	}
	return sd
}

// RefreshBySrvIfAvailable performs a DNS SRV lookup and updates list with the results
// of the lookup
func (sd *ServerDiscovery) RefreshBySrvIfAvailable() {
	if newList := sd.LookupSrvInstances(); newList != nil {
		sd.SetInstances(newList)
	}
}

// LookupSrvInstances resolves the SRV record without touching the stored
// list. It returns nil when there is no SRV record, the lookup fails, or the
// result has no well-formed names. Running it without holding a lock keeps a
// slow resolver from blocking GetInstances callers; callers serialize the
// SetInstances that saves the result.
func (sd *ServerDiscovery) LookupSrvInstances() []ServerAddress {
	if sd.srvRecord == nil {
		return nil
	}
	newList, err := sd.srvRecord.LookUp()
	if err != nil {
		glog.V(0).Infof("failed to lookup SRV for %s: %v", *sd.srvRecord, err)
	}
	if newList == nil || len(newList) == 0 {
		glog.V(0).Infof("looked up SRV for %s, but found no well-formed names", *sd.srvRecord)
		return nil
	}
	return newList
}

// SetInstances saves a resolved address list. Callers racing a refresh must
// serialize this with their GetInstances reads.
func (sd *ServerDiscovery) SetInstances(newList []ServerAddress) {
	if !reflect.DeepEqual(sd.list, newList) {
		sd.list = newList
	}
}

// GetInstances returns a copy of the latest known list of addresses
// call RefreshBySrvIfAvailable prior to this in order to get a more up-to-date view
func (sd *ServerDiscovery) GetInstances() (addresses []ServerAddress) {
	for _, a := range sd.list {
		addresses = append(addresses, a)
	}
	return addresses
}
func (sd *ServerDiscovery) GetInstancesAsStrings() (addresses []string) {
	for _, i := range sd.list {
		addresses = append(addresses, string(i))
	}
	return addresses
}
func (sd *ServerDiscovery) GetInstancesAsMap() (addresses map[string]ServerAddress) {
	addresses = make(map[string]ServerAddress)
	for _, i := range sd.list {
		addresses[string(i)] = i
	}
	return addresses
}
