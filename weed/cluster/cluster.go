package cluster

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"math"
	"sync"
	"time"
)

const (
	MasterType       = "master"
	VolumeServerType = "volumeServer"
	FilerType        = "filer"
	BrokerType       = "broker"
)

type FilerGroup string
type Filers struct {
	members map[pb.ServerAddress]*ClusterNode
	leaders *Leaders
}
type Leaders struct {
	leaders [3]pb.ServerAddress
}

type DataCenter string
type Rack string
type DataCenterBrokers struct {
	brokers map[Rack]*RackBrokers
}
type RackBrokers struct {
	brokers map[pb.ServerAddress]*ClusterNode
}

type ClusterNode struct {
	Address    pb.ServerAddress
	Version    string
	counter    int
	CreatedTs  time.Time
	DataCenter DataCenter
	Rack       Rack
}

type Cluster struct {
	filerGroup2filers map[FilerGroup]*Filers
	filersLock        sync.RWMutex
	brokers           map[DataCenter]*DataCenterBrokers
	brokersLock       sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		filerGroup2filers: make(map[FilerGroup]*Filers),
		brokers:           make(map[DataCenter]*DataCenterBrokers),
	}
}

func (cluster *Cluster) getFilers(filerGroup FilerGroup, createIfNotFound bool) *Filers {
	filers, found := cluster.filerGroup2filers[filerGroup]
	if !found && createIfNotFound {
		filers = &Filers{
			members: make(map[pb.ServerAddress]*ClusterNode),
			leaders: &Leaders{},
		}
		cluster.filerGroup2filers[filerGroup] = filers
	}
	return filers
}

func (cluster *Cluster) AddClusterNode(ns, nodeType string, dataCenter DataCenter, rack Rack, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroup(ns)
	switch nodeType {
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		filers := cluster.getFilers(filerGroup, true)
		if existingNode, found := filers.members[address]; found {
			existingNode.counter++
			return nil
		}
		filers.members[address] = &ClusterNode{
			Address:    address,
			Version:    version,
			counter:    1,
			CreatedTs:  time.Now(),
			DataCenter: dataCenter,
			Rack:       rack,
		}
		return ensureFilerLeaders(filers, true, filerGroup, nodeType, address)
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()
		existingDataCenterBrokers, foundDataCenter := cluster.brokers[dataCenter]
		if !foundDataCenter {
			existingDataCenterBrokers = &DataCenterBrokers{
				brokers: make(map[Rack]*RackBrokers),
			}
			cluster.brokers[dataCenter] = existingDataCenterBrokers
		}
		existingRackBrokers, foundRack := existingDataCenterBrokers.brokers[rack]
		if !foundRack {
			existingRackBrokers = &RackBrokers{
				brokers: make(map[pb.ServerAddress]*ClusterNode),
			}
			existingDataCenterBrokers.brokers[rack] = existingRackBrokers
		}

		if existingBroker, found := existingRackBrokers.brokers[address]; found {
			existingBroker.counter++
			return nil
		}
		existingRackBrokers.brokers[address] = &ClusterNode{
			Address:    address,
			Version:    version,
			counter:    1,
			CreatedTs:  time.Now(),
			DataCenter: dataCenter,
			Rack:       rack,
		}
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) RemoveClusterNode(ns string, nodeType string, dataCenter DataCenter, rack Rack, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroup(ns)
	switch nodeType {
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		filers := cluster.getFilers(filerGroup, false)
		if filers == nil {
			return nil
		}
		if existingNode, found := filers.members[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(filers.members, address)
				return ensureFilerLeaders(filers, false, filerGroup, nodeType, address)
			}
		}
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()

		existingDataCenterBrokers, foundDataCenter := cluster.brokers[dataCenter]
		if !foundDataCenter {
			return nil
		}
		existingRackBrokers, foundRack := existingDataCenterBrokers.brokers[Rack(rack)]
		if !foundRack {
			return nil
		}

		existingBroker, found := existingRackBrokers.brokers[address]
		if !found {
			return nil
		}
		existingBroker.counter--
		if existingBroker.counter <= 0 {
			delete(existingRackBrokers.brokers, address)
			return []*master_pb.KeepConnectedResponse{
				{
					ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
						NodeType: nodeType,
						Address:  string(address),
						IsAdd:    false,
					},
				},
			}
		}
		return nil

	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    false,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(filerGroup FilerGroup, nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case FilerType:
		cluster.filersLock.RLock()
		defer cluster.filersLock.RUnlock()
		filers := cluster.getFilers(filerGroup, false)
		if filers == nil {
			return
		}
		for _, node := range filers.members {
			nodes = append(nodes, node)
		}
	case BrokerType:
		cluster.brokersLock.RLock()
		defer cluster.brokersLock.RUnlock()
		for _, dcNodes := range cluster.brokers {
			for _, rackNodes := range dcNodes.brokers {
				for _, node := range rackNodes.brokers {
					nodes = append(nodes, node)
				}
			}
		}
	case MasterType:
	}
	return
}

func (cluster *Cluster) IsOneLeader(filerGroup FilerGroup, address pb.ServerAddress) bool {
	filers := cluster.getFilers(filerGroup, false)
	if filers == nil {
		return false
	}
	return filers.leaders.isOneLeader(address)
}

func ensureFilerLeaders(filers *Filers, isAdd bool, filerGroup FilerGroup, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if filers.leaders.addLeaderIfVacant(address) {
			// has added the address as one leader
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      true,
				},
			})
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      true,
				},
			})
		}
	} else {
		if filers.leaders.removeLeaderIfExists(address) {

			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      false,
				},
			})

			// pick the freshest one, since it is less likely to go away
			var shortestDuration int64 = math.MaxInt64
			now := time.Now()
			var candidateAddress pb.ServerAddress
			for _, node := range filers.members {
				if filers.leaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.CreatedTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				filers.leaders.addLeaderIfVacant(candidateAddress)
				// added a new leader
				result = append(result, &master_pb.KeepConnectedResponse{
					ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
						NodeType: nodeType,
						Address:  string(candidateAddress),
						IsLeader: true,
						IsAdd:    true,
					},
				})
			}
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      false,
				},
			})
		}
	}
	return
}

func (leaders *Leaders) addLeaderIfVacant(address pb.ServerAddress) (hasChanged bool) {
	if leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == "" {
			leaders.leaders[i] = address
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) removeLeaderIfExists(address pb.ServerAddress) (hasChanged bool) {
	if !leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			leaders.leaders[i] = ""
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) isOneLeader(address pb.ServerAddress) bool {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			return true
		}
	}
	return false
}
func (leaders *Leaders) GetLeaders() (addresses []pb.ServerAddress) {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] != "" {
			addresses = append(addresses, leaders.leaders[i])
		}
	}
	return
}
