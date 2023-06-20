package cluster

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"sync"
	"time"
)

const (
	MasterType       = "master"
	VolumeServerType = "volumeServer"
	FilerType        = "filer"
	BrokerType       = "broker"
)

type FilerGroupName string
type DataCenter string
type Rack string

type Leaders struct {
	leaders [3]pb.ServerAddress
}
type ClusterNode struct {
	Address    pb.ServerAddress
	Version    string
	counter    int
	CreatedTs  time.Time
	DataCenter DataCenter
	Rack       Rack
}
type GroupMembers struct {
	members map[pb.ServerAddress]*ClusterNode
}
type ClusterNodeGroups struct {
	groupMembers map[FilerGroupName]*GroupMembers
	sync.RWMutex
}
type Cluster struct {
	filerGroups  *ClusterNodeGroups
	brokerGroups *ClusterNodeGroups
}

func newClusterNodeGroups() *ClusterNodeGroups {
	return &ClusterNodeGroups{
		groupMembers: map[FilerGroupName]*GroupMembers{},
	}
}
func (g *ClusterNodeGroups) getGroupMembers(filerGroup FilerGroupName, createIfNotFound bool) *GroupMembers {
	members, found := g.groupMembers[filerGroup]
	if !found && createIfNotFound {
		members = &GroupMembers{
			members: make(map[pb.ServerAddress]*ClusterNode),
		}
		g.groupMembers[filerGroup] = members
	}
	return members
}

func (m *GroupMembers) addMember(dataCenter DataCenter, rack Rack, address pb.ServerAddress, version string) *ClusterNode {
	if existingNode, found := m.members[address]; found {
		existingNode.counter++
		return nil
	}
	t := &ClusterNode{
		Address:    address,
		Version:    version,
		counter:    1,
		CreatedTs:  time.Now(),
		DataCenter: dataCenter,
		Rack:       rack,
	}
	m.members[address] = t
	return t
}
func (m *GroupMembers) removeMember(address pb.ServerAddress) bool {
	if existingNode, found := m.members[address]; !found {
		return false
	} else {
		existingNode.counter--
		if existingNode.counter <= 0 {
			delete(m.members, address)
			return true
		}
	}
	return false
}

func (m *GroupMembers) GetMembers() (addresses []pb.ServerAddress) {
	for k := range m.members {
		addresses = append(addresses, k)
	}
	return
}

func (g *ClusterNodeGroups) AddClusterNode(filerGroup FilerGroupName, nodeType string, dataCenter DataCenter, rack Rack, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, true)
	if t := m.addMember(dataCenter, rack, address, version); t != nil {
		return buildClusterNodeUpdateMessage(true, filerGroup, nodeType, address)
	}
	return nil
}
func (g *ClusterNodeGroups) RemoveClusterNode(filerGroup FilerGroupName, nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	if m.removeMember(address) {
		return buildClusterNodeUpdateMessage(false, filerGroup, nodeType, address)
	}
	return nil
}
func (g *ClusterNodeGroups) ListClusterNode(filerGroup FilerGroupName) (nodes []*ClusterNode) {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	for _, node := range m.members {
		nodes = append(nodes, node)
	}
	return
}

func NewCluster() *Cluster {
	return &Cluster{
		filerGroups:  newClusterNodeGroups(),
		brokerGroups: newClusterNodeGroups(),
	}
}

func (cluster *Cluster) getGroupMembers(filerGroup FilerGroupName, nodeType string, createIfNotFound bool) *GroupMembers {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.getGroupMembers(filerGroup, createIfNotFound)
	case BrokerType:
		return cluster.brokerGroups.getGroupMembers(filerGroup, createIfNotFound)
	}
	return nil
}

func (cluster *Cluster) AddClusterNode(ns, nodeType string, dataCenter DataCenter, rack Rack, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroupName(ns)
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.AddClusterNode(filerGroup, nodeType, dataCenter, rack, address, version)
	case BrokerType:
		return cluster.brokerGroups.AddClusterNode(filerGroup, nodeType, dataCenter, rack, address, version)
	case MasterType:
		return buildClusterNodeUpdateMessage(true, filerGroup, nodeType, address)
	}
	return nil
}

func (cluster *Cluster) RemoveClusterNode(ns string, nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroupName(ns)
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.RemoveClusterNode(filerGroup, nodeType, address)
	case BrokerType:
		return cluster.brokerGroups.RemoveClusterNode(filerGroup, nodeType, address)
	case MasterType:
		return buildClusterNodeUpdateMessage(false, filerGroup, nodeType, address)
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(filerGroup FilerGroupName, nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.ListClusterNode(filerGroup)
	case BrokerType:
		return cluster.brokerGroups.ListClusterNode(filerGroup)
	case MasterType:
	}
	return
}

func buildClusterNodeUpdateMessage(isAdd bool, filerGroup FilerGroupName, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	result = append(result, &master_pb.KeepConnectedResponse{
		ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
			FilerGroup: string(filerGroup),
			NodeType:   nodeType,
			Address:    string(address),
			IsAdd:      isAdd,
		},
	})
	return
}
