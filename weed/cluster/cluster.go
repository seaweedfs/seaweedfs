package cluster

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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
	leaders *Leaders
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
			leaders: &Leaders{},
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

func (g *ClusterNodeGroups) AddClusterNode(filerGroup FilerGroupName, nodeType string, dataCenter DataCenter, rack Rack, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, true)
	if t := m.addMember(dataCenter, rack, address, version); t != nil {
		return ensureGroupLeaders(m, true, filerGroup, nodeType, address)
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
		return ensureGroupLeaders(m, false, filerGroup, nodeType, address)
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
func (g *ClusterNodeGroups) IsOneLeader(filerGroup FilerGroupName, address pb.ServerAddress) bool {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return false
	}
	return m.leaders.isOneLeader(address)
}
func (g *ClusterNodeGroups) ListClusterNodeLeaders(filerGroup FilerGroupName) (nodes []pb.ServerAddress) {
	g.Lock()
	defer g.Unlock()
	m := g.getGroupMembers(filerGroup, false)
	if m == nil {
		return nil
	}
	return m.leaders.GetLeaders()
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

func (cluster *Cluster) RemoveClusterNode(ns string, nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroupName(ns)
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.RemoveClusterNode(filerGroup, nodeType, address)
	case BrokerType:
		return cluster.brokerGroups.RemoveClusterNode(filerGroup, nodeType, address)
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

func (cluster *Cluster) ListClusterNodeLeaders(filerGroup FilerGroupName, nodeType string) (nodes []pb.ServerAddress) {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.ListClusterNodeLeaders(filerGroup)
	case BrokerType:
		return cluster.brokerGroups.ListClusterNodeLeaders(filerGroup)
	case MasterType:
	}
	return
}

func (cluster *Cluster) IsOneLeader(filerGroup FilerGroupName, nodeType string, address pb.ServerAddress) bool {
	switch nodeType {
	case FilerType:
		return cluster.filerGroups.IsOneLeader(filerGroup, address)
	case BrokerType:
		return cluster.brokerGroups.IsOneLeader(filerGroup, address)
	case MasterType:
	}
	return false
}

func ensureGroupLeaders(m *GroupMembers, isAdd bool, filerGroup FilerGroupName, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if m.leaders.addLeaderIfVacant(address) {
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
		if m.leaders.removeLeaderIfExists(address) {

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
			for _, node := range m.members {
				if m.leaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.CreatedTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				m.leaders.addLeaderIfVacant(candidateAddress)
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
