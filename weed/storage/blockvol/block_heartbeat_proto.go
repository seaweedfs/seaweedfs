package blockvol

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// InfoMessageToProto converts a Go wire type to proto.
func InfoMessageToProto(m BlockVolumeInfoMessage) *master_pb.BlockVolumeInfoMessage {
	return &master_pb.BlockVolumeInfoMessage{
		Path:            m.Path,
		VolumeSize:      m.VolumeSize,
		BlockSize:       m.BlockSize,
		Epoch:           m.Epoch,
		Role:            m.Role,
		WalHeadLsn:      m.WalHeadLsn,
		CheckpointLsn:   m.CheckpointLsn,
		HasLease:        m.HasLease,
		DiskType:        m.DiskType,
		ReplicaDataAddr: m.ReplicaDataAddr,
		ReplicaCtrlAddr: m.ReplicaCtrlAddr,
		HealthScore:     m.HealthScore,
		ScrubErrors:     m.ScrubErrors,
		LastScrubTime:   m.LastScrubTime,
		ReplicaDegraded: m.ReplicaDegraded,
		DurabilityMode:  m.DurabilityMode,
	}
}

// InfoMessageFromProto converts a proto to Go wire type.
func InfoMessageFromProto(p *master_pb.BlockVolumeInfoMessage) BlockVolumeInfoMessage {
	if p == nil {
		return BlockVolumeInfoMessage{}
	}
	return BlockVolumeInfoMessage{
		Path:            p.Path,
		VolumeSize:      p.VolumeSize,
		BlockSize:       p.BlockSize,
		Epoch:           p.Epoch,
		Role:            p.Role,
		WalHeadLsn:      p.WalHeadLsn,
		CheckpointLsn:   p.CheckpointLsn,
		HasLease:        p.HasLease,
		DiskType:        p.DiskType,
		ReplicaDataAddr: p.ReplicaDataAddr,
		ReplicaCtrlAddr: p.ReplicaCtrlAddr,
		HealthScore:     p.HealthScore,
		ScrubErrors:     p.ScrubErrors,
		LastScrubTime:   p.LastScrubTime,
		ReplicaDegraded: p.ReplicaDegraded,
		DurabilityMode:  p.DurabilityMode,
	}
}

// InfoMessagesToProto converts a slice of Go wire types to proto.
func InfoMessagesToProto(msgs []BlockVolumeInfoMessage) []*master_pb.BlockVolumeInfoMessage {
	out := make([]*master_pb.BlockVolumeInfoMessage, len(msgs))
	for i, m := range msgs {
		out[i] = InfoMessageToProto(m)
	}
	return out
}

// InfoMessagesFromProto converts a slice of proto messages to Go wire types.
func InfoMessagesFromProto(protos []*master_pb.BlockVolumeInfoMessage) []BlockVolumeInfoMessage {
	out := make([]BlockVolumeInfoMessage, len(protos))
	for i, p := range protos {
		out[i] = InfoMessageFromProto(p)
	}
	return out
}

// ShortInfoToProto converts a Go short info to proto.
func ShortInfoToProto(m BlockVolumeShortInfoMessage) *master_pb.BlockVolumeShortInfoMessage {
	return &master_pb.BlockVolumeShortInfoMessage{
		Path:       m.Path,
		VolumeSize: m.VolumeSize,
		BlockSize:  m.BlockSize,
		DiskType:   m.DiskType,
	}
}

// ShortInfoFromProto converts a proto short info to Go wire type.
func ShortInfoFromProto(p *master_pb.BlockVolumeShortInfoMessage) BlockVolumeShortInfoMessage {
	if p == nil {
		return BlockVolumeShortInfoMessage{}
	}
	return BlockVolumeShortInfoMessage{
		Path:       p.Path,
		VolumeSize: p.VolumeSize,
		BlockSize:  p.BlockSize,
		DiskType:   p.DiskType,
	}
}

// AssignmentToProto converts a Go assignment to proto.
func AssignmentToProto(a BlockVolumeAssignment) *master_pb.BlockVolumeAssignment {
	pb := &master_pb.BlockVolumeAssignment{
		Path:            a.Path,
		Epoch:           a.Epoch,
		Role:            a.Role,
		LeaseTtlMs:      a.LeaseTtlMs,
		ReplicaDataAddr: a.ReplicaDataAddr,
		ReplicaCtrlAddr: a.ReplicaCtrlAddr,
		RebuildAddr:     a.RebuildAddr,
	}
	for _, ra := range a.ReplicaAddrs {
		pb.ReplicaAddrs = append(pb.ReplicaAddrs, &master_pb.ReplicaAddrMessage{
			DataAddr: ra.DataAddr,
			CtrlAddr: ra.CtrlAddr,
		})
	}
	return pb
}

// AssignmentFromProto converts a proto assignment to Go wire type.
// Precedence: if ReplicaAddrs is non-empty, use it and ignore scalar fields.
func AssignmentFromProto(p *master_pb.BlockVolumeAssignment) BlockVolumeAssignment {
	if p == nil {
		return BlockVolumeAssignment{}
	}
	a := BlockVolumeAssignment{
		Path:        p.Path,
		Epoch:       p.Epoch,
		Role:        p.Role,
		LeaseTtlMs:  p.LeaseTtlMs,
		RebuildAddr: p.RebuildAddr,
	}
	if len(p.ReplicaAddrs) > 0 {
		// Multi-replica: populate ReplicaAddrs, leave scalar fields empty.
		for _, ra := range p.ReplicaAddrs {
			a.ReplicaAddrs = append(a.ReplicaAddrs, ReplicaAddr{
				DataAddr: ra.DataAddr,
				CtrlAddr: ra.CtrlAddr,
			})
		}
	} else {
		// Backward compat: use scalar fields.
		a.ReplicaDataAddr = p.ReplicaDataAddr
		a.ReplicaCtrlAddr = p.ReplicaCtrlAddr
	}
	return a
}

// AssignmentsToProto converts a slice of Go assignments to proto.
func AssignmentsToProto(as []BlockVolumeAssignment) []*master_pb.BlockVolumeAssignment {
	out := make([]*master_pb.BlockVolumeAssignment, len(as))
	for i, a := range as {
		out[i] = AssignmentToProto(a)
	}
	return out
}

// AssignmentsFromProto converts a slice of proto assignments to Go wire types.
func AssignmentsFromProto(protos []*master_pb.BlockVolumeAssignment) []BlockVolumeAssignment {
	out := make([]BlockVolumeAssignment, len(protos))
	for i, p := range protos {
		out[i] = AssignmentFromProto(p)
	}
	return out
}

