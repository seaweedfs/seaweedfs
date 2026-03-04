package blockvol

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// InfoMessageToProto converts a Go wire type to proto.
func InfoMessageToProto(m BlockVolumeInfoMessage) *master_pb.BlockVolumeInfoMessage {
	return &master_pb.BlockVolumeInfoMessage{
		Path:          m.Path,
		VolumeSize:    m.VolumeSize,
		BlockSize:     m.BlockSize,
		Epoch:         m.Epoch,
		Role:          m.Role,
		WalHeadLsn:    m.WalHeadLsn,
		CheckpointLsn: m.CheckpointLsn,
		HasLease:      m.HasLease,
		DiskType:      m.DiskType,
	}
}

// InfoMessageFromProto converts a proto to Go wire type.
func InfoMessageFromProto(p *master_pb.BlockVolumeInfoMessage) BlockVolumeInfoMessage {
	if p == nil {
		return BlockVolumeInfoMessage{}
	}
	return BlockVolumeInfoMessage{
		Path:          p.Path,
		VolumeSize:    p.VolumeSize,
		BlockSize:     p.BlockSize,
		Epoch:         p.Epoch,
		Role:          p.Role,
		WalHeadLsn:    p.WalHeadLsn,
		CheckpointLsn: p.CheckpointLsn,
		HasLease:      p.HasLease,
		DiskType:      p.DiskType,
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
	return &master_pb.BlockVolumeAssignment{
		Path:       a.Path,
		Epoch:      a.Epoch,
		Role:       a.Role,
		LeaseTtlMs: a.LeaseTtlMs,
	}
}

// AssignmentFromProto converts a proto assignment to Go wire type.
func AssignmentFromProto(p *master_pb.BlockVolumeAssignment) BlockVolumeAssignment {
	if p == nil {
		return BlockVolumeAssignment{}
	}
	return BlockVolumeAssignment{
		Path:       p.Path,
		Epoch:      p.Epoch,
		Role:       p.Role,
		LeaseTtlMs: p.LeaseTtlMs,
	}
}

// AssignmentsFromProto converts a slice of proto assignments to Go wire types.
func AssignmentsFromProto(protos []*master_pb.BlockVolumeAssignment) []BlockVolumeAssignment {
	out := make([]BlockVolumeAssignment, len(protos))
	for i, p := range protos {
		out[i] = AssignmentFromProto(p)
	}
	return out
}
