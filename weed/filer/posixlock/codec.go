package posixlock

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// The held lock set rides in an inode's entry metadata, so the wire format must
// stay backward compatible across filer versions. It is a protobuf message
// (LockSetProto), so new fields can be added without breaking old readers.

// Marshal encodes the held locks for storage in the inode's entry metadata. A
// lock-free set encodes to nil, so an inode that holds no locks carries no blob.
func (s *Set) Marshal() ([]byte, error) {
	if len(s.locks) == 0 {
		return nil, nil
	}
	pb := &LockSetProto{Locks: make([]*LockProto, len(s.locks))}
	for i, l := range s.locks {
		pb.Locks[i] = &LockProto{
			Start:   l.Start,
			End:     l.End,
			Type:    l.Type,
			Sid:     l.Sid,
			Owner:   l.Owner,
			Pid:     l.Pid,
			IsFlock: l.IsFlock,
		}
	}
	return proto.Marshal(pb)
}

// Unmarshal decodes a Set produced by Marshal. Empty input is an empty set, so a
// missing metadata key reads back as "no locks held". A malformed blob is
// rejected rather than silently misread.
func Unmarshal(b []byte) (*Set, error) {
	if len(b) == 0 {
		return &Set{}, nil
	}
	pb := &LockSetProto{}
	if err := proto.Unmarshal(b, pb); err != nil {
		return nil, fmt.Errorf("posixlock: %w", err)
	}
	locks := make([]Range, len(pb.Locks))
	for i, l := range pb.Locks {
		locks[i] = Range{
			Start:   l.Start,
			End:     l.End,
			Type:    l.Type,
			Sid:     l.Sid,
			Owner:   l.Owner,
			Pid:     l.Pid,
			IsFlock: l.IsFlock,
		}
	}
	return &Set{locks: locks}, nil
}
