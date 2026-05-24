package posixlock

import (
	"encoding/binary"
	"fmt"
)

// marshalVersion guards the on-disk format of a serialized Set so a later change
// is detected rather than silently misread. The lock set rides in an inode's
// entry metadata, so the format must stay stable across filer versions.
const marshalVersion byte = 1

// recordSize is the fixed encoded width of one Range: Start, End, Sid, Owner
// (8 each), Pid (4), Type (1), IsFlock (1).
const recordSize = 8 + 8 + 8 + 8 + 4 + 1 + 1

// Marshal encodes the held locks for storage in the inode's entry metadata. A
// lock-free set encodes to nil, so an inode that holds no locks carries no blob.
func (s *Set) Marshal() []byte {
	if len(s.locks) == 0 {
		return nil
	}
	b := make([]byte, 5+len(s.locks)*recordSize)
	b[0] = marshalVersion
	binary.BigEndian.PutUint32(b[1:], uint32(len(s.locks)))
	off := 5
	for _, l := range s.locks {
		binary.BigEndian.PutUint64(b[off:], l.Start)
		binary.BigEndian.PutUint64(b[off+8:], l.End)
		binary.BigEndian.PutUint64(b[off+16:], l.Sid)
		binary.BigEndian.PutUint64(b[off+24:], l.Owner)
		binary.BigEndian.PutUint32(b[off+32:], l.Pid)
		b[off+36] = byte(l.Type)
		if l.IsFlock {
			b[off+37] = 1
		}
		off += recordSize
	}
	return b
}

// Unmarshal decodes a Set produced by Marshal. Empty input is an empty set, so a
// missing metadata key reads back as "no locks held".
func Unmarshal(b []byte) (*Set, error) {
	if len(b) == 0 {
		return &Set{}, nil
	}
	if b[0] != marshalVersion {
		return nil, fmt.Errorf("posixlock: unknown format version %d", b[0])
	}
	if len(b) < 5 {
		return nil, fmt.Errorf("posixlock: truncated header (%d bytes)", len(b))
	}
	n := int(binary.BigEndian.Uint32(b[1:]))
	if len(b) != 5+n*recordSize {
		return nil, fmt.Errorf("posixlock: %d bytes does not match %d records", len(b), n)
	}
	locks := make([]Range, n)
	off := 5
	for i := range locks {
		locks[i] = Range{
			Start:   binary.BigEndian.Uint64(b[off:]),
			End:     binary.BigEndian.Uint64(b[off+8:]),
			Sid:     binary.BigEndian.Uint64(b[off+16:]),
			Owner:   binary.BigEndian.Uint64(b[off+24:]),
			Pid:     binary.BigEndian.Uint32(b[off+32:]),
			Type:    uint32(b[off+36]),
			IsFlock: b[off+37] == 1,
		}
		off += recordSize
	}
	return &Set{locks: locks}, nil
}
