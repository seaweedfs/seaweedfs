package distsim

type Write struct {
	LSN   uint64
	Block uint64
	Value uint64
}

type Snapshot struct {
	LSN   uint64
	State map[uint64]uint64
}

type Reference struct {
	writes    []Write
	snapshots map[uint64]Snapshot
}

func NewReference() *Reference {
	return &Reference{snapshots: map[uint64]Snapshot{}}
}

func (r *Reference) Apply(w Write) {
	r.writes = append(r.writes, w)
}

func (r *Reference) StateAt(lsn uint64) map[uint64]uint64 {
	state := make(map[uint64]uint64)
	for _, w := range r.writes {
		if w.LSN > lsn {
			break
		}
		state[w.Block] = w.Value
	}
	return state
}

func cloneMap(in map[uint64]uint64) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (r *Reference) TakeSnapshot(lsn uint64) Snapshot {
	s := Snapshot{LSN: lsn, State: cloneMap(r.StateAt(lsn))}
	r.snapshots[lsn] = s
	return s
}

func (r *Reference) SnapshotAt(lsn uint64) (Snapshot, bool) {
	s, ok := r.snapshots[lsn]
	return s, ok
}

type Node struct {
	Extent map[uint64]uint64
}

func NewNode() *Node {
	return &Node{Extent: map[uint64]uint64{}}
}

func (n *Node) ApplyWrite(w Write) {
	n.Extent[w.Block] = w.Value
}

func (n *Node) LoadSnapshot(s Snapshot) {
	n.Extent = cloneMap(s.State)
}

func (n *Node) ReplayFromWrites(writes []Write, startExclusive, endInclusive uint64) {
	for _, w := range writes {
		if w.LSN <= startExclusive {
			continue
		}
		if w.LSN > endInclusive {
			break
		}
		n.ApplyWrite(w)
	}
}

func EqualState(a, b map[uint64]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
