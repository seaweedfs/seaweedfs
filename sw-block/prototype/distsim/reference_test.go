package distsim

import "testing"

func TestWALReplayPreservesHistoricalValue(t *testing.T) {
	ref := NewReference()
	ref.Apply(Write{LSN: 10, Block: 7, Value: 10})
	ref.Apply(Write{LSN: 12, Block: 7, Value: 12})

	node := NewNode()
	node.ReplayFromWrites(ref.writes, 0, 10)

	want := ref.StateAt(10)
	if !EqualState(node.Extent, want) {
		t.Fatalf("replay mismatch: got=%v want=%v", node.Extent, want)
	}
}

func TestCurrentExtentCannotRecoverOldLSN(t *testing.T) {
	ref := NewReference()
	ref.Apply(Write{LSN: 10, Block: 7, Value: 10})
	ref.Apply(Write{LSN: 12, Block: 7, Value: 12})

	primary := NewNode()
	for _, w := range ref.writes {
		primary.ApplyWrite(w)
	}

	wantOld := ref.StateAt(10)
	if EqualState(primary.Extent, wantOld) {
		t.Fatalf("latest extent should not equal old LSN state: latest=%v old=%v", primary.Extent, wantOld)
	}
}

func TestSnapshotAtCpLSNRecoversCorrectHistoricalValue(t *testing.T) {
	ref := NewReference()
	ref.Apply(Write{LSN: 10, Block: 7, Value: 10})
	snap := ref.TakeSnapshot(10)
	ref.Apply(Write{LSN: 12, Block: 7, Value: 12})

	node := NewNode()
	node.LoadSnapshot(snap)

	want := ref.StateAt(10)
	if !EqualState(node.Extent, want) {
		t.Fatalf("snapshot mismatch: got=%v want=%v", node.Extent, want)
	}
}

func TestSnapshotPlusTrailingReplayReachesTargetLSN(t *testing.T) {
	ref := NewReference()
	ref.Apply(Write{LSN: 10, Block: 7, Value: 10})
	ref.Apply(Write{LSN: 11, Block: 2, Value: 11})
	snap := ref.TakeSnapshot(11)
	ref.Apply(Write{LSN: 12, Block: 7, Value: 12})
	ref.Apply(Write{LSN: 13, Block: 9, Value: 13})

	node := NewNode()
	node.LoadSnapshot(snap)
	node.ReplayFromWrites(ref.writes, 11, 13)

	want := ref.StateAt(13)
	if !EqualState(node.Extent, want) {
		t.Fatalf("snapshot+replay mismatch: got=%v want=%v", node.Extent, want)
	}
}
