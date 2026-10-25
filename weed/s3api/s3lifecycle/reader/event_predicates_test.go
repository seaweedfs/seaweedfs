package reader

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

// IsCreate / IsDelete are the small but routing-critical predicates
// the dispatcher uses to decide which match path applies. Both were
// previously exercised only through end-to-end Tick/Match tests; pin
// them directly here.

func TestEventIsCreate_PopulatedNewEntryNoOldEntry(t *testing.T) {
	e := &Event{NewEntry: &filer_pb.Entry{Name: "k"}}
	assert.True(t, e.IsCreate())
	assert.False(t, e.IsDelete())
}

func TestEventIsDelete_PopulatedOldEntryNoNewEntry(t *testing.T) {
	e := &Event{OldEntry: &filer_pb.Entry{Name: "k"}}
	assert.True(t, e.IsDelete())
	assert.False(t, e.IsCreate())
}

func TestEventBothEntries_NeitherCreateNorDelete(t *testing.T) {
	// An update event carries both old and new; the predicates are
	// strict (one or the other, not both) so the router can route
	// updates through the dedicated path rather than treating them
	// as creates or deletes.
	e := &Event{
		OldEntry: &filer_pb.Entry{Name: "k"},
		NewEntry: &filer_pb.Entry{Name: "k"},
	}
	assert.False(t, e.IsCreate(), "update event must not classify as create")
	assert.False(t, e.IsDelete(), "update event must not classify as delete")
}

func TestEventNoEntries_NeitherCreateNorDelete(t *testing.T) {
	// A degenerate event with neither side populated must not classify
	// as either; otherwise a metadata-only filer event with no bucket
	// payload could trigger spurious dispatches.
	e := &Event{}
	assert.False(t, e.IsCreate())
	assert.False(t, e.IsDelete())
}
