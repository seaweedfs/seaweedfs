package erasure_coding

import (
	"strconv"
	"strings"
)

// ShardTransferControls is the maintenance-form presentation of max_shard_transfers.
// Parallel is not persisted. The stored value stays one optional cap:
// unchecked → 0, checked with an empty max → unset, checked with N → N.
type ShardTransferControls struct {
	Parallel bool
	MaxText  string
}

// ShardTransferControlsFromCap maps a persisted cap onto the checkbox and the N box.
func ShardTransferControlsFromCap(cap *int) ShardTransferControls {
	if cap == nil {
		return ShardTransferControls{Parallel: true}
	}
	if *cap <= 0 {
		return ShardTransferControls{Parallel: false}
	}
	return ShardTransferControls{Parallel: true, MaxText: strconv.Itoa(*cap)}
}

// Cap maps the form back to the persisted pointer. A number left in the box
// while the checkbox is off is ignored.
func (c ShardTransferControls) Cap() *int {
	if !c.Parallel {
		zero := 0
		return &zero
	}
	text := strings.TrimSpace(c.MaxText)
	if text == "" {
		return nil
	}
	n, err := strconv.Atoi(text)
	if err != nil || n <= 0 {
		zero := 0
		return &zero
	}
	return &n
}
