package csi

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// FormatSnapshotID creates a CSI-standard snapshot ID: "snap-{volumeID}-{snapID}".
func FormatSnapshotID(volumeID string, snapID uint32) string {
	return fmt.Sprintf("snap-%s-%d", volumeID, snapID)
}

// ParseSnapshotID reverses FormatSnapshotID, returning volumeID and snapID.
// The volumeID may contain dashes, so we split on the last dash.
func ParseSnapshotID(csiSnapID string) (string, uint32, error) {
	if !strings.HasPrefix(csiSnapID, "snap-") {
		return "", 0, fmt.Errorf("invalid snapshot ID format: missing snap- prefix")
	}
	rest := csiSnapID[len("snap-"):]

	lastDash := strings.LastIndex(rest, "-")
	if lastDash < 0 || lastDash == 0 {
		return "", 0, fmt.Errorf("invalid snapshot ID format: no volume/snap separator")
	}

	volumeID := rest[:lastDash]
	numStr := rest[lastDash+1:]

	id, err := strconv.ParseUint(numStr, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("invalid snapshot ID format: %w", err)
	}

	return volumeID, uint32(id), nil
}

// snapshotNameToID converts a snapshot name to a deterministic uint32 via FNV-32a hash.
func snapshotNameToID(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}
