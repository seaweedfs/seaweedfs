// Package balancer holds balancing policy shared by the shell volume.balance
// command and the maintenance balance worker so the two implementations do not
// drift. It is dependency-light and takes plain values, so both the raw
// master_pb world (shell) and the ActiveTopology world (worker) can call it.
package balancer

// DefaultMaxDiskUsagePercent is the default physical-disk high-water mark for
// balancing: a move target whose disk is at or above this used percentage is
// skipped. It guards against an over-configured maxVolumeCount making a
// physically full server look under-utilized by slot count.
const DefaultMaxDiskUsagePercent = 90

// DiskTooFullAfter reports whether landing incomingBytes on a disk would push its
// physical used space to or above maxUsagePercent of totalBytes. It judges the
// disk against its own total, so heterogeneous disk sizes are handled correctly.
//
// It returns false (no opinion) when the gate is disabled (maxUsagePercent <= 0
// or >= 100) or the disk reports no capacity (totalBytes == 0), so callers fall
// back to slot-only behavior for servers that do not report disk bytes.
//
// Pass incomingBytes = 0 to test current fullness, or a volume's worth of bytes
// to test fullness after the move.
func DiskTooFullAfter(totalBytes, freeBytes, incomingBytes uint64, maxUsagePercent int) bool {
	if maxUsagePercent <= 0 || maxUsagePercent >= 100 || totalBytes == 0 {
		return false
	}
	var used uint64
	if totalBytes > freeBytes {
		used = totalBytes - freeBytes
	}
	usedAfter := float64(used) + float64(incomingBytes)
	return usedAfter*100 >= float64(totalBytes)*float64(maxUsagePercent)
}
