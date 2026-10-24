package types

import "testing"

// The disk IO probe keys its per-disk-type settings by ReadableString, so a
// disk configured as "hdd" has to come back out as "hdd" and not as the empty
// string HardDriveType is stored as.
func TestReadableStringRoundTrip(t *testing.T) {
	for configured, want := range map[string]string{
		"":          HddType,
		"hdd":       HddType,
		"HDD":       HddType,
		"ssd":       SsdType,
		"nvme":      NvmeType,
		"nvme-gen5": "nvme-gen5",
	} {
		if got := ToDiskType(configured).ReadableString(); got != want {
			t.Errorf("-disk %q: got %q, want %q", configured, got, want)
		}
	}
}
