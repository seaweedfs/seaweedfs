package storage

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// A volume server heartbeats every VolumePulsePeriod whether or not anything
// moved, so this is what a server holding this many volumes allocates just to
// stay connected.
func benchCollectHeartbeat(b *testing.B, count int) {
	store := newTestStore(b, 1)
	location := store.Locations[0]
	for i := 1; i <= count; i++ {
		mountTestVolume(b, location, needle.VolumeId(i))
	}
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store.CollectHeartbeat()
	}
}

func BenchmarkCollectHeartbeat(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("%dVolumes", count), func(b *testing.B) {
			benchCollectHeartbeat(b, count)
		})
	}
}
