package weed_server

import (
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func (fs *FilerServer) loopUpdateMetric() {

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		inFlightDataSize := atomic.LoadInt64(&fs.inFlightDataSize)
		stats.FilerInFlightDataSizeGauge.Set(float64(inFlightDataSize))
	}

}
