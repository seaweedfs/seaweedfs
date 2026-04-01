package mount

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const (
	// Default quota check interval
	defaultQuotaCheckInterval = 61 * time.Second
	// Faster check interval when approaching quota (within 10%)
	fastQuotaCheckInterval = 5 * time.Second
	// Threshold for switching to fast check (90% of quota)
	quotaWarningThreshold = 0.9
)

// uncommittedBytes tracks bytes written locally but not yet reflected in filer statistics.
// This is used for real-time quota enforcement between periodic checks.
var uncommittedBytes int64

// AddUncommittedBytes adds bytes to the uncommitted write counter.
// Called when data is written to the mount.
func (wfs *WFS) AddUncommittedBytes(bytes int64) {
	if wfs.option.Quota > 0 {
		atomic.AddInt64(&uncommittedBytes, bytes)
	}
}

// SubtractUncommittedBytes subtracts bytes from the uncommitted counter.
// Called when data is flushed to filer or on quota refresh.
func (wfs *WFS) SubtractUncommittedBytes(bytes int64) {
	if wfs.option.Quota > 0 {
		current := atomic.AddInt64(&uncommittedBytes, -bytes)
		// Don't let it go negative
		if current < 0 {
			atomic.StoreInt64(&uncommittedBytes, 0)
		}
	}
}

// ResetUncommittedBytes resets the counter after a quota check syncs with filer.
func (wfs *WFS) ResetUncommittedBytes() {
	atomic.StoreInt64(&uncommittedBytes, 0)
}

// GetUncommittedBytes returns the current uncommitted byte count.
func (wfs *WFS) GetUncommittedBytes() int64 {
	return atomic.LoadInt64(&uncommittedBytes)
}

// IsOverQuotaWithUncommitted checks if quota is exceeded including uncommitted writes.
// This provides real-time quota enforcement between periodic checks.
func (wfs *WFS) IsOverQuotaWithUncommitted() bool {
	if wfs.option.Quota <= 0 {
		return false
	}
	if wfs.IsOverQuota {
		return true
	}
	// Check if uncommitted writes would exceed quota
	uncommitted := atomic.LoadInt64(&uncommittedBytes)
	usedSize := int64(wfs.stats.UsedSize)
	return (usedSize + uncommitted) > wfs.option.Quota
}

func (wfs *WFS) loopCheckQuota() {

	// Skip quota checking if no quota is set
	if wfs.option.Quota <= 0 {
		return
	}

	// Check quota immediately on mount, don't wait for first interval
	wfs.checkQuotaOnce()

	for {
		// Adaptive interval: check more frequently when approaching quota
		interval := wfs.getQuotaCheckInterval()
		time.Sleep(interval)

		if wfs.option.Quota <= 0 {
			continue
		}

		wfs.checkQuotaOnce()
	}
}

// getQuotaCheckInterval returns the check interval based on current usage.
// Returns a shorter interval when approaching quota limit.
func (wfs *WFS) getQuotaCheckInterval() time.Duration {
	if wfs.option.Quota <= 0 {
		return defaultQuotaCheckInterval
	}

	usedSize := int64(wfs.stats.UsedSize)
	uncommitted := atomic.LoadInt64(&uncommittedBytes)
	totalUsed := usedSize + uncommitted

	// If we're at 90% or more of quota, check more frequently
	if float64(totalUsed) >= float64(wfs.option.Quota)*quotaWarningThreshold {
		return fastQuotaCheckInterval
	}
	return defaultQuotaCheckInterval
}

func (wfs *WFS) checkQuotaOnce() {
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.StatisticsRequest{
			Collection:  wfs.option.Collection,
			Replication: wfs.option.Replication,
			Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
			DiskType:    string(wfs.option.DiskType),
		}

		resp, err := client.Statistics(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("reading quota usage %v: %v", request, err)
			return err
		}
		glog.V(4).Infof("read quota usage: %+v", resp)

		// Update the stats cache with latest filer data
		wfs.stats.UsedSize = resp.UsedSize
		wfs.stats.TotalSize = resp.TotalSize

		// Reset uncommitted counter since we now have fresh data from filer
		wfs.ResetUncommittedBytes()

		isOverQuota := int64(resp.UsedSize) > wfs.option.Quota
		if isOverQuota && !wfs.IsOverQuota {
			glog.Warningf("Quota Exceeded! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
		} else if !isOverQuota && wfs.IsOverQuota {
			glog.Warningf("Within quota limit! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
		}
		wfs.IsOverQuota = isOverQuota

		return nil
	})

	if err != nil {
		glog.Warningf("read quota usage: %v", err)
	}
}
