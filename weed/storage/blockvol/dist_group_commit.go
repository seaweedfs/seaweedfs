package blockvol

import (
	"sync"
)

// MakeDistributedSync creates a sync function that runs local WAL fsync and
// replica barriers in parallel. Supports N replicas via ShipperGroup.
//
// If no replicas are configured (group is nil or empty) or all replicas are
// degraded, it falls back to local-only sync.
//
// Local fsync is the durability point. Replica barrier failures degrade
// individual shippers but never fail the write.
func MakeDistributedSync(walSync func() error, group *ShipperGroup, vol *BlockVol) func() error {
	return func() error {
		if group == nil || group.Len() == 0 || group.AllDegraded() {
			return walSync()
		}

		// The highest LSN that needs to be durable is nextLSN-1.
		lsnMax := vol.nextLSN.Load() - 1

		var localErr error
		var barrierErrs []error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			localErr = walSync()
		}()
		go func() {
			defer wg.Done()
			barrierErrs = group.BarrierAll(lsnMax)
		}()
		wg.Wait()

		if localErr != nil {
			return localErr
		}
		// Remote failures: degrade individual shippers (already done by Barrier).
		for _, err := range barrierErrs {
			if err != nil {
				vol.degradeReplica(err)
			}
		}
		return nil
	}
}
