package blockvol

import (
	"sync"
)

// MakeDistributedSync creates a sync function that runs local WAL fsync and
// replica barrier in parallel. If no replica is configured or the replica is
// degraded, it falls back to local-only sync (Phase 3 behavior).
//
// walSync: the local fsync function (typically fd.Sync)
// shipper: the WAL shipper to the replica (may be nil)
// vol: the BlockVol (used to read nextLSN and trigger degradation)
func MakeDistributedSync(walSync func() error, shipper *WALShipper, vol *BlockVol) func() error {
	return func() error {
		if shipper == nil || shipper.IsDegraded() {
			return walSync()
		}

		// The highest LSN that needs to be durable is nextLSN-1.
		lsnMax := vol.nextLSN.Load() - 1

		var localErr, remoteErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			localErr = walSync()
		}()
		go func() {
			defer wg.Done()
			remoteErr = shipper.Barrier(lsnMax)
		}()
		wg.Wait()

		if localErr != nil {
			return localErr
		}
		if remoteErr != nil {
			// Local succeeded, replica failed --degrade but don't fail the client.
			vol.degradeReplica(remoteErr)
			return nil
		}
		return nil
	}
}
