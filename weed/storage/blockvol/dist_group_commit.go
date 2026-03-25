package blockvol

import (
	"fmt"
	"sync"
)

// MakeDistributedSync creates a sync function that runs local WAL fsync and
// replica barriers in parallel. Supports N replicas via ShipperGroup.
//
// Durability semantics depend on vol.DurabilityMode():
//   - best_effort: local fsync = ACK; replica failures degrade shippers but never fail writes
//   - sync_all: ALL replica barriers must succeed, else write returns ErrDurabilityBarrierFailed
//   - sync_quorum: quorum (RF/2+1) of nodes must be durable, else ErrDurabilityQuorumLost
func MakeDistributedSync(walSync func() error, group *ShipperGroup, vol *BlockVol) func() error {
	return func() error {
		mode := vol.DurabilityMode()

		if group == nil || group.Len() == 0 {
			// No replicas configured — local sync only.
			return walSync()
		}

		// Note: we always attempt BarrierAll, even when all shippers are
		// Disconnected or Degraded. Barrier() handles bootstrap (Disconnected)
		// and reconnect (Degraded) paths. Only Connecting/CatchingUp/NeedsRebuild
		// are pre-rejected by individual shippers.

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

		// Count barrier failures and degrade shippers.
		failCount := 0
		for _, err := range barrierErrs {
			if err != nil {
				failCount++
				vol.degradeReplica(err)
			}
		}

		switch mode {
		case DurabilitySyncAll:
			if failCount > 0 {
				if vol.Metrics != nil {
					vol.Metrics.DurabilityBarrierFailedTotal.Add(1)
				}
				return fmt.Errorf("%w: %d of %d barriers failed",
					ErrDurabilityBarrierFailed, failCount, len(barrierErrs))
			}
		case DurabilitySyncQuorum:
			rf := group.Len() + 1 // total nodes including primary
			quorum := rf/2 + 1
			durableNodes := 1 + (len(barrierErrs) - failCount) // primary + successful barriers
			if durableNodes < quorum {
				if vol.Metrics != nil {
					vol.Metrics.DurabilityQuorumLostTotal.Add(1)
				}
				return fmt.Errorf("%w: %d durable of %d needed",
					ErrDurabilityQuorumLost, durableNodes, quorum)
			}
		}
		// best_effort: barrier failures already logged via degradeReplica, return nil.
		return nil
	}
}
