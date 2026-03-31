package lock_manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCluster simulates a cluster of filer nodes with DLMs.
// It wires up ReplicateFn so that replication calls arrive at the
// correct peer's DLM, enabling end-to-end backup testing without gRPC.
type testCluster struct {
	mu    sync.Mutex
	nodes map[pb.ServerAddress]*DistributedLockManager
}

func newTestCluster(hosts ...pb.ServerAddress) *testCluster {
	c := &testCluster{nodes: make(map[pb.ServerAddress]*DistributedLockManager)}
	servers := make([]pb.ServerAddress, len(hosts))
	copy(servers, hosts)

	for _, host := range hosts {
		dlm := NewDistributedLockManager(host)
		dlm.LockRing.SetSnapshot(servers)
		c.nodes[host] = dlm
	}

	// Wire up replication: each node's ReplicateFn calls the backup's DLM directly
	for _, dlm := range c.nodes {
		d := dlm // capture
		d.ReplicateFn = func(server pb.ServerAddress, key string, expiredAtNs int64, token string, owner string, generation int64, seq int64, isUnlock bool) {
			c.mu.Lock()
			target, ok := c.nodes[server]
			c.mu.Unlock()
			if !ok {
				return // server is down
			}
			if isUnlock {
				target.RemoveBackupLockIfSeq(key, seq)
			} else {
				target.InsertBackupLock(key, expiredAtNs, token, owner, generation, seq)
			}
		}
	}

	return c
}

func (c *testCluster) removeNode(host pb.ServerAddress) {
	c.mu.Lock()
	delete(c.nodes, host)
	c.mu.Unlock()

	// Update all remaining nodes' rings
	remaining := c.getServers()
	for _, dlm := range c.getNodes() {
		dlm.LockRing.SetSnapshot(remaining)
	}
}

func (c *testCluster) addNode(host pb.ServerAddress) {
	c.mu.Lock()
	dlm := NewDistributedLockManager(host)
	c.nodes[host] = dlm
	c.mu.Unlock()

	// Wire up replication
	dlm.ReplicateFn = func(server pb.ServerAddress, key string, expiredAtNs int64, token string, owner string, generation int64, seq int64, isUnlock bool) {
		c.mu.Lock()
		target, ok := c.nodes[server]
		c.mu.Unlock()
		if !ok {
			return
		}
		if isUnlock {
			target.RemoveBackupLockIfSeq(key, seq)
		} else {
			target.InsertBackupLock(key, expiredAtNs, token, owner, generation, seq)
		}
	}

	servers := c.getServers()
	for _, n := range c.getNodes() {
		n.LockRing.SetSnapshot(servers)
	}
}

func (c *testCluster) getNodes() map[pb.ServerAddress]*DistributedLockManager {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[pb.ServerAddress]*DistributedLockManager, len(c.nodes))
	for k, v := range c.nodes {
		cp[k] = v
	}
	return cp
}

func (c *testCluster) getServers() []pb.ServerAddress {
	c.mu.Lock()
	defer c.mu.Unlock()
	var servers []pb.ServerAddress
	for s := range c.nodes {
		servers = append(servers, s)
	}
	return servers
}

func (c *testCluster) get(host pb.ServerAddress) *DistributedLockManager {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[host]
}

// acquireLock tries to acquire a lock on the correct primary node.
// It follows redirects (movedTo) like a real client would.
func (c *testCluster) acquireLock(key, owner string, ttl time.Duration) (renewToken string, generation int64, primaryHost pb.ServerAddress, err error) {
	// Try any node first (simulates client connecting to seed filer)
	for _, dlm := range c.getNodes() {
		expiry := time.Now().Add(ttl).UnixNano()
		var movedTo pb.ServerAddress
		var lockErr error
		_, renewToken, generation, movedTo, lockErr = dlm.LockWithTimeout(key, expiry, "", owner)
		if movedTo != "" && movedTo != dlm.Host {
			// Follow redirect
			target := c.get(movedTo)
			if target == nil {
				err = fmt.Errorf("primary %s is down", movedTo)
				return
			}
			_, renewToken, generation, _, lockErr = target.LockWithTimeout(key, expiry, "", owner)
			if lockErr != nil {
				err = lockErr
				return
			}
			primaryHost = movedTo
			// Wait briefly for async replication to complete
			time.Sleep(10 * time.Millisecond)
			return
		}
		if lockErr != nil {
			err = lockErr
			return
		}
		primaryHost = dlm.Host
		time.Sleep(10 * time.Millisecond)
		return
	}
	err = fmt.Errorf("no nodes available")
	return
}

// renewLock renews a lock on the primary node
func (c *testCluster) renewLock(key, owner, token string, ttl time.Duration, primaryHost pb.ServerAddress) (newToken string, generation int64, err error) {
	target := c.get(primaryHost)
	if target == nil {
		err = fmt.Errorf("primary %s is down", primaryHost)
		return
	}
	expiry := time.Now().Add(ttl).UnixNano()
	var movedTo pb.ServerAddress
	var lockErr error
	_, newToken, generation, movedTo, lockErr = target.LockWithTimeout(key, expiry, token, owner)
	if movedTo != "" && movedTo != primaryHost {
		target = c.get(movedTo)
		if target == nil {
			err = fmt.Errorf("new primary %s is down", movedTo)
			return
		}
		_, newToken, generation, _, lockErr = target.LockWithTimeout(key, expiry, "", owner)
	}
	err = lockErr
	time.Sleep(10 * time.Millisecond)
	return
}

// --- Test Cases ---

func TestDLM_PrimaryCrash_BackupPromotes(t *testing.T) {
	// Scenario: Lock is acquired, primary crashes, backup should have the lock
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	key := "test-lock-primary-crash"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, renewToken)

	// Find the backup for this key
	_, backup := cluster.get(primaryHost).LockRing.GetPrimaryAndBackup(key)
	require.NotEmpty(t, backup, "should have a backup server")

	// Verify backup has the lock
	backupDlm := cluster.get(backup)
	backupLock, found := backupDlm.GetLock(key)
	require.True(t, found, "backup should have the lock")
	assert.True(t, backupLock.IsBackup, "lock on backup should be marked as backup")
	assert.Equal(t, renewToken, backupLock.Token, "backup should have the same token")

	// Crash the primary
	cluster.removeNode(primaryHost)

	// Simulate topology change: promote backup locks
	for _, dlm := range cluster.getNodes() {
		locks := dlm.AllLocks()
		for _, lock := range locks {
			newPrimary, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
			if newPrimary == dlm.Host && lock.IsBackup {
				dlm.PromoteLock(lock.Key)
			}
		}
	}

	// The backup should now be the primary
	newPrimary := backupDlm.LockRing.GetPrimary(key)
	assert.Equal(t, backup, newPrimary, "backup should be the new primary")

	// The promoted lock should work — verify it's no longer a backup
	promotedLock, found := backupDlm.GetLock(key)
	require.True(t, found, "lock should still exist after promotion")
	assert.False(t, promotedLock.IsBackup, "lock should be promoted to primary")

	// Client should be able to renew with the same token on the new primary
	newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, backup)
	require.NoError(t, err)
	assert.NotEmpty(t, newToken, "renewal on new primary should succeed")
}

func TestDLM_BackupCrash_PrimaryContinues(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	key := "test-lock-backup-crash"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	_, backup := cluster.get(primaryHost).LockRing.GetPrimaryAndBackup(key)

	// Crash the backup
	cluster.removeNode(backup)

	// Primary should still work — renew the lock
	newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, primaryHost)
	require.NoError(t, err)
	assert.NotEmpty(t, newToken, "primary should continue working after backup crash")

	// Verify primary is still the primary for this key
	newPrimary := cluster.get(primaryHost).LockRing.GetPrimary(key)
	assert.Equal(t, primaryHost, newPrimary)
}

func TestDLM_BothPrimaryAndBackupCrash(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	key := "test-lock-both-crash"
	_, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	_, backup := cluster.get(primaryHost).LockRing.GetPrimaryAndBackup(key)

	// Crash both
	cluster.removeNode(primaryHost)
	cluster.removeNode(backup)

	// The lock is lost — the surviving node should be able to acquire it fresh
	newToken, _, _, err := cluster.acquireLock(key, "owner2", 30*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, newToken, "new owner should acquire lock after both crash")
}

func TestDLM_RollingRestart(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	// Acquire multiple locks
	type lockState struct {
		key, owner, token string
		generation        int64
		primary           pb.ServerAddress
	}
	locks := make([]lockState, 5)
	for i := range locks {
		key := fmt.Sprintf("rolling-lock-%d", i)
		token, gen, primary, err := cluster.acquireLock(key, fmt.Sprintf("owner-%d", i), 30*time.Second)
		require.NoError(t, err)
		locks[i] = lockState{key: key, owner: fmt.Sprintf("owner-%d", i), token: token, generation: gen, primary: primary}
	}

	// Rolling restart: remove and re-add each node one at a time.
	// After removing a node, promote backups and re-replicate to new backups
	// to maintain the invariant that each lock has a backup copy.
	for _, host := range hosts {
		cluster.removeNode(host)

		// Simulate full OnDlmChangeSnapshot: promote backups and re-replicate
		for _, dlm := range cluster.getNodes() {
			for _, lock := range dlm.AllLocks() {
				newPrimary, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
				if newPrimary == dlm.Host && lock.IsBackup {
					dlm.PromoteLock(lock.Key)
				}
			}
			// Re-replicate all primary locks to their new backups
			for _, lock := range dlm.AllLocks() {
				newPrimary, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
				if newPrimary == dlm.Host && !lock.IsBackup {
					dlm.replicateToBackup(lock.Key, lock.ExpiredAtNs, lock.Token, lock.Owner, lock.Generation, lock.Seq, false)
				}
			}
		}

		time.Sleep(10 * time.Millisecond)

		// Re-add the node
		cluster.addNode(host)
		time.Sleep(10 * time.Millisecond)
	}

	// After rolling restart, locks should survive via backup promotion
	survivedCount := 0
	for _, ls := range locks {
		for _, dlm := range cluster.getNodes() {
			lock, found := dlm.GetLock(ls.key)
			if found && !lock.IsBackup {
				survivedCount++
				break
			}
		}
	}
	t.Logf("Locks survived rolling restart: %d / %d", survivedCount, len(locks))
	require.Greater(t, survivedCount, 0, "at least some locks should survive a rolling restart via backup promotion")
}

func TestDLM_GenerationIncrementsOnNewAcquisition(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888"}
	cluster := newTestCluster(hosts...)

	key := "gen-test-lock"

	// Acquire lock — generation should be > 0
	token1, gen1, primary, err := cluster.acquireLock(key, "owner1", 2*time.Second)
	require.NoError(t, err)
	assert.Greater(t, gen1, int64(0))

	// Renew — generation should stay the same
	token2, gen2, err := cluster.renewLock(key, "owner1", token1, 2*time.Second, primary)
	require.NoError(t, err)
	assert.Equal(t, gen1, gen2, "generation should not change on renewal")

	// Let lock expire
	time.Sleep(3 * time.Second)

	// Re-acquire — generation should increment
	_, gen3, _, err := cluster.acquireLock(key, "owner2", 30*time.Second)
	require.NoError(t, err)
	assert.Greater(t, gen3, gen1, "generation should increment on new acquisition")
	_ = token2
}

func TestDLM_ReplicationFailure_PrimaryStillWorks(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	// Break replication by setting a no-op ReplicateFn on all nodes
	for _, dlm := range cluster.getNodes() {
		dlm.ReplicateFn = func(server pb.ServerAddress, key string, expiredAtNs int64, token string, owner string, generation int64, seq int64, isUnlock bool) {
			// Simulate replication failure: do nothing
		}
	}

	key := "repl-fail-lock"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	// Primary should have the lock
	primaryDlm := cluster.get(primaryHost)
	lock, found := primaryDlm.GetLock(key)
	require.True(t, found, "primary should have the lock")
	assert.False(t, lock.IsBackup)

	// Backup should NOT have it (replication failed)
	_, backup := primaryDlm.LockRing.GetPrimaryAndBackup(key)
	backupDlm := cluster.get(backup)
	_, found = backupDlm.GetLock(key)
	assert.False(t, found, "backup should not have the lock when replication fails")

	// Primary should still be able to renew
	newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, primaryHost)
	require.NoError(t, err)
	assert.NotEmpty(t, newToken)
}

func TestDLM_UnlockReplicatesToBackup(t *testing.T) {
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888"}
	cluster := newTestCluster(hosts...)

	key := "unlock-repl-lock"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	_, backup := cluster.get(primaryHost).LockRing.GetPrimaryAndBackup(key)

	// Verify backup has the lock
	_, found := cluster.get(backup).GetLock(key)
	require.True(t, found, "backup should have the lock")

	// Unlock on primary
	primaryDlm := cluster.get(primaryHost)
	movedTo, err := primaryDlm.Unlock(key, renewToken)
	require.NoError(t, err)
	assert.Empty(t, movedTo)

	// Wait for async replication
	time.Sleep(20 * time.Millisecond)

	// Backup should also have removed the lock
	_, found = cluster.get(backup).GetLock(key)
	assert.False(t, found, "backup should remove lock after unlock replication")
}

func TestDLM_TopologyChange_LockSurvivesServerAddition(t *testing.T) {
	// Start with 2 servers, acquire lock, add a 3rd server
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888"}
	cluster := newTestCluster(hosts...)

	key := "topo-add-lock"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	// Add a new server
	cluster.addNode("filer3:8888")
	time.Sleep(20 * time.Millisecond)

	// The lock should still be accessible — either the same primary or on a new one
	// Try to renew on the original primary first
	newPrimary := cluster.get(primaryHost).LockRing.GetPrimary(key)
	if newPrimary == primaryHost {
		// Still on same primary
		newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, primaryHost)
		require.NoError(t, err)
		assert.NotEmpty(t, newToken)
	}
	// If primary changed, the lock may need transfer — that's handled by OnDlmChangeSnapshot
	// which is tested at the server level
}

func TestDLM_ConsistentHashing_MinimalDisruption(t *testing.T) {
	// Verify that removing a server only affects locks on that server
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	// Acquire 50 locks
	type lockInfo struct {
		key, token string
		primary    pb.ServerAddress
	}
	locks := make([]lockInfo, 50)
	for i := range locks {
		key := fmt.Sprintf("min-disrupt-%d", i)
		token, _, primary, err := cluster.acquireLock(key, "owner", 30*time.Second)
		require.NoError(t, err)
		locks[i] = lockInfo{key: key, token: token, primary: primary}
	}

	// Count locks per server before removal
	countBefore := make(map[pb.ServerAddress]int)
	for _, l := range locks {
		countBefore[l.primary]++
	}
	t.Logf("Lock distribution before: %v", countBefore)

	// Remove filer2
	cluster.removeNode("filer2:8888")

	// Count how many locks changed primary
	changed := 0
	for _, l := range locks {
		// Check where the lock should be now
		for _, dlm := range cluster.getNodes() {
			newPrimary := dlm.LockRing.GetPrimary(l.key)
			if newPrimary != l.primary {
				changed++
			}
			break
		}
	}

	// Only locks from filer2 should have changed
	assert.Equal(t, countBefore["filer2:8888"], changed,
		"only locks from removed server should change primary")
}

func TestDLM_NodeDropAndJoin_OwnershipDisruption(t *testing.T) {
	// Scenario: 3 nodes, acquire locks, one drops and a NEW node joins quickly.
	// The new node steals hash ranges from surviving nodes, not just from the
	// departed node. This test measures the disruption.
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	// Acquire many locks
	numLocks := 100
	type lockInfo struct {
		key, token string
		primary    pb.ServerAddress
	}
	locks := make([]lockInfo, numLocks)
	for i := range locks {
		key := fmt.Sprintf("churn-lock-%d", i)
		token, _, primary, err := cluster.acquireLock(key, "owner", 30*time.Second)
		require.NoError(t, err)
		locks[i] = lockInfo{key: key, token: token, primary: primary}
	}

	// Record primary for each lock before the change
	beforePrimary := make(map[string]pb.ServerAddress)
	for _, l := range locks {
		beforePrimary[l.key] = l.primary
	}

	// Drop filer3 and immediately add filer4
	cluster.removeNode("filer3:8888")

	// Promote backups on remaining nodes (simulates OnDlmChangeSnapshot)
	for _, dlm := range cluster.getNodes() {
		for _, lock := range dlm.AllLocks() {
			p, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
			if p == dlm.Host && lock.IsBackup {
				dlm.PromoteLock(lock.Key)
			}
		}
		// Re-replicate primary locks to new backups
		for _, lock := range dlm.AllLocks() {
			p, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
			if p == dlm.Host && !lock.IsBackup {
				dlm.replicateToBackup(lock.Key, lock.ExpiredAtNs, lock.Token, lock.Owner, lock.Generation, lock.Seq, false)
			}
		}
	}
	time.Sleep(10 * time.Millisecond)

	// Now add filer4 (new node, empty)
	cluster.addNode("filer4:8888")
	time.Sleep(10 * time.Millisecond)

	// Simulate OnDlmChangeSnapshot on all nodes after filer4 joins:
	// transfer locks that now belong to filer4
	for host, dlm := range cluster.getNodes() {
		for _, lock := range dlm.AllLocks() {
			p, _ := dlm.LockRing.GetPrimaryAndBackup(lock.Key)
			if p != host && !lock.IsBackup {
				// This lock should move to the new primary
				target := cluster.get(p)
				if target != nil {
					target.InsertLock(lock.Key, lock.ExpiredAtNs, lock.Token, lock.Owner, lock.Generation, lock.Seq)
					dlm.DemoteLock(lock.Key)
				}
			}
		}
	}
	time.Sleep(10 * time.Millisecond)

	// Count disruptions: locks whose primary changed to a node other than filer3's successor
	disruptedFromSurvivors := 0
	disruptedFromDeparted := 0
	movedToFiler4 := 0
	for _, l := range locks {
		// What's the new primary?
		var newPrimary pb.ServerAddress
		for _, dlm := range cluster.getNodes() {
			newPrimary = dlm.LockRing.GetPrimary(l.key)
			break
		}
		oldPrimary := beforePrimary[l.key]
		if newPrimary != oldPrimary {
			if oldPrimary == "filer3:8888" {
				disruptedFromDeparted++
			} else {
				disruptedFromSurvivors++
			}
		}
		if newPrimary == "filer4:8888" {
			movedToFiler4++
		}
	}

	t.Logf("Locks disrupted from departed filer3: %d / %d", disruptedFromDeparted, numLocks)
	t.Logf("Locks disrupted from surviving filer1/filer2: %d / %d", disruptedFromSurvivors, numLocks)
	t.Logf("Locks now on new filer4: %d / %d", movedToFiler4, numLocks)

	// The key concern: filer4 joining disrupts locks on surviving nodes
	// With consistent hashing, new node steals ~1/N of each surviving node's keys
	// Verify that the transfer logic above moved those locks to filer4
	for _, l := range locks {
		var newPrimary pb.ServerAddress
		for _, dlm := range cluster.getNodes() {
			newPrimary = dlm.LockRing.GetPrimary(l.key)
			break
		}
		target := cluster.get(newPrimary)
		require.NotNil(t, target, "primary %s should exist", newPrimary)

		lock, found := target.GetLock(l.key)
		if !found {
			// Lock may have only a backup copy if transfer happened but
			// the lock was on the departed node and wasn't re-replicated.
			// Check all nodes for any copy.
			anyFound := false
			for _, dlm := range cluster.getNodes() {
				if _, f := dlm.GetLock(l.key); f {
					anyFound = true
					break
				}
			}
			if !anyFound {
				t.Errorf("lock %s completely lost (primary should be %s)", l.key, newPrimary)
			}
			continue
		}
		assert.False(t, lock.IsBackup, "lock %s on primary %s should not be a backup", l.key, newPrimary)
	}
}

func TestDLM_RenewalDuringTransferWindow(t *testing.T) {
	// When a new node joins and steals a key range from a surviving node,
	// there's a window between ring update and lock transfer. During this
	// window, a client renewal should still succeed on the old primary
	// (because it still holds the lock locally).
	hosts := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	cluster := newTestCluster(hosts...)

	key := "transfer-window-lock"
	renewToken, _, primaryHost, err := cluster.acquireLock(key, "owner1", 30*time.Second)
	require.NoError(t, err)

	// Add a new node — this may change the primary for this key
	cluster.addNode("filer4:8888")
	time.Sleep(10 * time.Millisecond)

	newPrimary := cluster.get(primaryHost).LockRing.GetPrimary(key)

	if newPrimary != primaryHost {
		// The key moved to filer4 according to the ring, but no transfer has happened.
		// Renewal on the OLD primary should still work because we still hold the lock.
		newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, primaryHost)
		require.NoError(t, err, "renewal on old primary should succeed during transfer window")
		assert.NotEmpty(t, newToken, "should get a new token from old primary")
		t.Logf("Key %s: primary changed from %s to %s, but renewal on old primary succeeded", key, primaryHost, newPrimary)
	} else {
		// Key didn't move — renewal works normally
		newToken, _, err := cluster.renewLock(key, "owner1", renewToken, 30*time.Second, primaryHost)
		require.NoError(t, err)
		assert.NotEmpty(t, newToken)
		t.Logf("Key %s: primary unchanged at %s", key, primaryHost)
	}
}

func TestDLM_StaleReplicationRejected(t *testing.T) {
	// Verify that a stale replication (lower seq) does not overwrite a newer one
	lm := NewLockManager()

	// Insert backup with seq=3
	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "owner1", 1, 3)
	lock, found := lm.GetLock("key1")
	require.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(3), lock.Seq)

	// Try to overwrite with stale seq=2 — should be rejected
	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "owner1", 1, 2)
	lock, found = lm.GetLock("key1")
	require.True(t, found)
	assert.Equal(t, "token-new", lock.Token, "stale replication should be rejected")
	assert.Equal(t, int64(3), lock.Seq)

	// Update with higher seq=4 — should succeed
	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-newer", "owner1", 1, 4)
	lock, found = lm.GetLock("key1")
	require.True(t, found)
	assert.Equal(t, "token-newer", lock.Token, "newer replication should be accepted")
	assert.Equal(t, int64(4), lock.Seq)

	// Stale unlock (seq=2) should not delete the lock
	removed := lm.RemoveBackupLockIfSeq("key1", 2)
	assert.False(t, removed, "stale unlock should be rejected")
	_, found = lm.GetLock("key1")
	assert.True(t, found, "lock should still exist after stale unlock")

	// Valid unlock (seq=5) should delete
	removed = lm.RemoveBackupLockIfSeq("key1", 5)
	assert.True(t, removed, "valid unlock should be accepted")
	_, found = lm.GetLock("key1")
	assert.False(t, found, "lock should be removed after valid unlock")
}
