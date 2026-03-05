package weed_server

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// VolumeStatus tracks the lifecycle of a block volume entry.
type VolumeStatus int

const (
	StatusPending VolumeStatus = iota // Created via RPC, not yet confirmed by heartbeat
	StatusActive                      // Confirmed by heartbeat from volume server
)

// BlockVolumeEntry tracks one block volume across the cluster.
type BlockVolumeEntry struct {
	Name         string
	VolumeServer string // volume server address (ip:port or grpc addr)
	Path         string // file path on volume server
	IQN          string
	ISCSIAddr    string
	SizeBytes    uint64
	Epoch        uint64
	Role         uint32
	Status       VolumeStatus

	// Replica tracking (CP6-3).
	ReplicaServer     string // replica VS address
	ReplicaPath       string // file path on replica VS
	ReplicaISCSIAddr  string
	ReplicaIQN        string
	ReplicaDataAddr   string // replica receiver data listen addr
	ReplicaCtrlAddr   string // replica receiver ctrl listen addr
	RebuildListenAddr string // rebuild server listen addr on primary

	// Lease tracking for failover (CP6-3 F2).
	LastLeaseGrant time.Time
	LeaseTTL       time.Duration
}

// BlockVolumeRegistry is the in-memory registry of block volumes.
// Rebuilt from heartbeats on master restart (no persistence).
type BlockVolumeRegistry struct {
	mu           sync.RWMutex
	volumes      map[string]*BlockVolumeEntry // keyed by name
	byServer     map[string]map[string]bool   // server -> set of volume names
	blockServers map[string]bool              // servers known to support block volumes

	// inflight guards concurrent CreateBlockVolume for the same name.
	inflight sync.Map // name -> *inflightEntry
}

type inflightEntry struct{}

// NewBlockVolumeRegistry creates an empty registry.
func NewBlockVolumeRegistry() *BlockVolumeRegistry {
	return &BlockVolumeRegistry{
		volumes:      make(map[string]*BlockVolumeEntry),
		byServer:     make(map[string]map[string]bool),
		blockServers: make(map[string]bool),
	}
}

// Register adds an entry to the registry.
// Returns error if a volume with the same name already exists.
func (r *BlockVolumeRegistry) Register(entry *BlockVolumeEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.volumes[entry.Name]; ok {
		return fmt.Errorf("block volume %q already registered", entry.Name)
	}
	r.volumes[entry.Name] = entry
	r.addToServer(entry.VolumeServer, entry.Name)
	return nil
}

// Unregister removes and returns the entry. Returns nil if not found.
func (r *BlockVolumeRegistry) Unregister(name string) *BlockVolumeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return nil
	}
	delete(r.volumes, name)
	r.removeFromServer(entry.VolumeServer, name)
	return entry
}

// Lookup returns the entry for the given name.
func (r *BlockVolumeRegistry) Lookup(name string) (*BlockVolumeEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.volumes[name]
	return e, ok
}

// ListByServer returns all entries hosted on the given server.
func (r *BlockVolumeRegistry) ListByServer(server string) []*BlockVolumeEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names, ok := r.byServer[server]
	if !ok {
		return nil
	}
	entries := make([]*BlockVolumeEntry, 0, len(names))
	for name := range names {
		if e, ok := r.volumes[name]; ok {
			entries = append(entries, e)
		}
	}
	return entries
}

// UpdateFullHeartbeat reconciles the registry from a full heartbeat.
// Called on the first heartbeat from a volume server.
// Marks reported volumes as Active, removes entries for this server
// that are not reported (stale).
func (r *BlockVolumeRegistry) UpdateFullHeartbeat(server string, infos []*master_pb.BlockVolumeInfoMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Mark server as block-capable since it sent block volume info.
	r.blockServers[server] = true

	// Build set of reported paths.
	reported := make(map[string]*master_pb.BlockVolumeInfoMessage, len(infos))
	for _, info := range infos {
		reported[info.Path] = info
	}

	// Find entries for this server that are NOT reported -> remove them.
	if names, ok := r.byServer[server]; ok {
		for name := range names {
			entry := r.volumes[name]
			if entry == nil {
				continue
			}
			if _, found := reported[entry.Path]; !found {
				delete(r.volumes, name)
				delete(names, name)
			}
		}
	}

	// Update or add entries for reported volumes.
	for _, info := range infos {
		// Find existing entry by path on this server.
		var existing *BlockVolumeEntry
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				if e := r.volumes[name]; e != nil && e.Path == info.Path {
					existing = e
					break
				}
			}
		}
		if existing != nil {
			// Update fields from heartbeat.
			existing.SizeBytes = info.VolumeSize
			existing.Epoch = info.Epoch
			existing.Role = info.Role
			existing.Status = StatusActive
			// R1-5: Refresh lease on heartbeat — VS is alive and running this volume.
			existing.LastLeaseGrant = time.Now()
			// F5: update replica addresses from heartbeat info.
			if info.ReplicaDataAddr != "" {
				existing.ReplicaDataAddr = info.ReplicaDataAddr
			}
			if info.ReplicaCtrlAddr != "" {
				existing.ReplicaCtrlAddr = info.ReplicaCtrlAddr
			}
		}
		// If no existing entry found by path, it was created outside master
		// (e.g., manually). We don't auto-register unknown volumes — they
		// must be created via CreateBlockVolume RPC.
	}
}

// UpdateDeltaHeartbeat processes incremental new/deleted block volumes.
// Called on subsequent heartbeats (not the first).
func (r *BlockVolumeRegistry) UpdateDeltaHeartbeat(server string, added []*master_pb.BlockVolumeShortInfoMessage, removed []*master_pb.BlockVolumeShortInfoMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove deleted volumes.
	for _, rm := range removed {
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				if e := r.volumes[name]; e != nil && e.Path == rm.Path {
					delete(r.volumes, name)
					delete(names, name)
					break
				}
			}
		}
	}

	// Mark newly appeared volumes as active (if they exist in registry).
	for _, add := range added {
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				if e := r.volumes[name]; e != nil && e.Path == add.Path {
					e.Status = StatusActive
					break
				}
			}
		}
	}
}

// PickServer returns the server address with the fewest block volumes.
// servers is the list of online volume server addresses.
// Returns error if no servers available.
func (r *BlockVolumeRegistry) PickServer(servers []string) (string, error) {
	if len(servers) == 0 {
		return "", fmt.Errorf("no block volume servers available")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	best := servers[0]
	bestCount := r.countForServer(best)
	for _, s := range servers[1:] {
		c := r.countForServer(s)
		if c < bestCount {
			best = s
			bestCount = c
		}
	}
	return best, nil
}

// AcquireInflight tries to acquire a per-name create lock.
// Returns true if acquired (caller must call ReleaseInflight when done).
// Returns false if another create is already in progress for this name.
func (r *BlockVolumeRegistry) AcquireInflight(name string) bool {
	_, loaded := r.inflight.LoadOrStore(name, &inflightEntry{})
	return !loaded // true = we stored it (acquired), false = already existed
}

// ReleaseInflight releases the per-name create lock.
func (r *BlockVolumeRegistry) ReleaseInflight(name string) {
	r.inflight.Delete(name)
}

// countForServer returns the number of volumes on the given server.
// Caller must hold at least RLock.
func (r *BlockVolumeRegistry) countForServer(server string) int {
	if names, ok := r.byServer[server]; ok {
		return len(names)
	}
	return 0
}

func (r *BlockVolumeRegistry) addToServer(server, name string) {
	if r.byServer[server] == nil {
		r.byServer[server] = make(map[string]bool)
	}
	r.byServer[server][name] = true
}

func (r *BlockVolumeRegistry) removeFromServer(server, name string) {
	if names, ok := r.byServer[server]; ok {
		delete(names, name)
		if len(names) == 0 {
			delete(r.byServer, server)
		}
	}
}

// SetReplica sets replica info for a registered volume.
func (r *BlockVolumeRegistry) SetReplica(name, server, path, iscsiAddr, iqn string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	// Remove old replica from byServer index before replacing.
	if entry.ReplicaServer != "" && entry.ReplicaServer != server {
		r.removeFromServer(entry.ReplicaServer, name)
	}
	entry.ReplicaServer = server
	entry.ReplicaPath = path
	entry.ReplicaISCSIAddr = iscsiAddr
	entry.ReplicaIQN = iqn
	// Also add to byServer index for the replica server.
	r.addToServer(server, name)
	return nil
}

// ClearReplica removes replica info for a registered volume.
func (r *BlockVolumeRegistry) ClearReplica(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	if entry.ReplicaServer != "" {
		r.removeFromServer(entry.ReplicaServer, name)
	}
	entry.ReplicaServer = ""
	entry.ReplicaPath = ""
	entry.ReplicaISCSIAddr = ""
	entry.ReplicaIQN = ""
	entry.ReplicaDataAddr = ""
	entry.ReplicaCtrlAddr = ""
	return nil
}

// SwapPrimaryReplica promotes the replica to primary and clears the old replica.
// The old primary becomes the new replica (if it reconnects, rebuild will handle it).
// Epoch is atomically computed as entry.Epoch+1 inside the lock (R2-F5).
// Returns the new epoch for use in assignment messages.
func (r *BlockVolumeRegistry) SwapPrimaryReplica(name string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return 0, fmt.Errorf("block volume %q not found", name)
	}
	if entry.ReplicaServer == "" {
		return 0, fmt.Errorf("block volume %q has no replica", name)
	}

	// Remove old primary from byServer index.
	r.removeFromServer(entry.VolumeServer, name)

	oldPrimaryServer := entry.VolumeServer
	oldPrimaryPath := entry.Path
	oldPrimaryIQN := entry.IQN
	oldPrimaryISCSI := entry.ISCSIAddr

	// Atomically bump epoch inside lock (R2-F5: prevents race with heartbeat updates).
	newEpoch := entry.Epoch + 1

	// Promote replica to primary.
	entry.VolumeServer = entry.ReplicaServer
	entry.Path = entry.ReplicaPath
	entry.IQN = entry.ReplicaIQN
	entry.ISCSIAddr = entry.ReplicaISCSIAddr
	entry.Epoch = newEpoch
	entry.Role = blockvol.RoleToWire(blockvol.RolePrimary) // R2-F3
	entry.LastLeaseGrant = time.Now()

	// Old primary becomes stale replica (will be rebuilt when it reconnects).
	entry.ReplicaServer = oldPrimaryServer
	entry.ReplicaPath = oldPrimaryPath
	entry.ReplicaIQN = oldPrimaryIQN
	entry.ReplicaISCSIAddr = oldPrimaryISCSI
	entry.ReplicaDataAddr = ""
	entry.ReplicaCtrlAddr = ""

	// Update byServer index: new primary server now hosts this volume.
	r.addToServer(entry.VolumeServer, name)
	return newEpoch, nil
}

// MarkBlockCapable records that the given server supports block volumes.
func (r *BlockVolumeRegistry) MarkBlockCapable(server string) {
	r.mu.Lock()
	r.blockServers[server] = true
	r.mu.Unlock()
}

// UnmarkBlockCapable removes a server from the block-capable set.
func (r *BlockVolumeRegistry) UnmarkBlockCapable(server string) {
	r.mu.Lock()
	delete(r.blockServers, server)
	r.mu.Unlock()
}

// BlockCapableServers returns the list of servers known to support block volumes.
func (r *BlockVolumeRegistry) BlockCapableServers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	servers := make([]string, 0, len(r.blockServers))
	for s := range r.blockServers {
		servers = append(servers, s)
	}
	return servers
}
