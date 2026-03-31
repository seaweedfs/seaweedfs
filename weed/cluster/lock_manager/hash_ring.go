package lock_manager

import (
	"hash/crc32"
	"sort"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

const DefaultVnodeCount = 50

// HashRing implements consistent hashing with virtual nodes.
// When a server is removed, only the keys that hashed to that server
// are remapped (to the next server on the ring), leaving all other
// key-to-server mappings stable.
type HashRing struct {
	mu            sync.RWMutex
	vnodeCount    int
	sortedHashes  []uint32                     // sorted ring positions
	vnodeToServer map[uint32]pb.ServerAddress   // ring position → server
	servers       map[pb.ServerAddress]struct{} // set of all servers
}

func NewHashRing(vnodeCount int) *HashRing {
	if vnodeCount <= 0 {
		vnodeCount = DefaultVnodeCount
	}
	return &HashRing{
		vnodeCount:    vnodeCount,
		vnodeToServer: make(map[uint32]pb.ServerAddress),
		servers:       make(map[pb.ServerAddress]struct{}),
	}
}

// AddServer adds a server with virtual nodes to the ring.
func (hr *HashRing) AddServer(server pb.ServerAddress) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.servers[server]; exists {
		return
	}
	hr.servers[server] = struct{}{}
	hr.rebuildRing()
}

// RemoveServer removes a server and its virtual nodes from the ring.
func (hr *HashRing) RemoveServer(server pb.ServerAddress) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.servers[server]; !exists {
		return
	}
	delete(hr.servers, server)
	hr.rebuildRing()
}

// SetServers replaces the entire server set.
func (hr *HashRing) SetServers(servers []pb.ServerAddress) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.servers = make(map[pb.ServerAddress]struct{}, len(servers))
	for _, s := range servers {
		hr.servers[s] = struct{}{}
	}
	hr.rebuildRing()
}

// GetPrimaryAndBackup returns the primary server for a key and its backup
// (the next distinct server clockwise on the ring).
// If there is only one server, backup is empty.
func (hr *HashRing) GetPrimaryAndBackup(key string) (primary, backup pb.ServerAddress) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return "", ""
	}

	hash := hashKey(key)
	idx := hr.search(hash)
	primary = hr.vnodeToServer[hr.sortedHashes[idx]]

	// Walk clockwise to find a different server for backup
	ringLen := len(hr.sortedHashes)
	for i := 1; i < ringLen; i++ {
		candidate := hr.vnodeToServer[hr.sortedHashes[(idx+i)%ringLen]]
		if candidate != primary {
			backup = candidate
			return
		}
	}
	// Only one server — no backup
	return primary, ""
}

// GetPrimary returns just the primary server for a key.
func (hr *HashRing) GetPrimary(key string) pb.ServerAddress {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return ""
	}

	hash := hashKey(key)
	idx := hr.search(hash)
	return hr.vnodeToServer[hr.sortedHashes[idx]]
}

// GetServers returns a sorted copy of all servers in the ring.
func (hr *HashRing) GetServers() []pb.ServerAddress {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	servers := make([]pb.ServerAddress, 0, len(hr.servers))
	for s := range hr.servers {
		servers = append(servers, s)
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})
	return servers
}

// ServerCount returns the number of servers in the ring.
func (hr *HashRing) ServerCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.servers)
}

// rebuildRing rebuilds the sorted hash ring from the current server set.
// Caller must hold hr.mu write lock.
func (hr *HashRing) rebuildRing() {
	hr.vnodeToServer = make(map[uint32]pb.ServerAddress, len(hr.servers)*hr.vnodeCount)
	hr.sortedHashes = make([]uint32, 0, len(hr.servers)*hr.vnodeCount)

	for server := range hr.servers {
		for i := 0; i < hr.vnodeCount; i++ {
			vnodeKey := vnodeKeyFor(server, i)
			hash := hashKey(vnodeKey)
			hr.vnodeToServer[hash] = server
			hr.sortedHashes = append(hr.sortedHashes, hash)
		}
	}
	sort.Slice(hr.sortedHashes, func(i, j int) bool {
		return hr.sortedHashes[i] < hr.sortedHashes[j]
	})
}

// search finds the first ring position >= hash.
func (hr *HashRing) search(hash uint32) int {
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= hash
	})
	if idx >= len(hr.sortedHashes) {
		idx = 0 // wrap around
	}
	return idx
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func vnodeKeyFor(server pb.ServerAddress, index int) string {
	// Use a format that distributes well across the ring
	buf := make([]byte, 0, len(server)+10)
	buf = append(buf, []byte(server)...)
	buf = append(buf, '#')
	buf = appendInt(buf, index)
	return string(buf)
}

func appendInt(buf []byte, n int) []byte {
	if n == 0 {
		return append(buf, '0')
	}
	// Simple int-to-string without importing strconv
	digits := [20]byte{}
	pos := len(digits)
	for n > 0 {
		pos--
		digits[pos] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, digits[pos:]...)
}
