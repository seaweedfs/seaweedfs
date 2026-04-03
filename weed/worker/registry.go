package worker

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Registry manages workers and their statistics
type Registry struct {
	workers map[string]*types.WorkerData
	stats   *types.RegistryStats
	mutex   sync.RWMutex
}

// Default global registry instance
var defaultRegistry *Registry
var registryOnce sync.Once

