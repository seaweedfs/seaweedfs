package topology

// NewActiveTopology creates a new ActiveTopology instance
func NewActiveTopology(recentTaskWindowSeconds int) *ActiveTopology {
	if recentTaskWindowSeconds <= 0 {
		recentTaskWindowSeconds = 10 // Default 10 seconds
	}

	return &ActiveTopology{
		nodes:                   make(map[string]*activeNode),
		disks:                   make(map[string]*activeDisk),
		volumeIndex:             make(map[uint32][]string),
		ecShardIndex:            make(map[uint32][]string),
		pendingTasks:            make(map[string]*taskState),
		assignedTasks:           make(map[string]*taskState),
		recentTasks:             make(map[string]*taskState),
		recentTaskWindowSeconds: recentTaskWindowSeconds,
	}
}
