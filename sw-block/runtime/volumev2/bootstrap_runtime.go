package volumev2

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BootstrapPrimaryVolume creates or opens one named volume on one selected node
// and applies a bounded primary assignment through the runtime-owned path.
func (m *InProcessRuntimeManager) BootstrapPrimaryVolume(volumeName, nodeID, path string, epoch uint64, opts blockvol.CreateOptions) error {
	if m == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	if volumeName == "" {
		return fmt.Errorf("volumev2: bootstrap volume name is required")
	}
	if nodeID == "" {
		return fmt.Errorf("volumev2: bootstrap node id is required")
	}
	if path == "" {
		return fmt.Errorf("volumev2: bootstrap path is required")
	}
	node, err := m.localNode(nodeID)
	if err != nil {
		return err
	}
	return node.ApplyAssignments([]masterv2.Assignment{{
		Name:          volumeName,
		Path:          path,
		NodeID:        nodeID,
		Epoch:         epoch,
		LeaseTTL:      30 * time.Second,
		CreateOptions: opts,
		Role:          "primary",
	}})
}
