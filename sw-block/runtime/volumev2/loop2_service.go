package volumev2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// Loop2ServiceConfig defines one bounded background Loop 2 observation service.
type Loop2ServiceConfig struct {
	VolumeName    string
	PrimaryNodeID string
	ExpectedEpoch uint64
	NodeIDs       []string
	Interval      time.Duration
}

// Loop2ServiceHandle controls one bounded background Loop 2 service.
type Loop2ServiceHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Stop terminates the background Loop 2 service.
func (h *Loop2ServiceHandle) Stop() {
	if h == nil || h.cancel == nil {
		return
	}
	h.cancel()
}

// Wait blocks until the background Loop 2 service exits.
func (h *Loop2ServiceHandle) Wait() {
	if h == nil || h.done == nil {
		return
	}
	<-h.done
}

// AutoFailoverConfig defines one bounded background auto-failover service.
type AutoFailoverConfig struct {
	VolumeName    string
	PrimaryNodeID string
	ExpectedEpoch uint64
	NodeIDs       []string
	Interval      time.Duration
}

// AutoFailoverHandle controls one bounded background auto-failover service.
type AutoFailoverHandle struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu             sync.RWMutex
	primaryNodeID  string
	expectedEpoch  uint64
	lastLoop2Error error
}

// Stop terminates the background auto-failover service.
func (h *AutoFailoverHandle) Stop() {
	if h == nil || h.cancel == nil {
		return
	}
	h.cancel()
}

// Wait blocks until the background auto-failover service exits.
func (h *AutoFailoverHandle) Wait() {
	if h == nil || h.done == nil {
		return
	}
	<-h.done
}

// PrimaryNodeID returns the current primary tracked by the service.
func (h *AutoFailoverHandle) PrimaryNodeID() string {
	if h == nil {
		return ""
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.primaryNodeID
}

// ExpectedEpoch returns the current expected epoch tracked by the service.
func (h *AutoFailoverHandle) ExpectedEpoch() uint64 {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.expectedEpoch
}

// LastLoop2Error returns the most recent non-fatal Loop 2 observation error.
func (h *AutoFailoverHandle) LastLoop2Error() error {
	if h == nil {
		return fmt.Errorf("volumev2: auto failover handle is nil")
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastLoop2Error
}

// StartLoop2Service starts one bounded background Loop 2 observation service.
func (m *InProcessRuntimeManager) StartLoop2Service(cfg Loop2ServiceConfig) (*Loop2ServiceHandle, error) {
	if err := validateLoop2ServiceConfig(cfg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	handle := &Loop2ServiceHandle{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		defer close(handle.done)
		ticker := time.NewTicker(normalizeLoop2Interval(cfg.Interval))
		defer ticker.Stop()
		for {
			_, _ = m.ObserveLoop2(cfg.VolumeName, cfg.PrimaryNodeID, cfg.ExpectedEpoch, cfg.NodeIDs...)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return handle, nil
}

// StartAutoFailoverService starts one bounded background auto-failover service.
// The current bounded trigger rule is intentionally fail-closed: only explicit
// primary evidence-query loss triggers failover. Healthy-but-lagging or
// ambiguous runtime states are observed but do not trigger automatically.
func (m *InProcessRuntimeManager) StartAutoFailoverService(cfg AutoFailoverConfig) (*AutoFailoverHandle, error) {
	if err := validateAutoFailoverConfig(cfg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	handle := &AutoFailoverHandle{
		cancel:        cancel,
		done:          make(chan struct{}),
		primaryNodeID: cfg.PrimaryNodeID,
		expectedEpoch: cfg.ExpectedEpoch,
	}
	go func() {
		defer close(handle.done)
		ticker := time.NewTicker(normalizeLoop2Interval(cfg.Interval))
		defer ticker.Stop()
		for {
			handle.mu.RLock()
			primaryNodeID := handle.primaryNodeID
			expectedEpoch := handle.expectedEpoch
			handle.mu.RUnlock()

			err := m.probeReplicaSummary(primaryNodeID, cfg.VolumeName, expectedEpoch)
			if err == nil {
				_, obsErr := m.ObserveLoop2(cfg.VolumeName, primaryNodeID, expectedEpoch, cfg.NodeIDs...)
				handle.mu.Lock()
				handle.lastLoop2Error = obsErr
				handle.mu.Unlock()
			} else {
				survivors := excludeNodeID(cfg.NodeIDs, primaryNodeID)
				if len(survivors) > 0 {
					result, failoverErr := m.ExecuteFailover(cfg.VolumeName, expectedEpoch, survivors...)
					if failoverErr == nil {
						handle.mu.Lock()
						handle.primaryNodeID = result.Assignment.NodeID
						if result.Assignment.Epoch != 0 {
							handle.expectedEpoch = result.Assignment.Epoch
						}
						handle.lastLoop2Error = nil
						handle.mu.Unlock()
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return handle, nil
}

func validateLoop2ServiceConfig(cfg Loop2ServiceConfig) error {
	if cfg.VolumeName == "" {
		return fmt.Errorf("volumev2: loop2 service volume name is required")
	}
	if cfg.PrimaryNodeID == "" {
		return fmt.Errorf("volumev2: loop2 service primary node id is required")
	}
	if len(cfg.NodeIDs) == 0 {
		return fmt.Errorf("volumev2: loop2 service node ids are required")
	}
	return nil
}

func validateAutoFailoverConfig(cfg AutoFailoverConfig) error {
	if cfg.VolumeName == "" {
		return fmt.Errorf("volumev2: auto failover volume name is required")
	}
	if cfg.PrimaryNodeID == "" {
		return fmt.Errorf("volumev2: auto failover primary node id is required")
	}
	if len(cfg.NodeIDs) == 0 {
		return fmt.Errorf("volumev2: auto failover node ids are required")
	}
	return nil
}

func normalizeLoop2Interval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 25 * time.Millisecond
	}
	return interval
}

func excludeNodeID(nodeIDs []string, excluded string) []string {
	out := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID == "" || nodeID == excluded {
			continue
		}
		out = append(out, nodeID)
	}
	return out
}

func (m *InProcessRuntimeManager) probeReplicaSummary(nodeID, volumeName string, expectedEpoch uint64) error {
	if m == nil || m.driver == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	targets, err := m.driver.resolveTargets([]string{nodeID})
	if err != nil {
		return err
	}
	if len(targets) != 1 || targets[0].Evidence == nil {
		return fmt.Errorf("volumev2: primary evidence target %q is unavailable", nodeID)
	}
	_, err = targets[0].Evidence.QueryReplicaSummary(protocolv2.ReplicaSummaryRequest{
		VolumeName:    volumeName,
		ExpectedEpoch: expectedEpoch,
	})
	return err
}
