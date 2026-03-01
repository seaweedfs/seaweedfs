package blockvol

import (
	"errors"
	"fmt"
	"time"
)

// BlockVolConfig configures tunable parameters for a BlockVol engine.
// A zero-value config is valid and will use defaults via applyDefaults().
type BlockVolConfig struct {
	GroupCommitMaxDelay     time.Duration // max wait before flushing a partial batch (default 1ms)
	GroupCommitMaxBatch     int           // flush immediately when this many waiters (default 64)
	GroupCommitLowWatermark int           // skip delay if fewer than this many pending (default 4)
	WALPressureThreshold   float64       // fraction of WAL used that triggers urgent flush (default 0.8)
	WALFullTimeout         time.Duration // max retry time when WAL is full (default 5s)
	FlushInterval          time.Duration // flusher periodic interval (default 100ms)
	DirtyMapShards         int           // number of dirty map shards, must be power-of-2 (default 256)
}

// DefaultConfig returns a BlockVolConfig with production defaults.
func DefaultConfig() BlockVolConfig {
	return BlockVolConfig{
		GroupCommitMaxDelay:     1 * time.Millisecond,
		GroupCommitMaxBatch:     64,
		GroupCommitLowWatermark: 4,
		WALPressureThreshold:   0.8,
		WALFullTimeout:         5 * time.Second,
		FlushInterval:          100 * time.Millisecond,
		DirtyMapShards:         256,
	}
}

// applyDefaults fills zero-value fields with production defaults.
func (c *BlockVolConfig) applyDefaults() {
	d := DefaultConfig()
	if c.GroupCommitMaxDelay == 0 {
		c.GroupCommitMaxDelay = d.GroupCommitMaxDelay
	}
	if c.GroupCommitMaxBatch == 0 {
		c.GroupCommitMaxBatch = d.GroupCommitMaxBatch
	}
	if c.GroupCommitLowWatermark == 0 {
		c.GroupCommitLowWatermark = d.GroupCommitLowWatermark
	}
	if c.WALPressureThreshold == 0 {
		c.WALPressureThreshold = d.WALPressureThreshold
	}
	if c.WALFullTimeout == 0 {
		c.WALFullTimeout = d.WALFullTimeout
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = d.FlushInterval
	}
	if c.DirtyMapShards == 0 {
		c.DirtyMapShards = d.DirtyMapShards
	}
}

var errInvalidConfig = errors.New("blockvol: invalid config")

// Validate checks that config values are within acceptable ranges.
func (c *BlockVolConfig) Validate() error {
	if c.DirtyMapShards <= 0 || (c.DirtyMapShards&(c.DirtyMapShards-1)) != 0 {
		return fmt.Errorf("%w: DirtyMapShards must be a positive power-of-2, got %d", errInvalidConfig, c.DirtyMapShards)
	}
	if c.WALPressureThreshold <= 0 || c.WALPressureThreshold > 1 {
		return fmt.Errorf("%w: WALPressureThreshold must be in (0,1], got %f", errInvalidConfig, c.WALPressureThreshold)
	}
	if c.GroupCommitMaxDelay <= 0 {
		return fmt.Errorf("%w: GroupCommitMaxDelay must be positive, got %v", errInvalidConfig, c.GroupCommitMaxDelay)
	}
	if c.GroupCommitMaxBatch <= 0 {
		return fmt.Errorf("%w: GroupCommitMaxBatch must be positive, got %d", errInvalidConfig, c.GroupCommitMaxBatch)
	}
	if c.GroupCommitLowWatermark < 0 {
		return fmt.Errorf("%w: GroupCommitLowWatermark must be >= 0, got %d", errInvalidConfig, c.GroupCommitLowWatermark)
	}
	if c.WALFullTimeout <= 0 {
		return fmt.Errorf("%w: WALFullTimeout must be positive, got %v", errInvalidConfig, c.WALFullTimeout)
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("%w: FlushInterval must be positive, got %v", errInvalidConfig, c.FlushInterval)
	}
	return nil
}
