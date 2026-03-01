package blockvol

import (
	"errors"
	"testing"
	"time"
)

func TestBlockVolConfig(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "config_defaults", run: testConfigDefaults},
		{name: "config_validate_good", run: testConfigValidateGood},
		{name: "config_validate_bad_shards", run: testConfigValidateBadShards},
		{name: "config_validate_bad_threshold", run: testConfigValidateBadThreshold},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.GroupCommitMaxDelay != 1*time.Millisecond {
		t.Errorf("GroupCommitMaxDelay = %v, want 1ms", cfg.GroupCommitMaxDelay)
	}
	if cfg.GroupCommitMaxBatch != 64 {
		t.Errorf("GroupCommitMaxBatch = %d, want 64", cfg.GroupCommitMaxBatch)
	}
	if cfg.GroupCommitLowWatermark != 4 {
		t.Errorf("GroupCommitLowWatermark = %d, want 4", cfg.GroupCommitLowWatermark)
	}
	if cfg.WALPressureThreshold != 0.8 {
		t.Errorf("WALPressureThreshold = %f, want 0.8", cfg.WALPressureThreshold)
	}
	if cfg.WALFullTimeout != 5*time.Second {
		t.Errorf("WALFullTimeout = %v, want 5s", cfg.WALFullTimeout)
	}
	if cfg.FlushInterval != 100*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 100ms", cfg.FlushInterval)
	}
	if cfg.DirtyMapShards != 256 {
		t.Errorf("DirtyMapShards = %d, want 256", cfg.DirtyMapShards)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig().Validate() = %v, want nil", err)
	}
}

func testConfigValidateGood(t *testing.T) {
	cases := []BlockVolConfig{
		DefaultConfig(),
		{
			GroupCommitMaxDelay:     5 * time.Millisecond,
			GroupCommitMaxBatch:     128,
			GroupCommitLowWatermark: 0,
			WALPressureThreshold:   1.0,
			WALFullTimeout:         10 * time.Second,
			FlushInterval:          50 * time.Millisecond,
			DirtyMapShards:         1,
		},
		{
			GroupCommitMaxDelay:     1 * time.Microsecond,
			GroupCommitMaxBatch:     1,
			GroupCommitLowWatermark: 100,
			WALPressureThreshold:   0.01,
			WALFullTimeout:         1 * time.Millisecond,
			FlushInterval:          1 * time.Millisecond,
			DirtyMapShards:         1024,
		},
	}
	for i, cfg := range cases {
		if err := cfg.Validate(); err != nil {
			t.Errorf("case %d: Validate() = %v, want nil", i, err)
		}
	}
}

func testConfigValidateBadShards(t *testing.T) {
	cases := []int{0, 3, 5, 7, 10, 15, -1}
	for _, shards := range cases {
		cfg := DefaultConfig()
		cfg.DirtyMapShards = shards
		err := cfg.Validate()
		if err == nil {
			t.Errorf("DirtyMapShards=%d: expected error, got nil", shards)
			continue
		}
		if !errors.Is(err, errInvalidConfig) {
			t.Errorf("DirtyMapShards=%d: expected errInvalidConfig, got %v", shards, err)
		}
	}
}

func testConfigValidateBadThreshold(t *testing.T) {
	cases := []float64{0, -0.1, -1, 1.01, 2.0}
	for _, thresh := range cases {
		cfg := DefaultConfig()
		cfg.WALPressureThreshold = thresh
		err := cfg.Validate()
		if err == nil {
			t.Errorf("WALPressureThreshold=%f: expected error, got nil", thresh)
			continue
		}
		if !errors.Is(err, errInvalidConfig) {
			t.Errorf("WALPressureThreshold=%f: expected errInvalidConfig, got %v", thresh, err)
		}
	}
}
