package mount

import (
	"sync/atomic"
	"testing"
)

func TestUncommittedBytesTracking(t *testing.T) {
	// Reset the global counter
	atomic.StoreInt64(&uncommittedBytes, 0)

	wfs := &WFS{
		option: &Option{
			Quota: 100 * 1024 * 1024, // 100MB
		},
	}

	// Test AddUncommittedBytes
	wfs.AddUncommittedBytes(1024)
	if got := wfs.GetUncommittedBytes(); got != 1024 {
		t.Errorf("AddUncommittedBytes: got %d, want 1024", got)
	}

	// Test accumulation
	wfs.AddUncommittedBytes(2048)
	if got := wfs.GetUncommittedBytes(); got != 3072 {
		t.Errorf("AddUncommittedBytes accumulation: got %d, want 3072", got)
	}

	// Test SubtractUncommittedBytes
	wfs.SubtractUncommittedBytes(1024)
	if got := wfs.GetUncommittedBytes(); got != 2048 {
		t.Errorf("SubtractUncommittedBytes: got %d, want 2048", got)
	}

	// Test ResetUncommittedBytes
	wfs.ResetUncommittedBytes()
	if got := wfs.GetUncommittedBytes(); got != 0 {
		t.Errorf("ResetUncommittedBytes: got %d, want 0", got)
	}
}

func TestUncommittedBytesDoesNotGoNegative(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)

	wfs := &WFS{
		option: &Option{
			Quota: 100 * 1024 * 1024,
		},
	}

	wfs.AddUncommittedBytes(100)
	wfs.SubtractUncommittedBytes(200) // Try to subtract more than available

	if got := wfs.GetUncommittedBytes(); got < 0 {
		t.Errorf("uncommittedBytes went negative: got %d", got)
	}
}

func TestIsOverQuotaWithUncommitted(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)

	tests := []struct {
		name        string
		quota       int64
		usedSize    uint64
		uncommitted int64
		isOverQuota bool
		want        bool
	}{
		{
			name:        "no quota set",
			quota:       0,
			usedSize:    1000,
			uncommitted: 1000,
			isOverQuota: false,
			want:        false,
		},
		{
			name:        "under quota",
			quota:       1000,
			usedSize:    400,
			uncommitted: 400,
			isOverQuota: false,
			want:        false,
		},
		{
			name:        "over quota with uncommitted",
			quota:       1000,
			usedSize:    600,
			uncommitted: 500,
			isOverQuota: false,
			want:        true,
		},
		{
			name:        "already over quota flag set",
			quota:       1000,
			usedSize:    500,
			uncommitted: 0,
			isOverQuota: true,
			want:        true,
		},
		{
			name:        "exactly at quota",
			quota:       1000,
			usedSize:    500,
			uncommitted: 500,
			isOverQuota: false,
			want:        false,
		},
		{
			name:        "one byte over quota",
			quota:       1000,
			usedSize:    500,
			uncommitted: 501,
			isOverQuota: false,
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt64(&uncommittedBytes, tt.uncommitted)

			wfs := &WFS{
				option: &Option{
					Quota: tt.quota,
				},
				IsOverQuota: tt.isOverQuota,
			}
			wfs.stats.UsedSize = tt.usedSize

			got := wfs.IsOverQuotaWithUncommitted()
			if got != tt.want {
				t.Errorf("IsOverQuotaWithUncommitted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetQuotaCheckInterval(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)

	tests := []struct {
		name        string
		quota       int64
		usedSize    uint64
		uncommitted int64
		wantFast    bool
	}{
		{
			name:        "no quota",
			quota:       0,
			usedSize:    0,
			uncommitted: 0,
			wantFast:    false,
		},
		{
			name:        "under 90% threshold",
			quota:       1000,
			usedSize:    800,
			uncommitted: 0,
			wantFast:    false,
		},
		{
			name:        "at 90% threshold",
			quota:       1000,
			usedSize:    900,
			uncommitted: 0,
			wantFast:    true,
		},
		{
			name:        "over 90% with uncommitted",
			quota:       1000,
			usedSize:    500,
			uncommitted: 410,
			wantFast:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt64(&uncommittedBytes, tt.uncommitted)

			wfs := &WFS{
				option: &Option{
					Quota: tt.quota,
				},
			}
			wfs.stats.UsedSize = tt.usedSize

			got := wfs.getQuotaCheckInterval()
			if tt.wantFast && got != fastQuotaCheckInterval {
				t.Errorf("getQuotaCheckInterval() = %v, want fast interval %v", got, fastQuotaCheckInterval)
			}
			if !tt.wantFast && got != defaultQuotaCheckInterval {
				t.Errorf("getQuotaCheckInterval() = %v, want default interval %v", got, defaultQuotaCheckInterval)
			}
		})
	}
}

func TestNoQuotaTrackingWhenDisabled(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)

	wfs := &WFS{
		option: &Option{
			Quota: 0, // No quota
		},
	}

	// Should not track when quota is disabled
	wfs.AddUncommittedBytes(1000)
	if got := wfs.GetUncommittedBytes(); got != 0 {
		t.Errorf("Should not track uncommitted bytes when quota disabled: got %d", got)
	}
}
