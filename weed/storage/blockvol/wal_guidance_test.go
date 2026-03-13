package blockvol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALSizingGuidance_AdequateGeneral(t *testing.T) {
	r := WALSizingGuidance(64<<20, 4096, WorkloadGeneral)
	if r.Level != "ok" {
		t.Fatalf("Level = %q, want ok; warnings: %v", r.Level, r.Warnings)
	}
}

func TestWALSizingGuidance_UndersizedDatabase(t *testing.T) {
	r := WALSizingGuidance(16<<20, 4096, WorkloadDatabase)
	if r.Level != "warn" {
		t.Fatalf("Level = %q, want warn", r.Level)
	}
	if len(r.Warnings) == 0 {
		t.Fatal("expected warnings for undersized database WAL")
	}
}

func TestWALSizingGuidance_UndersizedThroughput(t *testing.T) {
	r := WALSizingGuidance(16<<20, 4096, WorkloadThroughput)
	if r.Level != "warn" {
		t.Fatalf("Level = %q, want warn", r.Level)
	}
}

func TestWALSizingGuidance_AbsoluteMinimum(t *testing.T) {
	// WAL smaller than 64 * blockSize.
	blockSize := uint64(65536)
	walSize := blockSize * 32 // only 32 entries worth
	r := WALSizingGuidance(walSize, blockSize, WorkloadGeneral)
	if r.Level != "warn" {
		t.Fatalf("Level = %q, want warn (WAL < 64*blockSize)", r.Level)
	}
}

func TestWALSizingGuidance_UnknownHint(t *testing.T) {
	r := WALSizingGuidance(64<<20, 4096, "mystery")
	// Unknown hint should produce an advisory, but 64MB is fine for general.
	if r.Level != "ok" {
		t.Fatalf("Level = %q, want ok for adequate size with unknown hint", r.Level)
	}
	found := false
	for _, w := range r.Warnings {
		if len(w) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected advisory warning for unknown hint")
	}
}

func TestEvaluateWALConfig_HighConcurrencySmallWAL(t *testing.T) {
	// 256KB WAL, 4KB blocks, 64 concurrent — WAL < 64 * 4KB * 4 = 1MB.
	r := EvaluateWALConfig(256*1024, 4096, 64, WorkloadGeneral)
	if r.Level != "warn" {
		t.Fatalf("Level = %q, want warn", r.Level)
	}
}

func TestEvaluateWALConfig_SaneDefaults(t *testing.T) {
	r := EvaluateWALConfig(64<<20, 4096, 16, WorkloadGeneral)
	if r.Level != "ok" {
		t.Fatalf("Level = %q, want ok; warnings: %v", r.Level, r.Warnings)
	}
}

func TestWALStatus_ReflectsVolumeState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.vol")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 << 20, // 1MB
		WALSize:    64 << 10, // 64KB
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	// Write something to make UsedFraction > 0.
	data := make([]byte, 4096)
	data[0] = 0xAA
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	ws := vol.WALStatus()
	if ws.UsedFraction <= 0 {
		t.Errorf("UsedFraction = %f, want > 0 after write", ws.UsedFraction)
	}
	if ws.SoftWatermark == 0 || ws.HardWatermark == 0 {
		t.Errorf("thresholds not populated: soft=%f hard=%f", ws.SoftWatermark, ws.HardWatermark)
	}
}

func TestWALStatus_NilAdmission(t *testing.T) {
	// Construct a minimal BlockVol with nil walAdmission.
	vol := &BlockVol{
		Metrics: NewEngineMetrics(),
	}
	ws := vol.WALStatus()
	if ws.PressureState != "normal" {
		t.Errorf("PressureState = %q, want normal for nil admission", ws.PressureState)
	}
	if ws.SoftWatermark != 0.7 || ws.HardWatermark != 0.9 {
		t.Errorf("default thresholds wrong: soft=%f hard=%f", ws.SoftWatermark, ws.HardWatermark)
	}
}

func TestWALStatus_IncludesThresholds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.vol")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 << 20,
		WALSize:    64 << 10,
	}, BlockVolConfig{
		WALSoftWatermark: 0.6,
		WALHardWatermark: 0.85,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	ws := vol.WALStatus()
	if ws.SoftWatermark != 0.6 {
		t.Errorf("SoftWatermark = %f, want 0.6", ws.SoftWatermark)
	}
	if ws.HardWatermark != 0.85 {
		t.Errorf("HardWatermark = %f, want 0.85", ws.HardWatermark)
	}
}

func init() {
	// Suppress log output during tests.
	_ = os.Stderr
}
