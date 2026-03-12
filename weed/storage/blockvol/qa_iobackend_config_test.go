//go:build ignore

package blockvol

import (
	"strings"
	"testing"
)

// =============================================================================
// QA Adversarial Tests for IOBackend Config (Item 3)
//
// Covers: ParseIOBackend, ResolveIOBackend, Validate for IOBackend field,
// edge cases, unknown values, io_uring rejection, case insensitivity.
// =============================================================================

// --- ParseIOBackend ---

func TestQA_ParseIOBackend_ValidInputs(t *testing.T) {
	cases := []struct {
		input string
		want  IOBackend
	}{
		{"auto", IOBackendAuto},
		{"AUTO", IOBackendAuto},
		{"Auto", IOBackendAuto},
		{"", IOBackendAuto},
		{"  auto  ", IOBackendAuto},
		{"standard", IOBackendStandard},
		{"STANDARD", IOBackendStandard},
		{"Standard", IOBackendStandard},
		{"  standard  ", IOBackendStandard},
		{"io_uring", IOBackendIOURing},
		{"IO_URING", IOBackendIOURing},
		{"Io_Uring", IOBackendIOURing},
		{"iouring", IOBackendIOURing},
		{"IOURING", IOBackendIOURing},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseIOBackend(tc.input)
			if err != nil {
				t.Fatalf("ParseIOBackend(%q): unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("ParseIOBackend(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestQA_ParseIOBackend_InvalidInputs(t *testing.T) {
	invalids := []string{
		"spdk",
		"uring",
		"io-uring",
		"io_uring_sqpoll",
		"direct",
		"aio",
		"posix",
		"libaio",
		"123",
		"null",
		"none",
	}
	for _, s := range invalids {
		t.Run(s, func(t *testing.T) {
			got, err := ParseIOBackend(s)
			if err == nil {
				t.Fatalf("ParseIOBackend(%q) = %v, want error", s, got)
			}
			if got != IOBackendAuto {
				t.Fatalf("ParseIOBackend(%q) error case should return Auto, got %v", s, got)
			}
			if !strings.Contains(err.Error(), "unknown IOBackend") {
				t.Fatalf("error should mention 'unknown IOBackend', got: %v", err)
			}
		})
	}
}

// --- IOBackend.String ---

func TestQA_IOBackend_String(t *testing.T) {
	cases := []struct {
		b    IOBackend
		want string
	}{
		{IOBackendAuto, "auto"},
		{IOBackendStandard, "standard"},
		{IOBackendIOURing, "io_uring"},
		{IOBackend(99), "unknown(99)"},
		{IOBackend(-1), "unknown(-1)"},
	}
	for _, tc := range cases {
		got := tc.b.String()
		if got != tc.want {
			t.Errorf("IOBackend(%d).String() = %q, want %q", int(tc.b), got, tc.want)
		}
	}
}

// --- ResolveIOBackend ---

func TestQA_ResolveIOBackend(t *testing.T) {
	// Auto resolves to standard.
	if got := ResolveIOBackend(IOBackendAuto); got != IOBackendStandard {
		t.Fatalf("ResolveIOBackend(Auto) = %v, want Standard", got)
	}
	// Standard stays standard.
	if got := ResolveIOBackend(IOBackendStandard); got != IOBackendStandard {
		t.Fatalf("ResolveIOBackend(Standard) = %v, want Standard", got)
	}
	// IOURing stays io_uring (resolve doesn't validate, just maps auto).
	if got := ResolveIOBackend(IOBackendIOURing); got != IOBackendIOURing {
		t.Fatalf("ResolveIOBackend(IOURing) = %v, want IOURing", got)
	}
}

// --- Validate IOBackend field ---

func TestQA_Config_Validate_IOBackend_AutoOK(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IOBackend = IOBackendAuto
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with IOBackendAuto: %v", err)
	}
}

func TestQA_Config_Validate_IOBackend_StandardOK(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IOBackend = IOBackendStandard
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with IOBackendStandard: %v", err)
	}
}

func TestQA_Config_Validate_IOBackend_IOURingRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IOBackend = IOBackendIOURing
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate should reject IOBackendIOURing (not yet implemented)")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Fatalf("error should mention 'not yet implemented', got: %v", err)
	}
}

func TestQA_Config_Validate_IOBackend_OutOfRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IOBackend = IOBackend(99)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate should reject out-of-range IOBackend")
	}
	if !strings.Contains(err.Error(), "unknown IOBackend") {
		t.Fatalf("error should mention 'unknown IOBackend', got: %v", err)
	}
}

func TestQA_Config_Validate_IOBackend_NegativeValue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IOBackend = IOBackend(-1)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate should reject negative IOBackend")
	}
}

// --- DefaultConfig IOBackend ---

func TestQA_DefaultConfig_IOBackend_IsAuto(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.IOBackend != IOBackendAuto {
		t.Fatalf("DefaultConfig().IOBackend = %v, want Auto (zero value)", cfg.IOBackend)
	}
}

// --- applyDefaults does NOT override IOBackend ---

func TestQA_ApplyDefaults_IOBackend_ZeroStaysAuto(t *testing.T) {
	cfg := BlockVolConfig{}
	cfg.applyDefaults()
	// IOBackend is not in applyDefaults — zero value (Auto) should remain.
	if cfg.IOBackend != IOBackendAuto {
		t.Fatalf("applyDefaults left IOBackend = %v, want Auto", cfg.IOBackend)
	}
}

func TestQA_ApplyDefaults_IOBackend_ExplicitPreserved(t *testing.T) {
	cfg := BlockVolConfig{IOBackend: IOBackendStandard}
	cfg.applyDefaults()
	if cfg.IOBackend != IOBackendStandard {
		t.Fatalf("applyDefaults changed IOBackend from Standard to %v", cfg.IOBackend)
	}
}

// --- Round-trip: parse → resolve → string ---

func TestQA_IOBackend_RoundTrip(t *testing.T) {
	for _, input := range []string{"auto", "standard"} {
		b, err := ParseIOBackend(input)
		if err != nil {
			t.Fatalf("ParseIOBackend(%q): %v", input, err)
		}
		resolved := ResolveIOBackend(b)
		s := resolved.String()
		if s != "standard" {
			t.Fatalf("round-trip %q → resolve → string = %q, want standard", input, s)
		}
	}
}

// --- Iota ordering stability ---

func TestQA_IOBackend_IotaValues(t *testing.T) {
	// These values are persisted/transmitted — they must never change.
	if IOBackendAuto != 0 {
		t.Fatalf("IOBackendAuto = %d, want 0", IOBackendAuto)
	}
	if IOBackendStandard != 1 {
		t.Fatalf("IOBackendStandard = %d, want 1", IOBackendStandard)
	}
	if IOBackendIOURing != 2 {
		t.Fatalf("IOBackendIOURing = %d, want 2", IOBackendIOURing)
	}
}
