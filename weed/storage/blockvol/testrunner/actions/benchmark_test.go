package actions

import (
	"testing"
)

func TestExtractHost(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"192.168.1.184:18400", "192.168.1.184"},
		{"10.0.0.3:4420", "10.0.0.3"},
		{":3299", ""},
		{"0.0.0.0:3299", "0.0.0.0"},
		{"[::]:3299", "::"},
		{"localhost:9555", "localhost"},
		{"", ""},
		{"no-port", "no-port"},
	}
	for _, tt := range tests {
		got := extractHost(tt.input)
		if got != tt.want {
			t.Errorf("extractHost(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestBenchmarkReportHeader_CrossMachineDetection(t *testing.T) {
	// Cross-machine: different IPs.
	p := extractHost("192.168.1.184:18400")
	r := extractHost("192.168.1.181:18401")
	if p == r {
		t.Fatal("expected different IPs for cross-machine")
	}

	// Same-host: same IP different port.
	p2 := extractHost("192.168.1.184:18400")
	r2 := extractHost("192.168.1.184:18401")
	if p2 != r2 {
		t.Fatal("expected same IP for same-host")
	}
}

func TestPostcheckPgdataLocalDetection(t *testing.T) {
	// pgdata under mount path — OK.
	mount := "/mnt/bench"
	pgdata := "/mnt/bench/pgdata"
	if !hasPrefix(pgdata, mount) {
		t.Fatal("pgdata under mount should be detected as OK")
	}

	// pgdata NOT under mount — suspect (local disk).
	pgdata2 := "/tmp/pgdata"
	if hasPrefix(pgdata2, mount) {
		t.Fatal("pgdata on /tmp should be detected as local disk")
	}
}

func hasPrefix(path, prefix string) bool {
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

func TestPreflightAddressCheck(t *testing.T) {
	// These should fail preflight.
	badAddrs := []string{":3299", "0.0.0.0:3299", "[::]:3299"}
	for _, addr := range badAddrs {
		host := extractHost(addr)
		if host != "" && host != "0.0.0.0" && host != "::" {
			t.Errorf("address %q should be detected as non-routable, got host=%q", addr, host)
		}
	}

	// These should pass.
	goodAddrs := []string{"192.168.1.181:5099", "10.0.0.3:4420"}
	for _, addr := range goodAddrs {
		host := extractHost(addr)
		if host == "" || host == "0.0.0.0" || host == "::" {
			t.Errorf("address %q should be routable, got host=%q", addr, host)
		}
	}
}
