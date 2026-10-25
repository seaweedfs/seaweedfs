package util

import "testing"

func TestHostKeyStripsPort(t *testing.T) {
	t.Parallel()
	if got := HostKey("10.87.0.3:8080"); got != "10.87.0.3" {
		t.Fatalf("HostKey replica: got %q", got)
	}
	if got := HostKey("10.87.0.3:33646"); got != "10.87.0.3" {
		t.Fatalf("HostKey worker: got %q", got)
	}
	if HostKey("10.87.0.3:8080") != HostKey("10.87.0.3:33646") {
		t.Fatal("worker and volume server on the same IP must share a HostKey")
	}
}

func TestHostKeysFromAddressesDedups(t *testing.T) {
	t.Parallel()
	got := HostKeysFromAddresses([]string{"10.0.0.1:8080", "10.0.0.1:8081", "10.0.0.2:8080", ""})
	if len(got) != 2 || got[0] != "10.0.0.1" || got[1] != "10.0.0.2" {
		t.Fatalf("got %v", got)
	}
}
