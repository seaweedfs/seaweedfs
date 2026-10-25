package framework

import "testing"

func TestUseRustVolumeServer(t *testing.T) {
	t.Setenv("VOLUME_SERVER_IMPL", "rust")
	if !useRustVolumeServer() {
		t.Fatalf("expected rust selection when VOLUME_SERVER_IMPL=rust")
	}

	t.Setenv("VOLUME_SERVER_IMPL", "go")
	if useRustVolumeServer() {
		t.Fatalf("expected go selection when VOLUME_SERVER_IMPL=go")
	}

	t.Setenv("VOLUME_SERVER_IMPL", "")
	if useRustVolumeServer() {
		t.Fatalf("expected go selection when VOLUME_SERVER_IMPL is unset")
	}
}
