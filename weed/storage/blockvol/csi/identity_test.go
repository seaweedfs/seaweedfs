package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestIdentity_GetPluginInfo(t *testing.T) {
	s := &identityServer{}
	resp, err := s.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo: %v", err)
	}
	if resp.Name != DriverName {
		t.Fatalf("name: got %q, want %q", resp.Name, DriverName)
	}
	if resp.VendorVersion != DriverVersion {
		t.Fatalf("version: got %q, want %q", resp.VendorVersion, DriverVersion)
	}
}

func TestIdentity_GetPluginCapabilities(t *testing.T) {
	s := &identityServer{}
	resp, err := s.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities: %v", err)
	}
	if len(resp.Capabilities) != 1 {
		t.Fatalf("capabilities: got %d, want 1", len(resp.Capabilities))
	}
	svc := resp.Capabilities[0].GetService()
	if svc == nil {
		t.Fatal("expected service capability")
	}
	if svc.Type != csi.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Fatalf("capability type: got %v, want CONTROLLER_SERVICE", svc.Type)
	}
}

func TestIdentity_Probe(t *testing.T) {
	s := &identityServer{}
	resp, err := s.Probe(context.Background(), &csi.ProbeRequest{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if resp.Ready == nil || !resp.Ready.Value {
		t.Fatal("expected ready=true")
	}
}
