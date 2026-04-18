package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestMountRegister_DisabledIsNoOp(t *testing.T) {
	fs := &FilerServer{} // mountPeerRegistry nil → registry disabled
	_, err := fs.MountRegister(context.Background(), &filer_pb.MountRegisterRequest{
		PeerAddr:   "mount-a:18080",
		TtlSeconds: 30,
	})
	if err != nil {
		t.Fatalf("MountRegister returned error with disabled registry: %v", err)
	}
	resp, err := fs.MountList(context.Background(), &filer_pb.MountListRequest{})
	if err != nil {
		t.Fatalf("MountList returned error with disabled registry: %v", err)
	}
	if len(resp.Mounts) != 0 {
		t.Errorf("expected empty list from disabled registry, got %d", len(resp.Mounts))
	}
}

func TestMountRegister_EnabledStoresAndLists(t *testing.T) {
	fs := &FilerServer{mountPeerRegistry: filer.NewMountPeerRegistry()}

	if _, err := fs.MountRegister(context.Background(), &filer_pb.MountRegisterRequest{
		PeerAddr:   "mount-a:18080",
		Rack:       "rack-a",
		TtlSeconds: 60,
	}); err != nil {
		t.Fatalf("MountRegister: %v", err)
	}
	if _, err := fs.MountRegister(context.Background(), &filer_pb.MountRegisterRequest{
		PeerAddr:   "mount-b:18080",
		Rack:       "rack-b",
		TtlSeconds: 60,
	}); err != nil {
		t.Fatalf("MountRegister: %v", err)
	}

	resp, err := fs.MountList(context.Background(), &filer_pb.MountListRequest{})
	if err != nil {
		t.Fatalf("MountList: %v", err)
	}
	if len(resp.Mounts) != 2 {
		t.Fatalf("expected 2 mounts, got %d", len(resp.Mounts))
	}

	byAddr := map[string]*filer_pb.MountInfo{}
	for _, m := range resp.Mounts {
		byAddr[m.PeerAddr] = m
	}
	if byAddr["mount-a:18080"].Rack != "rack-a" {
		t.Errorf("unexpected rack for mount-a: %+v", byAddr["mount-a:18080"])
	}
	if byAddr["mount-b:18080"].Rack != "rack-b" {
		t.Errorf("unexpected rack for mount-b: %+v", byAddr["mount-b:18080"])
	}
	if byAddr["mount-a:18080"].LastSeenNs == 0 {
		t.Errorf("last_seen_ns should be populated")
	}
}

func TestMountRegister_RenewIsIdempotent(t *testing.T) {
	fs := &FilerServer{mountPeerRegistry: filer.NewMountPeerRegistry()}
	for i := 0; i < 5; i++ {
		if _, err := fs.MountRegister(context.Background(), &filer_pb.MountRegisterRequest{
			PeerAddr:   "mount-a:18080",
			TtlSeconds: 30,
		}); err != nil {
			t.Fatalf("MountRegister iteration %d: %v", i, err)
		}
	}
	resp, _ := fs.MountList(context.Background(), &filer_pb.MountListRequest{})
	if len(resp.Mounts) != 1 {
		t.Errorf("repeated renew should still show 1 entry, got %d", len(resp.Mounts))
	}
}
