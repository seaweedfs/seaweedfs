package csi

import (
	"context"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/volumev2"
)

func TestV2RuntimeBackend_CreateLookupAndPublish(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := volumev2.NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	node, err := volumev2.New(volumev2.Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volume node: %v", err)
	}
	defer node.Close()
	if err := manager.RegisterNode(node); err != nil {
		t.Fatalf("register node: %v", err)
	}

	backend := NewV2RuntimeBackend(manager, "node-a", t.TempDir(), "iqn.2026-04.com.seaweedfs:test.csi.")
	info, err := backend.CreateVolume(context.Background(), "csi-v2-vol", 1<<20)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if info.ISCSIAddr == "" || info.IQN == "" {
		t.Fatalf("missing exported target info: %+v", info)
	}

	looked, err := backend.LookupVolume(context.Background(), "csi-v2-vol")
	if err != nil {
		t.Fatalf("lookup volume: %v", err)
	}
	if looked.ISCSIAddr != info.ISCSIAddr || looked.IQN != info.IQN {
		t.Fatalf("lookup mismatch: create=%+v lookup=%+v", info, looked)
	}

	controller := &controllerServer{backend: backend}
	pub, err := controller.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "csi-v2-vol",
		NodeId:   "node-a",
	})
	if err != nil {
		t.Fatalf("controller publish: %v", err)
	}
	if pub.PublishContext["iscsiAddr"] == "" || pub.PublishContext["iqn"] == "" {
		t.Fatalf("missing publish context: %+v", pub.PublishContext)
	}
}
