package blockcmd

import (
	"errors"
	"reflect"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

type fakeOps struct {
	applyRoleFn         func(blockvol.BlockVolumeAssignment) (bool, error)
	startReceiverFn     func(blockvol.BlockVolumeAssignment) (bool, error)
	configureShipperFn  func(string, []engine.ReplicaAssignment) (bool, bool, error)
	startRecoveryTaskFn func(string, blockvol.BlockVolumeAssignment) (bool, error)
	invalidateSessionFn func(string, string) (bool, error)
	startCatchUpFn      func(string, uint64) (bool, error)
	startRebuildFn      func(string, uint64) (bool, error)
}

func (f fakeOps) ApplyRole(a blockvol.BlockVolumeAssignment) (bool, error) {
	if f.applyRoleFn == nil {
		return false, nil
	}
	return f.applyRoleFn(a)
}

func (f fakeOps) StartReceiver(a blockvol.BlockVolumeAssignment) (bool, error) {
	if f.startReceiverFn == nil {
		return false, nil
	}
	return f.startReceiverFn(a)
}

func (f fakeOps) ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (bool, bool, error) {
	if f.configureShipperFn == nil {
		return false, false, nil
	}
	return f.configureShipperFn(volumeID, replicas)
}

func (f fakeOps) StartRecoveryTask(replicaID string, assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if f.startRecoveryTaskFn == nil {
		return false, nil
	}
	return f.startRecoveryTaskFn(replicaID, assignment)
}

func (f fakeOps) InvalidateSession(volumeID, reason string) (bool, error) {
	if f.invalidateSessionFn == nil {
		return false, nil
	}
	return f.invalidateSessionFn(volumeID, reason)
}

func (f fakeOps) StartCatchUp(volumeID string, targetLSN uint64) (bool, error) {
	if f.startCatchUpFn == nil {
		return false, nil
	}
	return f.startCatchUpFn(volumeID, targetLSN)
}

func (f fakeOps) StartRebuild(volumeID string, targetLSN uint64) (bool, error) {
	if f.startRebuildFn == nil {
		return false, nil
	}
	return f.startRebuildFn(volumeID, targetLSN)
}

type fakeEffects struct {
	recorded  []string
	events    []engine.Event
	published map[string]engine.PublicationProjection
}

func (f *fakeEffects) RecordCommand(volumeID, name string) {
	f.recorded = append(f.recorded, volumeID+":"+name)
}

func (f *fakeEffects) EmitCoreEvent(ev engine.Event) {
	f.events = append(f.events, ev)
}

func (f *fakeEffects) PublishProjection(volumeID string, projection engine.PublicationProjection) error {
	if f.published == nil {
		f.published = make(map[string]engine.PublicationProjection)
	}
	f.published[volumeID] = projection
	return nil
}

func TestDispatcher_ApplyRoleRequiresMatchingAssignment(t *testing.T) {
	d := NewDispatcher(fakeOps{}, &fakeEffects{})
	err := d.Run([]engine.Command{engine.ApplyRoleCommand{VolumeID: "vol1"}}, &blockvol.BlockVolumeAssignment{Path: "other"})
	if err == nil {
		t.Fatal("expected path mismatch error")
	}
}

func TestDispatcher_StartReceiverRecordsAndEmitsObserved(t *testing.T) {
	effects := &fakeEffects{}
	d := NewDispatcher(fakeOps{
		startReceiverFn: func(a blockvol.BlockVolumeAssignment) (bool, error) {
			return true, nil
		},
	}, effects)
	assignment := &blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		ReplicaDataAddr: "10.0.0.1:9333",
		ReplicaCtrlAddr: "10.0.0.1:9334",
	}
	if err := d.Run([]engine.Command{engine.StartReceiverCommand{VolumeID: "vol1"}}, assignment); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(effects.recorded, []string{"vol1:start_receiver"}) {
		t.Fatalf("recorded=%v", effects.recorded)
	}
	if len(effects.events) != 1 {
		t.Fatalf("events=%d", len(effects.events))
	}
	if _, ok := effects.events[0].(engine.ReceiverReadyObserved); !ok {
		t.Fatalf("event=%T", effects.events[0])
	}
}

func TestDispatcher_ConfigureShipperKeepsHostEffectsServerSide(t *testing.T) {
	effects := &fakeEffects{}
	d := NewDispatcher(fakeOps{
		configureShipperFn: func(volumeID string, replicas []engine.ReplicaAssignment) (bool, bool, error) {
			return true, true, nil
		},
	}, effects)
	err := d.Run([]engine.Command{engine.ConfigureShipperCommand{
		VolumeID: "vol1",
		Replicas: []engine.ReplicaAssignment{{ReplicaID: "vol1/vs2", Endpoint: engine.Endpoint{DataAddr: "data", CtrlAddr: "ctrl"}}},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(effects.recorded, []string{"vol1:configure_shipper"}) {
		t.Fatalf("recorded=%v", effects.recorded)
	}
	if len(effects.events) != 2 {
		t.Fatalf("events=%d", len(effects.events))
	}
	if _, ok := effects.events[0].(engine.ShipperConfiguredObserved); !ok {
		t.Fatalf("event0=%T", effects.events[0])
	}
	if _, ok := effects.events[1].(engine.ShipperConnectedObserved); !ok {
		t.Fatalf("event1=%T", effects.events[1])
	}
}

func TestDispatcher_PublishProjectionUsesHostEffect(t *testing.T) {
	effects := &fakeEffects{}
	d := NewDispatcher(fakeOps{}, effects)
	proj := engine.PublicationProjection{VolumeID: "vol1", Role: engine.RolePrimary}
	if err := d.Run([]engine.Command{engine.PublishProjectionCommand{VolumeID: "vol1", Projection: proj}}, nil); err != nil {
		t.Fatal(err)
	}
	got, ok := effects.published["vol1"]
	if !ok || got.VolumeID != "vol1" || got.Role != engine.RolePrimary {
		t.Fatalf("published=%v", effects.published)
	}
}

func TestDispatcher_StopsOnFirstError(t *testing.T) {
	effects := &fakeEffects{}
	d := NewDispatcher(fakeOps{
		startCatchUpFn: func(volumeID string, targetLSN uint64) (bool, error) {
			return false, errors.New("boom")
		},
		startRebuildFn: func(volumeID string, targetLSN uint64) (bool, error) {
			t.Fatal("should not execute after first error")
			return false, nil
		},
	}, effects)
	err := d.Run([]engine.Command{
		engine.StartCatchUpCommand{VolumeID: "vol1", TargetLSN: 10},
		engine.StartRebuildCommand{VolumeID: "vol1", TargetLSN: 20},
	}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
