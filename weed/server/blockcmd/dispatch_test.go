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

func (f fakeOps) StartCatchUp(replicaID string, targetLSN uint64) (bool, error) {
	if f.startCatchUpFn == nil {
		return false, nil
	}
	return f.startCatchUpFn(replicaID, targetLSN)
}

func (f fakeOps) StartRebuild(replicaID string, targetLSN uint64) (bool, error) {
	if f.startRebuildFn == nil {
		return false, nil
	}
	return f.startRebuildFn(replicaID, targetLSN)
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
		startCatchUpFn: func(replicaID string, targetLSN uint64) (bool, error) {
			return false, errors.New("boom")
		},
		startRebuildFn: func(replicaID string, targetLSN uint64) (bool, error) {
			t.Fatal("should not execute after first error")
			return false, nil
		},
	}, effects)
	err := d.Run([]engine.Command{
		engine.StartCatchUpCommand{VolumeID: "vol1", ReplicaID: "vol1/r1", TargetLSN: 10},
		engine.StartRebuildCommand{VolumeID: "vol1", ReplicaID: "vol1/r1", TargetLSN: 20},
	}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

type fakeRecoveryCoordinator struct {
	startedReplica string
	startedAssigns []blockvol.BlockVolumeAssignment
	catchUpCalls   []struct {
		replicaID string
		targetLSN uint64
	}
	rebuildCalls []struct {
		replicaID string
		targetLSN uint64
	}
}

func (f *fakeRecoveryCoordinator) StartRecoveryTask(replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	f.startedReplica = replicaID
	f.startedAssigns = assignments
}

func (f *fakeRecoveryCoordinator) ExecutePendingCatchUp(replicaID string, targetLSN uint64) error {
	f.catchUpCalls = append(f.catchUpCalls, struct {
		replicaID string
		targetLSN uint64
	}{replicaID: replicaID, targetLSN: targetLSN})
	return nil
}

func (f *fakeRecoveryCoordinator) ExecutePendingRebuild(replicaID string, targetLSN uint64) error {
	f.rebuildCalls = append(f.rebuildCalls, struct {
		replicaID string
		targetLSN uint64
	}{replicaID: replicaID, targetLSN: targetLSN})
	return nil
}

type fakeProjectionReader struct {
	proj engine.PublicationProjection
	ok   bool
}

func (f fakeProjectionReader) Projection(volumeID string) (engine.PublicationProjection, bool) {
	if !f.ok || f.proj.VolumeID != volumeID {
		return engine.PublicationProjection{}, false
	}
	return f.proj, true
}

type fakeProjectionCache struct {
	published map[string]engine.PublicationProjection
}

func (f *fakeProjectionCache) StoreProjection(volumeID string, projection engine.PublicationProjection) {
	if f.published == nil {
		f.published = make(map[string]engine.PublicationProjection)
	}
	f.published[volumeID] = projection
}

type fakeSessionInvalidator struct {
	reasons []string
	states  []engine.ReplicaState
}

func (f *fakeSessionInvalidator) InvalidateSession(reason string, targetState engine.ReplicaState) {
	f.reasons = append(f.reasons, reason)
	f.states = append(f.states, targetState)
}

func TestServiceOps_StartRecoveryTaskUsesRecoveryCoordinator(t *testing.T) {
	rec := &fakeRecoveryCoordinator{}
	ops := NewServiceOps(fakeOps{}, rec, nil, nil)
	assign := blockvol.BlockVolumeAssignment{Path: "vol1"}
	executed, err := ops.StartRecoveryTask("vol1/vs2", assign)
	if err != nil {
		t.Fatal(err)
	}
	if !executed {
		t.Fatal("expected executed")
	}
	if rec.startedReplica != "vol1/vs2" || len(rec.startedAssigns) != 1 || rec.startedAssigns[0].Path != "vol1" {
		t.Fatalf("started=%q assigns=%v", rec.startedReplica, rec.startedAssigns)
	}
}

func TestHostEffects_PublishProjectionPrefersLatestCoreProjection(t *testing.T) {
	cache := &fakeProjectionCache{}
	effects := NewHostEffects(
		nil,
		nil,
		fakeProjectionReader{
			proj: engine.PublicationProjection{VolumeID: "vol1", Role: engine.RoleReplica},
			ok:   true,
		},
		cache,
	)
	err := effects.PublishProjection("vol1", engine.PublicationProjection{VolumeID: "vol1", Role: engine.RolePrimary})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := cache.published["vol1"]
	if !ok {
		t.Fatal("expected cached projection")
	}
	if got.Role != engine.RoleReplica {
		t.Fatalf("role=%v", got.Role)
	}
}

func TestHostEffects_RecordAndEmitUseCallbacks(t *testing.T) {
	var recorded []string
	var events []engine.Event
	effects := NewHostEffects(
		func(volumeID, name string) {
			recorded = append(recorded, volumeID+":"+name)
		},
		func(ev engine.Event) {
			events = append(events, ev)
		},
		nil,
		nil,
	)
	effects.RecordCommand("vol1", "apply_role")
	effects.EmitCoreEvent(engine.RoleApplied{ID: "vol1"})
	if !reflect.DeepEqual(recorded, []string{"vol1:apply_role"}) {
		t.Fatalf("recorded=%v", recorded)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	if _, ok := events[0].(engine.RoleApplied); !ok {
		t.Fatalf("event=%T", events[0])
	}
}

func TestServiceOps_InvalidateSessionUsesProjectionAndSenderResolver(t *testing.T) {
	s1 := &fakeSessionInvalidator{}
	s2 := &fakeSessionInvalidator{}
	ops := NewServiceOps(
		fakeOps{},
		nil,
		fakeProjectionReader{
			ok: true,
			proj: engine.PublicationProjection{
				VolumeID:   "vol1",
				ReplicaIDs: []string{"vol1/vs2", "vol1/vs3"},
			},
		},
		func(replicaID string) SessionInvalidator {
			switch replicaID {
			case "vol1/vs2":
				return s1
			case "vol1/vs3":
				return s2
			default:
				return nil
			}
		},
	)
	executed, err := ops.InvalidateSession("vol1", "test_reason")
	if err != nil {
		t.Fatal(err)
	}
	if !executed {
		t.Fatal("expected executed")
	}
	if !reflect.DeepEqual(s1.reasons, []string{"test_reason"}) || !reflect.DeepEqual(s2.reasons, []string{"test_reason"}) {
		t.Fatalf("reasons1=%v reasons2=%v", s1.reasons, s2.reasons)
	}
	if len(s1.states) != 1 || s1.states[0] != engine.StateDisconnected {
		t.Fatalf("states1=%v", s1.states)
	}
}

func TestServiceOps_StartRecoveryTask_NilRecoveryIsNoop(t *testing.T) {
	ops := NewServiceOps(fakeOps{}, nil, nil, nil)
	executed, err := ops.StartRecoveryTask("vol1/vs2", blockvol.BlockVolumeAssignment{Path: "vol1"})
	if err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("expected noop when recovery is nil")
	}
}
