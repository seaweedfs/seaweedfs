package shell

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

func TestEvacuatePrefersLowerVolumeRatio(t *testing.T) {
	volumes := mkByteVolumes(1, 1, 1)
	source := makeByteNode("source", 10, 0, 0, volumes)
	fuller := makeByteNode("fuller", 10, 0, 0, mkByteVolumes(10, 4, 1))
	emptier := makeByteNode("emptier", 10, 0, 0, mkByteVolumes(20, 2, 1))

	// Put the 40%-full server first: truncating 0.2 - 0.4 to int makes the ratios tie.
	moved, err := moveAwayOneNormalVolume(nil, nil, volumes[0], source, []*Node{fuller, emptier}, false)
	if err != nil || !moved {
		t.Fatalf("evacuate: moved=%v, err=%v", moved, err)
	}
	if _, found := emptier.selectedVolumes[volumes[0].Id]; !found {
		t.Fatal("expected evacuation to choose the server with lower occupancy")
	}
}

func TestVolumeServerEvacuate(t *testing.T) {
	targetServer := ""
	volumeRack := "rack1"
	c := commandVolumeServerEvacuate{
		targetServer: &targetServer,
		volumeRack:   &volumeRack,
	}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	if err := c.evacuateNormalVolumes(nil, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}

func TestVolumeServerEvacuateNoLockFlag(t *testing.T) {
	c := &commandVolumeServerEvacuate{}

	// Test without -noLock: when -apply is set and shell is unlocked, Do must fail with lock error
	unlockedEnv := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	err := c.Do([]string{"-apply"}, unlockedEnv, io.Discard)
	if err == nil {
		t.Fatalf("expected error when running with -apply without lock or -noLock")
	}
	if !strings.Contains(err.Error(), "need to run \"lock\" first to continue") {
		t.Fatalf("expected lock error message, got: %v", err)
	}

	// Test with -noLock: should set commandEnv.noLock to true and bypass the lock requirement
	noLockEnv := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	errNoLock := c.Do([]string{"-noLock", "-apply"}, noLockEnv, io.Discard)
	if !noLockEnv.noLock {
		t.Fatalf("expected commandEnv.noLock to be true when -noLock flag is provided")
	}
	if err := noLockEnv.confirmIsLocked([]string{"volumeServer.evacuate"}); err != nil {
		t.Fatalf("confirmIsLocked should succeed when noLock is true: %v", err)
	}
	// The lock error should not be returned because -noLock bypassed it; it should reach node validation
	if errNoLock == nil || !strings.Contains(errNoLock.Error(), "need to specify volume server") {
		t.Fatalf("expected node validation error after bypassing lock check, got: %v", errNoLock)
	}
}

func TestVolumeServerEvacuateReturnsFlagParseError(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	err := (&commandVolumeServerEvacuate{}).Do([]string{"-noLock=invalid", "-apply"}, env, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "invalid boolean value") {
		t.Fatalf("expected a flag parse error, got: %v", err)
	}
	if env.noLock {
		t.Fatalf("a rejected -noLock must not unlock the shell")
	}
}

func TestVolumeServerEvacuateHelpIsNotAnError(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	if err := (&commandVolumeServerEvacuate{}).Do([]string{"-h"}, env, io.Discard); err != nil {
		t.Fatalf("-h should print usage without failing, got: %v", err)
	}
}
