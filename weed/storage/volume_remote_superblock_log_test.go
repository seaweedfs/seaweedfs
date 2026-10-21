package storage

import (
	"errors"
	"testing"
)

func TestRemoteSuperBlockLogOnlyOnError(t *testing.T) {
	var logged int
	old := logRemoteSuperBlockFault
	logRemoteSuperBlockFault = func(string, ...interface{}) { logged++ }
	defer func() { logRemoteSuperBlockFault = old }()
	logRemoteSuperBlockErr(219, nil)
	if logged != 0 {
		t.Fatalf("a nil error was logged at ERROR (the \"readSuperBlock remote volume 219: <nil>\" line)")
	}
	logRemoteSuperBlockErr(219, errors.New("timeout")) // positive control
	if logged != 1 {
		t.Fatalf("a real error must still be logged, got %d", logged)
	}
}
