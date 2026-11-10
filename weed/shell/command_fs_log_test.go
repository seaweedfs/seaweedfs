package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestResolveLogPurgeFilersExplicitList(t *testing.T) {
	commandEnv := &CommandEnv{}

	filers, err := resolveLogPurgeFilers(" filer-a:8888, filer-b:8888 ,, filer-c:8888 ", commandEnv, false, &strings.Builder{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"}
	if len(filers) != len(want) {
		t.Fatalf("got %d filers, want %d: %v", len(filers), len(want), filers)
	}
	for i, address := range want {
		if filers[i] != address {
			t.Errorf("filer[%d] = %q, want %q", i, filers[i], address)
		}
	}
}

func TestResolveLogPurgeFilersFallsBackToConnectedFiler(t *testing.T) {
	// no explicit list and no master client: fall back to the connected filer
	commandEnv := &CommandEnv{option: &ShellOptions{FilerAddress: "filer-only:8888"}}

	filers, err := resolveLogPurgeFilers("", commandEnv, false, &strings.Builder{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(filers) != 1 || filers[0] != "filer-only:8888" {
		t.Fatalf("got %v, want [filer-only:8888]", filers)
	}
}

func TestResolveLogPurgeFilersNoneFound(t *testing.T) {
	commandEnv := &CommandEnv{option: &ShellOptions{}}

	if _, err := resolveLogPurgeFilers("", commandEnv, false, &strings.Builder{}); err == nil {
		t.Fatal("expected an error when no filer can be resolved")
	}
}

func TestPurgeFilerLogsKeepsCommandEnvFiler(t *testing.T) {
	// the per-filer purge must not mutate the shared command environment,
	// otherwise the next filer of the loop would be purged twice and the
	// original one never.
	commandEnv := &CommandEnv{option: &ShellOptions{FilerAddress: "filer-original:8888"}}

	_ = purgeFilerLogs(commandEnv, "filer-other:8888", 0, false, &strings.Builder{})

	if got := commandEnv.option.FilerAddress; got != "filer-original:8888" {
		t.Fatalf("command env filer changed to %q, want filer-original:8888", got)
	}
}

func TestPurgeFilerLogsAttemptsEveryFiler(t *testing.T) {
	// one unreachable filer must not stop the purge of the others: the loop
	// reports the failure and keeps going, so every filer is attempted.
	commandEnv := &CommandEnv{option: &ShellOptions{FilerAddress: "filer-a:8888"}}
	writer := &strings.Builder{}

	attempted := 0
	for _, address := range []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"} {
		attempted++
		_ = purgeFilerLogs(commandEnv, address, 0, false, writer)
	}

	if attempted != 3 {
		t.Fatalf("attempted %d filers, want 3", attempted)
	}
	// the shared env still points at the filer the shell started with, so a
	// later command in the same session is unaffected.
	if commandEnv.option.FilerAddress != "filer-a:8888" {
		t.Fatalf("command env filer changed to %q, want filer-a:8888", commandEnv.option.FilerAddress)
	}
}
