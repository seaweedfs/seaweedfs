package shell

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

func TestResolveLogPurgeFilersRejectsEmptyExplicitList(t *testing.T) {
	commandEnv := &CommandEnv{option: &ShellOptions{FilerAddress: "filer-only:8888"}}

	for _, filerList := range []string{" ", ",,", " , "} {
		if filers, err := resolveLogPurgeFilers(filerList, commandEnv, false, &strings.Builder{}); err == nil {
			t.Errorf("-filer=%q: got %v, want an error", filerList, filers)
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

func TestResolveLogPurgeFilersFromMaster(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr
	srv.filers.Store(3)

	commandEnv, cancel := discoveryEnv(addr, 10*time.Second)
	defer cancel()
	commandEnv.option.FilerAddress = "filer-connected:8888"

	filers, err := resolveLogPurgeFilers("", commandEnv, false, &strings.Builder{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []pb.ServerAddress{"127.0.0.1:8800", "127.0.0.1:8801", "127.0.0.1:8802"}
	if !slices.Equal(filers, want) {
		t.Fatalf("got %v, want %v", filers, want)
	}
}

func TestResolveLogPurgeFilersEmptyMasterSkipsSecondLookup(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr

	commandEnv, cancel := discoveryEnv(addr, 10*time.Second)
	defer cancel()

	start := time.Now()
	if filers, err := resolveLogPurgeFilers("", commandEnv, false, &strings.Builder{}); err == nil {
		t.Fatalf("got %v, want an error", filers)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("resolve took %v, it should not wait out the shell's filer discovery", elapsed)
	}

	commandEnv.option.FilerAddress = "filer-connected:8888"
	filers, err := resolveLogPurgeFilers("", commandEnv, false, &strings.Builder{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(filers, []pb.ServerAddress{"filer-connected:8888"}) {
		t.Fatalf("got %v, want [filer-connected:8888]", filers)
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

func TestFsLogPurgeContinuesPastUnreachableFiler(t *testing.T) {
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		filer.SystemLogDir + "/2020-01-01": {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1}},
		filer.SystemLogDir + "/today":      {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}},
	}}
	commandEnv := startStubFiler(t, stub)
	reachable := commandEnv.option.FilerAddress

	err := (&commandFsLogPurge{}).Do([]string{"-daysAgo=1", "-filer=127.0.0.1:1.1," + string(reachable) + ",127.0.0.1:2.2"}, commandEnv, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "failed to purge 2 of 3 filers") {
		t.Fatalf("got error %v, want both unreachable filers reported", err)
	}
	if !slices.Equal(stub.deleted, []string{filer.SystemLogDir + "/2020-01-01"}) {
		t.Fatalf("reachable filer deleted %v, want only the old log directory", stub.deleted)
	}
	if commandEnv.option.FilerAddress != reachable {
		t.Fatalf("command env filer changed to %q, want %q", commandEnv.option.FilerAddress, reachable)
	}
}
