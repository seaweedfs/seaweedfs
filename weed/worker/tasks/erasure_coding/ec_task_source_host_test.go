package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func TestLateBindSourceToWorkerHostEqualFileCountPrefersWorkerHost(t *testing.T) {
	a := replicaFileCount{url: "full-01:8080", fileCount: 21}
	b := replicaFileCount{url: "full-02:8081", fileCount: 21}
	worker := "full-02:9334"

	// Sources [A,B] — topology-first is A; worker on B must still copy B.
	if got := lateBindSourceToWorkerHost(a.url, worker, []replicaFileCount{a, b}); got != b.url {
		t.Fatalf("Sources [A,B] worker B: got %s want %s", got, b.url)
	}
	// Invert Sources — still B.
	if got := lateBindSourceToWorkerHost(b.url, worker, []replicaFileCount{b, a}); got != b.url {
		t.Fatalf("Sources [B,A] worker B: got %s want %s", got, b.url)
	}
}

func TestLateBindSourceToWorkerHostDivergeKeepsMaxFileCount(t *testing.T) {
	a := replicaFileCount{url: "full-01:8080", fileCount: 40}
	b := replicaFileCount{url: "full-02:8081", fileCount: 21}
	worker := "full-02:9334"

	if got := lateBindSourceToWorkerHost(a.url, worker, []replicaFileCount{a, b}); got != a.url {
		t.Fatalf("diverge [A,B]: got %s want max FileCount %s", got, a.url)
	}
	if got := lateBindSourceToWorkerHost(a.url, worker, []replicaFileCount{b, a}); got != a.url {
		t.Fatalf("diverge [B,A]: got %s want max FileCount %s", got, a.url)
	}
}

func TestBindSourceAfterIdxGateDoesNotKeepStubURL(t *testing.T) {
	task := NewErasureCodingTask("ec-idx-gate", "stub:1", 9, "c", nil)
	bound := bindSourceAfterIdxGate(task.server, wdclient.Location{Url: "full-01:8080"})
	task.server = bound
	if task.server == "stub:1" {
		t.Fatal("after gate t.server is still a stub URL")
	}
	if task.server != "full-01:8080" {
		t.Fatalf("got %s, want replica URL", task.server)
	}
}
