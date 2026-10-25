package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func TestLateBindSourceToWorkerHostPrefersWorkerHost(t *testing.T) {
	a := wdclient.Location{Url: "full-01:8080"}
	b := wdclient.Location{Url: "full-02:8081"}
	worker := "full-02:9334"

	// Sources [A,B] — topology-first is A; worker on B must still copy B.
	if got := lateBindSourceToWorkerHost(a.Url, worker, []wdclient.Location{a, b}); got != b.Url {
		t.Fatalf("Sources [A,B] worker B: got %s want %s", got, b.Url)
	}
	// Invert Sources — still B.
	if got := lateBindSourceToWorkerHost(b.Url, worker, []wdclient.Location{b, a}); got != b.Url {
		t.Fatalf("Sources [B,A] worker B: got %s want %s", got, b.Url)
	}
}

func TestLateBindSourceToWorkerHostNoMatchKeepsCurrent(t *testing.T) {
	a := wdclient.Location{Url: "full-01:8080"}
	b := wdclient.Location{Url: "full-02:8081"}

	if got := lateBindSourceToWorkerHost(a.Url, "other-host:9334", []wdclient.Location{a, b}); got != a.Url {
		t.Fatalf("no worker-host replica: got %s want %s", got, a.Url)
	}
	if got := lateBindSourceToWorkerHost(a.Url, "", []wdclient.Location{a, b}); got != a.Url {
		t.Fatalf("empty worker address: got %s want %s", got, a.Url)
	}
}
