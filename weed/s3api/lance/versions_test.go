package lance

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
)

func newVersionHarness(t *testing.T) *testHarness {
	t.Helper()
	h := newTestHarness(t)
	h.server.SetManagedVersioning(true)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/declare", `{}`, http.StatusOK)
	return h
}

// Managed versioning exists to give a commit a real put-if-not-exists. Two
// writers racing for the same version must not both believe they won, because
// the loser is the one that rebases; if both win, one commit is lost.
func TestOnlyOneWriterReservesAVersion(t *testing.T) {
	h := newVersionHarness(t)

	const writers = 8
	var wg sync.WaitGroup
	results := make([]int, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"version":1,"manifest_path":"_versions/1.manifest-%d"}`, i)
			results[i] = h.do(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/create", body).Code
		}(i)
	}
	wg.Wait()

	won, lost := 0, 0
	for _, code := range results {
		switch code {
		case http.StatusOK:
			won++
		case http.StatusConflict:
			lost++
		default:
			t.Fatalf("unexpected status %d from a version reservation", code)
		}
	}
	if won != 1 {
		t.Fatalf("%d writers reserved version 1; exactly one may win", won)
	}
	if lost != writers-1 {
		t.Fatalf("%d writers lost, want %d", lost, writers-1)
	}

	// The winner's manifest is the one the namespace kept.
	described := decode[DescribeTableVersionResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/describe", `{"version":1}`, http.StatusOK))
	if described.Version.Version != 1 || described.Version.ManifestPath == "" {
		t.Fatalf("described version = %+v", described.Version)
	}
	if described.Version.TimestampMillis == 0 {
		t.Fatal("version record has no timestamp")
	}
}

func TestVersionListingAndDeletion(t *testing.T) {
	h := newVersionHarness(t)

	for v := 1; v <= 5; v++ {
		body := fmt.Sprintf(`{"version":%d,"manifest_path":"_versions/%d.manifest","manifest_size":%d,"naming_scheme":"V2"}`, v, v, v*100)
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/create", body, http.StatusOK)
	}

	listed := decode[ListTableVersionsResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/list", `{}`, http.StatusOK))
	if len(listed.Versions) != 5 {
		t.Fatalf("listed %d versions, want 5", len(listed.Versions))
	}
	for i, v := range listed.Versions {
		if v.Version != int64(i+1) {
			t.Fatalf("versions out of order: %+v", listed.Versions)
		}
	}

	descending := decode[ListTableVersionsResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/list", `{"descending":true,"limit":2}`, http.StatusOK))
	if len(descending.Versions) != 2 || descending.Versions[0].Version != 5 || descending.Versions[1].Version != 4 {
		t.Fatalf("descending listing = %+v", descending.Versions)
	}

	// Ranges are half-open, so this drops 1 and 2 and keeps 3.
	deleted := decode[BatchDeleteTableVersionsResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/delete",
			`{"ranges":[{"start":1,"end":3}]}`, http.StatusOK))
	if deleted.Deleted != 2 {
		t.Fatalf("deleted %d versions, want 2", deleted.Deleted)
	}
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/describe", `{"version":1}`, http.StatusNotFound)
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/describe", `{"version":3}`, http.StatusOK)
}

// With managed versioning off the namespace does not claim to order commits, so
// it must say so rather than half-implementing the contract.
func TestVersionOperationsAreOffByDefault(t *testing.T) {
	h := newTestHarness(t)
	h.createBucket(t, "analytics")
	h.mustDo(t, http.MethodPost, "/v1/namespace/analytics$sales/create", `{}`, http.StatusOK)

	declared := decode[DeclareTableResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/declare", `{}`, http.StatusOK))
	if declared.ManagedVersioning {
		t.Fatal("managed_versioning must default to false")
	}
	h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/version/create",
		`{"version":1,"manifest_path":"_versions/1.manifest"}`, http.StatusNotImplemented)
}

func TestManagedVersioningIsAdvertisedWhenOn(t *testing.T) {
	h := newVersionHarness(t)
	described := decode[DescribeTableResponse](t,
		h.mustDo(t, http.MethodPost, "/v1/table/analytics$sales$orders/describe", `{}`, http.StatusOK))
	if !described.ManagedVersioning {
		t.Fatal("describe must advertise managed_versioning so the client routes commits here")
	}
}
