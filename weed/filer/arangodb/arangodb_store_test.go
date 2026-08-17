package arangodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestStore(t *testing.T) *ArangodbStore {
	t.Helper()

	if os.Getenv("RUN_ARANGODB_TESTS") != "1" {
		t.Skip("arangodb tests are disabled. Start an arangodb server and set RUN_ARANGODB_TESTS=1 to enable, ARANGODB_ADDR defaults to http://127.0.0.1:8529.")
	}

	addr := os.Getenv("ARANGODB_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8529"
	}
	user := os.Getenv("ARANGODB_USER")
	if user == "" {
		user = "root"
	}

	store := &ArangodbStore{databaseName: fmt.Sprintf("seaweed_test_%d", time.Now().UnixNano())}
	store.buckets = make(map[string]driver.Collection, 3)
	if err := store.connection([]string{addr}, user, os.Getenv("ARANGODB_PASSWORD"), true); err != nil {
		t.Fatalf("connect to arangodb at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		if err := store.database.Remove(context.Background()); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
	return store
}

func insertTestEntry(t *testing.T, store *ArangodbStore, path string) {
	t.Helper()
	if err := store.InsertEntry(context.Background(), &filer.Entry{FullPath: util.FullPath(path)}); err != nil {
		t.Fatalf("insert %s: %v", path, err)
	}
}

func countDocuments(t *testing.T, store *ArangodbStore, bucket string) int64 {
	t.Helper()
	collection, err := store.ensureBucket(context.Background(), bucket)
	if err != nil {
		t.Fatalf("ensure bucket %s: %v", bucket, err)
	}
	count, err := collection.Count(context.Background())
	if err != nil {
		t.Fatalf("count %s: %v", bucket, err)
	}
	return count
}

func listNames(t *testing.T, store *ArangodbStore, dir util.FullPath, prefix string) []string {
	t.Helper()
	var names []string
	_, err := store.ListDirectoryPrefixedEntries(context.Background(), dir, "", true, 100, prefix,
		func(entry *filer.Entry) (bool, error) {
			names = append(names, entry.Name())
			return true, nil
		})
	if err != nil {
		t.Fatalf("list %s prefix %q: %v", dir, prefix, err)
	}
	return names
}

// AQL operators in a list prefix must be matched literally, never executed.
func TestListDirectoryPrefixedEntriesAqlInjection(t *testing.T) {
	store := newTestStore(t)

	insertTestEntry(t, store, "/buckets/victim/secret")
	insertTestEntry(t, store, "/buckets/tenant/regular")

	injection := "x\" && false || (FOR q IN `victim` LIMIT 1 UPDATE q WITH {injected:\"yes\"} IN `victim` RETURN \"\") || \""
	if names := listNames(t, store, util.FullPath("/buckets/tenant"), injection); len(names) != 0 {
		t.Errorf("expected no match for injected prefix, got %v", names)
	}

	var victim Model
	collection, err := store.ensureBucket(context.Background(), "victim")
	if err != nil {
		t.Fatalf("ensure bucket victim: %v", err)
	}
	if _, err := collection.ReadDocument(context.Background(), hashString("/buckets/victim/secret"), &victim); err != nil {
		t.Fatalf("read victim document: %v", err)
	}
	if victim.Name != "secret" {
		t.Errorf("victim document was modified across buckets: %+v", victim)
	}
}

// A prefix full of AQL operators must not be evaluated as a query.
func TestListDirectoryPrefixedEntriesPrefixNotEvaluated(t *testing.T) {
	store := newTestStore(t)

	insertTestEntry(t, store, "/buckets/tenant/a")
	insertTestEntry(t, store, "/buckets/tenant/b")
	insertTestEntry(t, store, "/buckets/tenant/c")

	start := time.Now()
	if names := listNames(t, store, util.FullPath("/buckets/tenant"), "x\" && false || TO_STRING(SLEEP(1)) && false || \""); len(names) != 0 {
		t.Errorf("expected no match for injected prefix, got %v", names)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("SLEEP() in prefix was executed, listing took %v", elapsed)
	}
}

// Quotes and newlines are ordinary object-name characters and must round-trip.
func TestListDirectoryPrefixedEntriesQuotedNames(t *testing.T) {
	store := newTestStore(t)

	names := []string{"plain", "quo\"te", "new\nline", "back\\slash"}
	for _, name := range names {
		insertTestEntry(t, store, "/buckets/tenant/"+name)
	}

	for _, name := range names {
		if got := listNames(t, store, util.FullPath("/buckets/tenant"), name); len(got) != 1 || got[0] != name {
			t.Errorf("prefix %q listed %v, want exactly [%q]", name, got, name)
		}
	}

	if got := listNames(t, store, util.FullPath("/buckets/tenant"), ""); len(got) != len(names) {
		t.Errorf("empty prefix listed %v, want %d entries", got, len(names))
	}
}

// A directory name full of AQL operators must only drop that directory's own children.
func TestDeleteFolderChildrenAqlInjection(t *testing.T) {
	store := newTestStore(t)

	for i := 0; i < 5; i++ {
		insertTestEntry(t, store, fmt.Sprintf("/buckets/tenant/keep%d", i))
	}
	injection := "x\" || true || \""
	insertTestEntry(t, store, "/buckets/tenant/"+injection+"/child")

	if err := store.DeleteFolderChildren(context.Background(), util.FullPath("/buckets/tenant/"+injection)); err != nil {
		t.Fatalf("delete folder children: %v", err)
	}

	if got := countDocuments(t, store, "tenant"); got != 5 {
		t.Errorf("tenant collection holds %d documents, want the 5 unrelated ones", got)
	}
}

// startFileName is request-controlled too and must not break out of the filter.
func TestListDirectoryEntriesQuotedStartFileName(t *testing.T) {
	store := newTestStore(t)

	insertTestEntry(t, store, "/buckets/tenant/a")
	insertTestEntry(t, store, "/buckets/tenant/z")

	var names []string
	_, err := store.ListDirectoryEntries(context.Background(), util.FullPath("/buckets/tenant"), "z\" || true || \"", false, 100,
		func(entry *filer.Entry) (bool, error) {
			names = append(names, entry.Name())
			return true, nil
		})
	if err != nil {
		t.Fatalf("list with quoted start file: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("listed %v, want nothing sorted after the literal start name", names)
	}
}
