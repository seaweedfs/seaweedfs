package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListedEntriesTrailingSlash(t *testing.T) {
	ctx := withListedEntries(context.Background())
	listed := listedEntriesFrom(ctx)

	listed.put("/dir/child", &FileInfo{name: "/dir/child", isDirectory: true})

	if listed.get("/dir/child") == nil {
		t.Error("exact path missed")
	}
	if listed.get("/dir/child/") == nil {
		t.Error("trailing slash missed: OpenFile stats directories as dir/")
	}
	if listed.get("/dir/other") != nil {
		t.Error("unlisted path hit")
	}
}

func TestListedEntriesWithoutCache(t *testing.T) {
	var missing *listedEntries

	missing.put("/dir/child", &FileInfo{name: "/dir/child"})
	if missing.get("/dir/child") != nil {
		t.Error("nil cache returned an entry")
	}
	if listedEntriesFrom(context.Background()) != nil {
		t.Error("plain context carried a cache")
	}
	if listedEntriesFrom(nil) != nil {
		t.Error("nil context carried a cache")
	}
}

func TestListedEntriesCap(t *testing.T) {
	listed := &listedEntries{byPath: make(map[string]*FileInfo)}
	for i := 0; i < maxListedEntries+10; i++ {
		listed.put(string(rune('a'+i%26))+string(rune(i)), &FileInfo{})
	}
	if len(listed.byPath) > maxListedEntries {
		t.Errorf("cache grew to %d, past the %d cap", len(listed.byPath), maxListedEntries)
	}
}

func TestListedEntriesPerRequest(t *testing.T) {
	var caches []*listedEntries

	handler := listedEntriesHandler{next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caches = append(caches, listedEntriesFrom(r.Context()))
	})}

	for i := 0; i < 2; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("PROPFIND", "/dir/", nil))
	}

	if caches[0] == nil || caches[1] == nil {
		t.Fatal("request context carried no cache")
	}
	if caches[0] == caches[1] {
		t.Error("two requests shared one cache")
	}
}
