package weed_server

import (
	"context"
	"net/http"
	"strings"
	"sync"
)

// A PROPFIND on a directory lists it once, then golang.org/x/net/webdav's walkFS
// throws the listing's FileInfo away and stats every child again - one filer
// lookup per entry, serially. listedEntries hands those stats back the entries the
// listing already fetched, for the lifetime of a single request.
type listedEntries struct {
	mu     sync.Mutex
	byPath map[string]*FileInfo
}

// maxListedEntries bounds the per-request memory. Past it, children fall back to
// individual lookups.
const maxListedEntries = 100000

type listedEntriesKey struct{}

func withListedEntries(ctx context.Context) context.Context {
	return context.WithValue(ctx, listedEntriesKey{}, &listedEntries{byPath: make(map[string]*FileInfo)})
}

func listedEntriesFrom(ctx context.Context) *listedEntries {
	if ctx == nil {
		return nil
	}
	listed, _ := ctx.Value(listedEntriesKey{}).(*listedEntries)
	return listed
}

// trailing slashes are optional on directory paths, so both sides normalize
func listedEntryKey(fullPath string) string {
	if len(fullPath) > 1 {
		return strings.TrimSuffix(fullPath, "/")
	}
	return fullPath
}

func (listed *listedEntries) put(fullPath string, fi *FileInfo) {
	if listed == nil {
		return
	}
	listed.mu.Lock()
	defer listed.mu.Unlock()
	if len(listed.byPath) >= maxListedEntries {
		return
	}
	listed.byPath[listedEntryKey(fullPath)] = fi
}

func (listed *listedEntries) get(fullPath string) *FileInfo {
	if listed == nil {
		return nil
	}
	listed.mu.Lock()
	defer listed.mu.Unlock()
	return listed.byPath[listedEntryKey(fullPath)]
}

type listedEntriesHandler struct {
	next http.Handler
}

func (h listedEntriesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.next.ServeHTTP(w, r.WithContext(withListedEntries(r.Context())))
}
