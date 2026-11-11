package filer_pb

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func startTreeFiler(t *testing.T, tree map[string][]string) *stubFilerClient {
	t.Helper()
	server := &stubFiler{}
	server.listings = func(req *ListEntriesRequest, send func(*Entry) error) error {
		names := append([]string(nil), tree[req.Directory]...)
		sort.Strings(names)
		sent := uint32(0)
		for _, name := range names {
			if req.StartFromFileName != "" && (name < req.StartFromFileName || name == req.StartFromFileName && !req.InclusiveStartFrom) {
				continue
			}
			if sent == req.Limit {
				break
			}
			_, isDir := tree[string(util.FullPath(req.Directory).Child(name))]
			if err := send(&Entry{Name: name, IsDirectory: isDir}); err != nil {
				return err
			}
			sent++
		}
		return nil
	}
	return startStubFiler(t, server)
}

func traverseDfsForTest(t *testing.T, filerClient FilerClient, root, resumeAfter util.FullPath) (visited map[string]int, progressed []string) {
	t.Helper()
	var mu sync.Mutex
	visited = make(map[string]int)
	err := TraverseDfs(context.Background(), filerClient, root, resumeAfter, func(parentPath util.FullPath, entry *Entry) error {
		mu.Lock()
		visited[string(parentPath.Child(entry.Name))]++
		mu.Unlock()
		return nil
	}, func(walked util.FullPath) {
		progressed = append(progressed, string(walked))
	})
	if err != nil {
		t.Fatal(err)
	}
	return visited, progressed
}

func TestTraverseDfsResumesAfterEachProgressPath(t *testing.T) {
	saved := traverseDfsPageSize
	traverseDfsPageSize = 2
	t.Cleanup(func() { traverseDfsPageSize = saved })

	filerClient := startTreeFiler(t, map[string][]string{
		"/":    {"a", "b", "c", "d", "e"},
		"/a":   {"x", "y", "z"},
		"/a/y": {"1", "2", "3"},
		"/c":   {},
		"/e":   {"f"},
	})

	visited, progressed := traverseDfsForTest(t, filerClient, "/", "")
	want := []string{"/a/x", "/a/y/1", "/a/y/2", "/a/y/3", "/a/y", "/a/z", "/a", "/b", "/c", "/d", "/e/f", "/e"}
	if !reflect.DeepEqual(progressed, want) {
		t.Fatalf("progress %v, want %v", progressed, want)
	}
	if len(visited) != len(want) {
		t.Fatalf("visited %v, want each of %v", visited, want)
	}
	for _, p := range want {
		if visited[p] != 1 {
			t.Fatalf("visited %s %d times, want once", p, visited[p])
		}
	}

	for i, resumeAfter := range want {
		visited, progressed := traverseDfsForTest(t, filerClient, "/", util.FullPath(resumeAfter))
		if !slices.Equal(progressed, want[i+1:]) {
			t.Fatalf("after %s: progress %v, want %v", resumeAfter, progressed, want[i+1:])
		}
		for _, p := range want[i+1:] {
			wantVisits := 1
			if strings.HasPrefix(resumeAfter, p+"/") {
				wantVisits = 0
			}
			if visited[p] != wantVisits {
				t.Fatalf("after %s: visited %s %d times, want %d", resumeAfter, p, visited[p], wantVisits)
			}
			delete(visited, p)
		}
		if len(visited) != 0 {
			t.Fatalf("after %s: visited %v again", resumeAfter, visited)
		}
	}
}

func TestTraverseDfsResumeAfterAMissingOrForeignPath(t *testing.T) {
	filerClient := startTreeFiler(t, map[string][]string{
		"/":    {"a", "b"},
		"/a":   {"x", "y"},
		"/a/y": {"1", "2", "3"},
	})

	_, progressed := traverseDfsForTest(t, filerClient, "/a/", "/a/y/15")
	if want := []string{"/a/y/2", "/a/y/3", "/a/y"}; !reflect.DeepEqual(progressed, want) {
		t.Fatalf("after a deleted path: progress %v, want %v", progressed, want)
	}

	_, progressed = traverseDfsForTest(t, filerClient, "/a", "/b")
	if want := []string{"/a/x", "/a/y/1", "/a/y/2", "/a/y/3", "/a/y"}; !reflect.DeepEqual(progressed, want) {
		t.Fatalf("after a path outside the walk: progress %v, want %v", progressed, want)
	}
}

func TestTraverseDfsReportsFilesBeforeTheirPageFinishes(t *testing.T) {
	filerClient := startTreeFiler(t, map[string][]string{
		"/":  {"a", "b", "c", "d"},
		"/d": {"x"},
	})

	var mu sync.Mutex
	var progressed []string
	reachedB := make(chan struct{})
	err := TraverseDfs(context.Background(), filerClient, "/", "", func(parentPath util.FullPath, entry *Entry) error {
		if entry.Name != "c" {
			return nil
		}
		select {
		case <-reachedB:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("no progress while %s was in flight", entry.Name)
		}
	}, func(walked util.FullPath) {
		mu.Lock()
		defer mu.Unlock()
		progressed = append(progressed, string(walked))
		if walked == "/b" {
			close(reachedB)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"/a", "/b", "/c", "/d/x", "/d"}; !reflect.DeepEqual(progressed, want) {
		t.Fatalf("progress %v, want %v", progressed, want)
	}
}
