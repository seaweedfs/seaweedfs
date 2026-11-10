package filer_pb

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var traverseDfsPageSize uint32 = 1024

// TraverseDfs walks the tree under parentPath depth first in listing order, so
// one path tells how far it got: every entry before it was visited, and so was
// the subtree of a directory it names. progress gets these paths in walk
// order, and a walk given one as resumeAfter skips everything up to it. fn may
// run concurrently for the files of one directory.
func TraverseDfs(ctx context.Context, filerClient FilerClient, parentPath, resumeAfter util.FullPath, fn func(parentPath util.FullPath, entry *Entry) error, progress func(walked util.FullPath)) error {
	parentPath = util.NormalizePath(string(parentPath))
	var resume []string
	if resumeAfter != "" && resumeAfter != parentPath && resumeAfter.IsUnder(parentPath) {
		glog.V(0).Infof("traverse %s after %s", parentPath, resumeAfter)
		resume = util.FullPath(strings.TrimPrefix(string(resumeAfter), strings.TrimSuffix(string(parentPath), "/"))).Split()
	}
	return traverseDfsDirectory(ctx, filerClient, parentPath, resume, fn, progress)
}

func traverseDfsDirectory(ctx context.Context, filerClient FilerClient, dir util.FullPath, resume []string, fn func(parentPath util.FullPath, entry *Entry) error, progress func(walked util.FullPath)) error {
	startFrom, inclusive := "", false
	if len(resume) > 0 {
		startFrom, inclusive = resume[0], true
	}
	for {
		var page []*Entry
		if err := List(ctx, filerClient, string(dir), "", func(entry *Entry, isLast bool) error {
			page = append(page, entry)
			return nil
		}, startFrom, inclusive, traverseDfsPageSize); err != nil {
			return err
		}

		listed := page
		// the entry the walk stopped at: done with its subtree when it ends the
		// path, else a directory to resume inside
		var resumeIn []string
		if inclusive && len(page) > 0 && page[0].Name == startFrom {
			if len(resume) == 1 {
				page = page[1:]
			} else if page[0].IsDirectory {
				resumeIn = resume[1:]
			}
		}

		if resumeIn != nil {
			child := dir.Child(page[0].Name)
			if err := traverseDfsDirectory(ctx, filerClient, child, resumeIn, fn, progress); err != nil {
				return err
			}
			progress(child)
			page = page[1:]
		}
		for len(page) > 0 {
			run := page
			if i := slices.IndexFunc(page, func(entry *Entry) bool { return entry.IsDirectory }); i >= 0 {
				run = page[:i+1]
			}
			if err := visitDfsRun(ctx, dir, run, fn, progress); err != nil {
				return err
			}
			if last := run[len(run)-1]; last.IsDirectory {
				child := dir.Child(last.Name)
				if err := traverseDfsDirectory(ctx, filerClient, child, nil, fn, progress); err != nil {
					return err
				}
				progress(child)
			}
			page = page[len(run):]
		}

		if uint32(len(listed)) < traverseDfsPageSize {
			return nil
		}
		last := listed[len(listed)-1].Name
		if !inclusive && last == startFrom {
			return fmt.Errorf("list %s: pagination stuck at %q", dir, startFrom)
		}
		startFrom, inclusive = last, false
	}
}

// visitDfsRun calls fn concurrently on entries that end at the first
// directory, and reports each file once every entry before it is done. The
// caller reports the directory after its subtree.
func visitDfsRun(ctx context.Context, dir util.FullPath, run []*Entry, fn func(parentPath util.FullPath, entry *Entry) error, progress func(walked util.FullPath)) error {
	var mu sync.Mutex
	done := make([]bool, len(run))
	next := 0
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)
	for i, entry := range run {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			if err := fn(dir, entry); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			done[i] = true
			for next < len(run) && done[next] && !run[next].IsDirectory {
				progress(dir.Child(run[next].Name))
				next++
			}
			return nil
		})
	}
	return g.Wait()
}
