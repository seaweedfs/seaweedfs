package filer_pb

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var traverseDfsPageSize uint32 = 1024

// TraverseDfs walks the tree under parentPath depth first in listing order, so
// one path tells how far it got: every entry before it was visited, and so was
// the subtree of a directory it names. progress gets these paths in walk
// order, and a walk given one as resumeAfter skips everything up to it. fn may
// run concurrently for the entries of one listing page.
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

		toVisit := page
		if resumeIn != nil {
			toVisit = page[1:]
		}
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(5)
		for _, entry := range toVisit {
			g.Go(func() error {
				if err := gctx.Err(); err != nil {
					return err
				}
				return fn(dir, entry)
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}

		for i, entry := range page {
			child := dir.Child(entry.Name)
			if entry.IsDirectory {
				var in []string
				if i == 0 {
					in = resumeIn
				}
				if err := traverseDfsDirectory(ctx, filerClient, child, in, fn, progress); err != nil {
					return err
				}
			}
			progress(child)
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
