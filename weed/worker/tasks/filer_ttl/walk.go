package filer_ttl

import (
	"context"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const walkPageSize = 1024

type walkStats struct {
	directories int64
	entries     int64
}

// ttlLocationPrefixes returns the TTL path rules that no other TTL rule
// encloses, so each subtree is walked once.
func ttlLocationPrefixes(fc *filer.FilerConf) []string {
	var prefixes []string
	for _, rule := range fc.ToProto().Locations {
		ttl, err := needle.ReadTTL(rule.GetTtl())
		if err != nil || ttl.ToSeconds() == 0 {
			continue
		}
		prefixes = append(prefixes, rule.LocationPrefix)
	}
	sort.Strings(prefixes)
	var outer []string
	for _, prefix := range prefixes {
		if n := len(outer); n > 0 && strings.HasPrefix(prefix, outer[n-1]) {
			continue
		}
		outer = append(outer, prefix)
	}
	return outer
}

// walkLocationPrefix lists every entry under a filer.conf location prefix.
// Listing is what removes expired rows: the filer drops each expired entry
// it meets and deletes the directories that leaves empty.
func walkLocationPrefix(ctx context.Context, client filer_pb.SeaweedFilerClient, locationPrefix string, stats *walkStats) error {
	ctx = filer_pb.WithChunksOmitted(ctx)
	if strings.HasSuffix(locationPrefix, "/") {
		return walkDirectory(ctx, client, util.FullPath(locationPrefix), "", stats)
	}
	dir, name := util.FullPath(locationPrefix).DirAndName()
	return walkDirectory(ctx, client, util.FullPath(dir), name, stats)
}

func walkDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, dir util.FullPath, namePrefix string, stats *walkStats) error {
	stats.directories++
	startFrom := ""
	for {
		var subDirs []util.FullPath
		listed := 0
		err := filer_pb.SeaweedList(ctx, client, string(dir), namePrefix, func(entry *filer_pb.Entry, isLast bool) error {
			listed++
			startFrom = entry.Name
			stats.entries++
			if entry.IsDirectory {
				subDirs = append(subDirs, dir.Child(entry.Name))
			}
			return nil
		}, startFrom, false, walkPageSize)
		if err != nil {
			return err
		}
		for _, subDir := range subDirs {
			if err := walkDirectory(ctx, client, subDir, "", stats); err != nil {
				return err
			}
		}
		if listed < walkPageSize {
			return nil
		}
	}
}
