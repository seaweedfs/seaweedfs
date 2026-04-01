package filer

import (
	"context"
	"math"
	"path/filepath"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func splitPattern(pattern string) (prefix string, restPattern string) {
	position := strings.Index(pattern, "*")
	if position >= 0 {
		return pattern[:position], pattern[position:]
	}
	position = strings.Index(pattern, "?")
	if position >= 0 {
		return pattern[:position], pattern[position:]
	}
	return "", restPattern
}

// For now, prefix and namePattern are mutually exclusive
func (f *Filer) ListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, namePattern string, namePatternExclude string) (entries []*Entry, hasMore bool, err error) {

	if limit > math.MaxInt32-1 {
		limit = math.MaxInt32 - 1
	}

	_, err = f.StreamListDirectoryEntries(ctx, p, startFileName, inclusive, limit+1, prefix, namePattern, namePatternExclude, func(entry *Entry) (bool, error) {
		entries = append(entries, entry)
		return true, nil
	})

	hasMore = int64(len(entries)) >= limit+1
	if hasMore {
		entries = entries[:limit]
	}

	return entries, hasMore, err
}

// CountDirectoryEntries counts entries in a directory up to limit
func (f *Filer) CountDirectoryEntries(ctx context.Context, p util.FullPath, limit int) (count int, err error) {
	entries, hasMore, err := f.ListDirectoryEntries(ctx, p, "", false, int64(limit), "", "", "")
	if err != nil {
		return 0, err
	}
	count = len(entries)
	if hasMore {
		count = limit // At least this many
	}
	return count, nil
}

// For now, prefix and namePattern are mutually exclusive
func (f *Filer) StreamListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, namePattern string, namePatternExclude string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0 : len(p)-1]
	}

	prefixInNamePattern, restNamePattern := splitPattern(namePattern)
	if prefixInNamePattern != "" {
		prefix = prefixInNamePattern
	}
	var missedCount int64

	missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, startFileName, inclusive, limit, prefix, restNamePattern, namePatternExclude, eachEntryFunc)

	for missedCount > 0 && err == nil {
		missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, lastFileName, false, missedCount, prefix, restNamePattern, namePatternExclude, eachEntryFunc)
	}

	return
}

func (f *Filer) doListPatternMatchedEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix, restNamePattern string, namePatternExclude string, eachEntryFunc ListEachEntryFunc) (missedCount int64, lastFileName string, err error) {

	if len(restNamePattern) == 0 && len(namePatternExclude) == 0 {
		lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix, eachEntryFunc)
		return 0, lastFileName, err
	}

	lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix, func(entry *Entry) (bool, error) {
		nameToTest := entry.Name()
		if len(namePatternExclude) > 0 {
			if matched, matchErr := filepath.Match(namePatternExclude, nameToTest); matchErr == nil && matched {
				missedCount++
				return true, nil
			}
		}
		if len(restNamePattern) > 0 {
			if matched, matchErr := filepath.Match(restNamePattern, nameToTest[len(prefix):]); matchErr == nil && !matched {
				missedCount++
				return true, nil
			}
		}

		res, resErr := eachEntryFunc(entry)

		if resErr != nil {
			return false, resErr
		}

		if !res {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return
	}
	return
}

func (f *Filer) doListValidEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	var expiredCount int64
	expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, startFileName, inclusive, limit, prefix, eachEntryFunc)
	for expiredCount > 0 && err == nil {
		expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, lastFileName, false, expiredCount, prefix, eachEntryFunc)
	}
	return
}
