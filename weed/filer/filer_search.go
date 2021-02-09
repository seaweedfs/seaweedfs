package filer

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/util"
	"path/filepath"
	"strings"
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
func (f *Filer) ListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, namePattern string) (entries []*Entry, hasMore bool, err error) {

	_, err = f.StreamListDirectoryEntries(ctx, p, startFileName, inclusive, limit+1, prefix, namePattern, func(entry *Entry) bool {
		entries = append(entries, entry)
		return true
	})

	hasMore = int64(len(entries)) >= limit+1
	if hasMore {
		entries = entries[:limit]
	}

	return entries, hasMore, err
}

// For now, prefix and namePattern are mutually exclusive
func (f *Filer) StreamListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, namePattern string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0 : len(p)-1]
	}

	prefixInNamePattern, restNamePattern := splitPattern(namePattern)
	if prefixInNamePattern != "" {
		prefix = prefixInNamePattern
	}
	var missedCount int64

	missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, startFileName, inclusive, limit, prefix, restNamePattern, eachEntryFunc)

	for missedCount > 0 && err == nil {
		missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, lastFileName, false, missedCount, prefix, restNamePattern, eachEntryFunc)
	}

	return
}

func (f *Filer) doListPatternMatchedEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix, restNamePattern string, eachEntryFunc ListEachEntryFunc) (missedCount int64, lastFileName string, err error) {

	if len(restNamePattern) == 0 {
		lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix, eachEntryFunc)
		return 0, lastFileName, err
	}

	lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix, func(entry *Entry) bool {
		nameToTest := strings.ToLower(entry.Name())
		if matched, matchErr := filepath.Match(restNamePattern, nameToTest[len(prefix):]); matchErr == nil && matched {
			if !eachEntryFunc(entry) {
				return false
			}
		} else {
			missedCount++
		}
		return true
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
