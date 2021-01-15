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
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0 : len(p)-1]
	}

	prefixInNamePattern, restNamePattern := splitPattern(namePattern)
	if prefixInNamePattern != "" {
		prefix = prefixInNamePattern
	}
	var missedCount int64
	var lastFileName string

	entries, hasMore, missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, startFileName, inclusive, limit, prefix, restNamePattern)

	for missedCount > 0 && err == nil {
		var makeupEntries []*Entry
		makeupEntries, hasMore, missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, lastFileName, false, missedCount, prefix, restNamePattern)
		for _, entry := range makeupEntries {
			entries = append(entries, entry)
		}
	}

	return entries, hasMore, err
}

func (f *Filer) doListPatternMatchedEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix, restNamePattern string) (matchedEntries []*Entry, hasMore bool, missedCount int64, lastFileName string, err error) {
	var foundEntries []*Entry

	foundEntries, hasMore, lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix)
	if err != nil {
		return
	}
	if len(restNamePattern) == 0 {
		return foundEntries, false, 0, lastFileName, nil
	}
	for _, entry := range foundEntries {
		nameToTest := strings.ToLower(entry.Name())
		if matched, matchErr := filepath.Match(restNamePattern, nameToTest[len(prefix):]); matchErr == nil && matched {
			matchedEntries = append(matchedEntries, entry)
		} else {
			missedCount++
		}
	}
	return
}

func (f *Filer) doListValidEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string) (entries []*Entry, hasMore bool, lastFileName string, err error) {
	var makeupEntries []*Entry
	var expiredCount int64
	entries, hasMore, expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, startFileName, inclusive, limit, prefix)
	for expiredCount > 0 && err == nil {
		makeupEntries, hasMore, expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, lastFileName, false, expiredCount, prefix)
		if err == nil {
			entries = append(entries, makeupEntries...)
		}
	}
	return
}
