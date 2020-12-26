package filer

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/util"
	"path/filepath"
	"strings"
)

func splitPattern(pattern string) (prefix string, restPattern string, hasUpper bool) {
	for i := 0; i < len(pattern); i++ {
		hasUpper = hasUpper || ('A' <= pattern[i] && pattern[i] <= 'Z')
	}
	position := strings.Index(pattern, "*")
	if position >= 0 {
		return pattern[:position], pattern[position:], hasUpper
	}
	position = strings.Index(pattern, "?")
	if position >= 0 {
		return pattern[:position], pattern[position:], hasUpper
	}
	return "", restPattern, hasUpper
}

func (f *Filer) ListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int, namePattern string) (entries []*Entry, err error) {
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0 : len(p)-1]
	}

	prefix, restNamePattern, hasUpper := splitPattern(namePattern)
	var missedCount int
	var lastFileName string

	entries, missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, startFileName, inclusive, limit, prefix, restNamePattern, hasUpper)

	for missedCount > 0 && err == nil {
		var makeupEntries []*Entry
		makeupEntries, missedCount, lastFileName, err = f.doListPatternMatchedEntries(ctx, p, lastFileName, false, missedCount, prefix, restNamePattern, hasUpper)
		for _, entry := range makeupEntries {
			entries = append(entries, entry)
		}
	}

	return entries, err
}

func (f *Filer) doListPatternMatchedEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int, prefix, restNamePattern string, hasUpper bool) (matchedEntries []*Entry, missedCount int, lastFileName string, err error) {
	var foundEntries []*Entry

	foundEntries, lastFileName, err = f.doListValidEntries(ctx, p, startFileName, inclusive, limit, prefix)
	if err != nil {
		return
	}
	if len(restNamePattern) == 0 {
		return foundEntries, 0, lastFileName, nil
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

func (f *Filer) doListValidEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*Entry, lastFileName string, err error) {
	var makeupEntries []*Entry
	var expiredCount int
	entries, expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, startFileName, inclusive, limit, prefix)
	for expiredCount > 0 && err == nil {
		makeupEntries, expiredCount, lastFileName, err = f.doListDirectoryEntries(ctx, p, lastFileName, false, expiredCount, prefix)
		if err == nil {
			entries = append(entries, makeupEntries...)
		}
	}
	return
}
