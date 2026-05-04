package s3api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVersionListPagerNewestFirstOrderingAndPagination(t *testing.T) {
	base := time.Date(2026, 2, 6, 11, 3, 16, 0, time.UTC)
	items := []versionListItem{
		{
			key:          "k10/repo/log_20260206110316",
			versionId:    "v1",
			lastModified: base,
		},
		{
			key:          "k10/repo/log_20260206110317",
			versionId:    "v2",
			lastModified: base.Add(1 * time.Second),
		},
		{
			key:          "k10/repo/log_20260206110315",
			versionId:    "v0",
			lastModified: base.Add(-1 * time.Second),
		},
	}

	// Page 1: newest-first
	pager := newVersionListPager(1, nil, false, "")
	for _, item := range []versionListItem{items[0], items[2], items[1]} {
		pager.consider(item)
	}

	sorted := pager.itemsSorted()
	s3a := &S3ApiServer{}
	truncated, nextKeyMarker, nextVersionIdMarker, isTruncated := s3a.truncateAndSetMarkers(sorted, 1)

	require.Len(t, truncated, 1)
	require.Equal(t, items[1].key, truncated[0].key)
	require.Equal(t, items[1].versionId, truncated[0].versionId)
	require.True(t, isTruncated)
	require.Equal(t, items[1].key, nextKeyMarker)
	require.Equal(t, items[1].versionId, nextVersionIdMarker)

	// Page 2: start after marker (should return the next newest item)
	marker := &versionListItem{
		key:          items[1].key,
		versionId:    items[1].versionId,
		lastModified: items[1].lastModified,
	}
	pager2 := newVersionListPager(1, marker, false, "")
	for _, item := range items {
		pager2.consider(item)
	}

	sorted2 := pager2.itemsSorted()
	truncated2, _, _, _ := s3a.truncateAndSetMarkers(sorted2, 1)

	require.Len(t, truncated2, 1)
	require.Equal(t, items[0].key, truncated2[0].key)
	require.Equal(t, items[0].versionId, truncated2[0].versionId)
}
