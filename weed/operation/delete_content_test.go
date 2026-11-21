package operation

import (
	"context"
	"net/http"
	"testing"
)

func TestDeleteFileIdsWithLookupVolumeId_NoActiveLocations(t *testing.T) {
	fileIds := []string{"3,01010101", "3,02020202"}
	lookupFunc := func(vids []string) (map[string]*LookupResult, error) {
		results := make(map[string]*LookupResult)
		for _, vid := range vids {
			results[vid] = &LookupResult{
				VolumeOrFileId: vid,
				Locations:      nil,
			}
		}
		return results, nil
	}

	results := DeleteFileIdsWithLookupVolumeId(context.Background(), nil, fileIds, lookupFunc)
	if len(results) != len(fileIds) {
		t.Fatalf("expected %d results, got %d", len(fileIds), len(results))
	}

	for _, res := range results {
		if res.Status != http.StatusNotFound {
			t.Errorf("expected status %d, got %d", http.StatusNotFound, res.Status)
		}
		if res.Error != "lookup error: volume 3 has no active locations" {
			t.Errorf("unexpected error message: %q", res.Error)
		}
	}
}

func TestDeleteFileIdsWithLookupVolumeId_MissingVolumeInLookup(t *testing.T) {
	fileIds := []string{"5,01010101"}
	lookupFunc := func(vids []string) (map[string]*LookupResult, error) {
		// Return empty map to simulate missing volume in lookup response
		return make(map[string]*LookupResult), nil
	}

	results := DeleteFileIdsWithLookupVolumeId(context.Background(), nil, fileIds, lookupFunc)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Status != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, results[0].Status)
	}
	if results[0].Error != "lookup error: volume 5 not found" {
		t.Errorf("unexpected error message: %q", results[0].Error)
	}
}
