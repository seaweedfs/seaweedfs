package weed_server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLookupDirectoryEntriesRejectsInvalidRequests(t *testing.T) {
	valid := &filer_pb.LookupDirectoryEntryRequest{Directory: "/batch", Name: "key"}
	tests := []struct {
		name     string
		req      *filer_pb.LookupDirectoryEntriesRequest
		wantCode codes.Code
	}{
		{name: "nil request", wantCode: codes.InvalidArgument},
		{name: "empty batch", req: &filer_pb.LookupDirectoryEntriesRequest{}, wantCode: codes.InvalidArgument},
		{name: "oversized", req: repeatedBatchLookupRequest(maxFilerBatchLookupRequests+1, valid), wantCode: codes.ResourceExhausted},
		{name: "nil item", req: &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{nil}}, wantCode: codes.InvalidArgument},
		{name: "relative directory", req: &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: "batch", Name: "key"}}}, wantCode: codes.InvalidArgument},
		{name: "empty name", req: &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch"}}}, wantCode: codes.InvalidArgument},
		{name: "nested name", req: &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch", Name: "a/b"}}}, wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			_, err := lookupDirectoryEntries(context.Background(), tt.req, 17,
				func(context.Context, util.FullPath) (*filer.Entry, int64, error) {
					called = true
					return nil, 0, nil
				},
				func(context.Context, []string) (map[string][]wdclient.Location, error) {
					called = true
					return nil, nil
				},
			)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("error code = %v, want %v: %v", status.Code(err), tt.wantCode, err)
			}
			if called {
				t.Fatal("invalid request reached lookup implementation")
			}
		})
	}
}

func TestLookupDirectoryEntriesAcceptsMaximumBatch(t *testing.T) {
	req := repeatedBatchLookupRequest(maxFilerBatchLookupRequests, &filer_pb.LookupDirectoryEntryRequest{
		Directory: "/batch",
		Name:      "key",
	})
	response, err := lookupDirectoryEntries(context.Background(), req, 13,
		func(context.Context, util.FullPath) (*filer.Entry, int64, error) {
			return nil, 99, filer_pb.ErrNotFound
		},
		func(context.Context, []string) (map[string][]wdclient.Location, error) {
			t.Fatal("volume lookup called for an all-miss batch")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("maximum-sized batch failed: %v", err)
	}
	if got := len(response.Results); got != maxFilerBatchLookupRequests {
		t.Fatalf("result count = %d, want %d", got, maxFilerBatchLookupRequests)
	}
	for _, index := range []int{0, maxFilerBatchLookupRequests - 1} {
		if got := response.Results[index]; got.LogTsNs != 99 || got.LogSignature != 13 {
			t.Fatalf("miss fence[%d] = (%d,%d), want (99,13)", index, got.LogTsNs, got.LogSignature)
		}
	}
}

func TestLookupDirectoryEntriesUsesFilerSignatureForFoundAndMiss(t *testing.T) {
	store := newRenameTestStore()
	if err := store.InsertEntry(context.Background(), newFileEntry("/batch/found", 1)); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
	testFiler := newRenameTestFiler(t, store)
	testFiler.Signature = 73
	server := &FilerServer{
		filer:          testFiler,
		entryLockTable: util.NewLockTable[util.FullPath](),
	}

	response, err := server.LookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests: []*filer_pb.LookupDirectoryEntryRequest{
			{Directory: "/batch", Name: "found"},
			{Directory: "/batch", Name: "missing"},
		},
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	if !response.Results[0].Found || response.Results[1].Found {
		t.Fatalf("unexpected found states: %v, %v", response.Results[0].Found, response.Results[1].Found)
	}
	for index, result := range response.Results {
		if result.LogSignature != 73 || result.LogTsNs == 0 {
			t.Fatalf("result[%d] fence = (%d,%d), want nonzero timestamp and signature 73", index, result.LogTsNs, result.LogSignature)
		}
	}
}

func TestPopulateBatchReadAuthMintsExactFidCapabilities(t *testing.T) {
	response := &filer_pb.LookupDirectoryEntriesResponse{Results: []*filer_pb.LookupDirectoryEntryResult{
		{Found: true, Entry: &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{
			{FileId: "7,abc"}, {FileId: "8,def"}, {FileId: "7,abc"},
		}}},
		{Found: false},
	}}
	populateBatchReadAuth(response, func(fid string) string { return "jwt:" + fid })
	if !reflect.DeepEqual(response.ReadAuth, map[string]string{
		"7,abc": "jwt:7,abc",
		"8,def": "jwt:8,def",
	}) {
		t.Fatalf("read auth = %#v", response.ReadAuth)
	}
}

func TestPopulateBatchReadAuthDoesNotInventCapabilitiesWhenSigningIsDisabled(t *testing.T) {
	response := &filer_pb.LookupDirectoryEntriesResponse{Results: []*filer_pb.LookupDirectoryEntryResult{
		{Found: true, Entry: &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "7,abc"}}}},
	}}
	populateBatchReadAuth(response, func(string) string { return "" })
	if len(response.ReadAuth) != 0 {
		t.Fatalf("read auth must be empty without a signing key: %#v", response.ReadAuth)
	}
}

func TestLookupDirectoryEntriesPreservesOrderBoundsParallelismAndDeduplicatesVolumes(t *testing.T) {
	const requestCount = 96
	requests := make([]*filer_pb.LookupDirectoryEntryRequest, requestCount)
	for index := range requests {
		requests[index] = &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/batch",
			Name:      fmt.Sprintf("item-%03d", index),
		}
	}

	var active atomic.Int32
	var activeMax atomic.Int32
	findEntry := func(_ context.Context, path util.FullPath) (*filer.Entry, int64, error) {
		index, err := strconv.Atoi(strings.TrimPrefix(string(path), "/batch/item-"))
		if err != nil {
			return nil, 0, err
		}
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := activeMax.Load()
			if current <= previous || activeMax.CompareAndSwap(previous, current) {
				break
			}
		}
		// Reverse completion order within each wave so append-based implementations fail.
		time.Sleep(time.Duration(requestCount-index) * 50 * time.Microsecond)
		if index == 7 {
			return nil, int64(1000 + index), errors.New("lookup failed")
		}
		if index == 11 {
			return nil, int64(1000 + index), filer_pb.ErrNotFound
		}
		volumeID := uint32(index%3 + 1)
		chunk := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: volumeID, FileKey: uint64(index + 1), Cookie: 17}}
		if index == 5 {
			chunk = &filer_pb.FileChunk{FileId: "2,0294cbb9892b"}
		}
		return &filer.Entry{FullPath: path, Chunks: []*filer_pb.FileChunk{chunk, chunk}}, int64(1000 + index), nil
	}

	volumeLookupCalls := 0
	var lookedUpVolumeIDs []string
	lookupVolumes := func(_ context.Context, volumeIDs []string) (map[string][]wdclient.Location, error) {
		volumeLookupCalls++
		lookedUpVolumeIDs = append([]string(nil), volumeIDs...)
		return map[string][]wdclient.Location{
			"1": {{Url: "volume-1:8080"}},
			"2": {{Url: "volume-2:8080"}},
			"3": {{Url: "volume-3:8080"}},
		}, nil
	}

	const logSignature = 0x1234
	response, err := lookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{Requests: requests}, logSignature, findEntry, lookupVolumes)
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	if got := activeMax.Load(); got <= 1 || got > maxFilerBatchLookupWorkers {
		t.Fatalf("parallel workers peaked at %d, want 2..%d", got, maxFilerBatchLookupWorkers)
	}
	if volumeLookupCalls != 1 {
		t.Fatalf("volume lookup calls = %d, want 1", volumeLookupCalls)
	}
	if want := []string{"1", "2", "3"}; !reflect.DeepEqual(lookedUpVolumeIDs, want) {
		t.Fatalf("volume IDs = %v, want %v", lookedUpVolumeIDs, want)
	}
	if got := len(response.LocationsMap); got != 3 {
		t.Fatalf("location map size = %d, want 3", got)
	}

	for index, result := range response.Results {
		if result.LogTsNs != int64(1000+index) || result.LogSignature != logSignature {
			t.Fatalf("result[%d] fence = (%d,%d), want (%d,%d)", index, result.LogTsNs, result.LogSignature, 1000+index, logSignature)
		}
		switch index {
		case 7:
			if result.Found || result.Entry != nil || result.Error != "lookup failed" {
				t.Fatalf("error result[%d] = %+v", index, result)
			}
		case 11:
			if result.Found || result.Entry != nil || result.Error != "" {
				t.Fatalf("not-found result[%d] = %+v", index, result)
			}
		default:
			if !result.Found || result.Entry == nil || result.Error != "" {
				t.Fatalf("found result[%d] = %+v", index, result)
			}
			if want := fmt.Sprintf("item-%03d", index); result.Entry.Name != want {
				t.Fatalf("result[%d] name = %q, want %q", index, result.Entry.Name, want)
			}
		}
	}
}

func TestLookupDirectoryEntriesMarksEntriesWithUnavailableVolumes(t *testing.T) {
	response, err := lookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests: []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch", Name: "key"}},
	}, 42, func(_ context.Context, path util.FullPath) (*filer.Entry, int64, error) {
		return &filer.Entry{
			FullPath: path,
			Chunks: []*filer_pb.FileChunk{{
				Fid: &filer_pb.FileId{VolumeId: 9, FileKey: 1, Cookie: 2},
			}},
		}, 123, nil
	}, func(_ context.Context, volumeIDs []string) (map[string][]wdclient.Location, error) {
		if !sort.StringsAreSorted(volumeIDs) {
			t.Fatalf("volume IDs are not sorted: %v", volumeIDs)
		}
		return nil, errors.New("master unavailable")
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	result := response.Results[0]
	if result.LogTsNs != 123 || result.LogSignature != 42 {
		t.Fatalf("result fence = (%d,%d), want (123,42)", result.LogTsNs, result.LogSignature)
	}
	if !result.Found || result.Entry == nil {
		t.Fatalf("metadata result lost when volume lookup failed: %+v", result)
	}
	if !strings.Contains(result.Error, "volume 9 has no locations") || !strings.Contains(result.Error, "master unavailable") {
		t.Fatalf("result error = %q", result.Error)
	}
	locations, ok := response.LocationsMap["9"]
	if !ok || locations == nil || len(locations.Locations) != 0 {
		t.Fatalf("missing volume map entry = %#v, present=%v", locations, ok)
	}
}

func cacheVolumeEntry(_ context.Context, path util.FullPath) (*filer.Entry, int64, error) {
	return &filer.Entry{
		FullPath: path,
		Chunks: []*filer_pb.FileChunk{{
			Fid: &filer_pb.FileId{VolumeId: 9, FileKey: 1, Cookie: 2},
		}},
	}, 123, nil
}

func TestLookupDirectoryEntriesCanTreatRetiredCacheVolumeAsMiss(t *testing.T) {
	response, err := lookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests:                []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch", Name: "key"}},
		UnavailableVolumeIsMiss: true,
	}, 42, cacheVolumeEntry, func(_ context.Context, volumeIDs []string) (map[string][]wdclient.Location, error) {
		// The master answered and left volume 9 out.
		return map[string][]wdclient.Location{}, errors.New("volume 9: volume id 9 not found")
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	result := response.Results[0]
	if result.Found || result.Entry != nil || result.Error != "" {
		t.Fatalf("retired cache volume did not become a clean miss: %+v", result)
	}
	if result.LogTsNs != 123 || result.LogSignature != 42 {
		t.Fatalf("miss fence = (%d,%d), want (123,42)", result.LogTsNs, result.LogSignature)
	}
	if _, exists := response.LocationsMap["9"]; exists {
		t.Fatalf("cache miss returned an unusable location: %#v", response.LocationsMap["9"])
	}
}

func TestLookupDirectoryEntriesKeepsCacheEntryWhenMasterIsUnreachable(t *testing.T) {
	response, err := lookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests:                []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch", Name: "key"}},
		UnavailableVolumeIsMiss: true,
	}, 42, cacheVolumeEntry, func(context.Context, []string) (map[string][]wdclient.Location, error) {
		return nil, errors.New("master unavailable")
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	result := response.Results[0]
	if !result.Found || result.Entry == nil {
		t.Fatalf("unanswered lookup was reported as a miss: %+v", result)
	}
	if !strings.Contains(result.Error, "volume 9 has no locations") || !strings.Contains(result.Error, "master unavailable") {
		t.Fatalf("result error = %q", result.Error)
	}
	locations, ok := response.LocationsMap["9"]
	if !ok || locations == nil || len(locations.Locations) != 0 {
		t.Fatalf("unresolved volume map entry = %#v, present=%v", locations, ok)
	}
}

func TestLookupDirectoryEntriesKeepsMalformedEntryErrorInCacheMode(t *testing.T) {
	response, err := lookupDirectoryEntries(context.Background(), &filer_pb.LookupDirectoryEntriesRequest{
		Requests:                []*filer_pb.LookupDirectoryEntryRequest{{Directory: "/batch", Name: "key"}},
		UnavailableVolumeIsMiss: true,
	}, 42, func(_ context.Context, path util.FullPath) (*filer.Entry, int64, error) {
		return &filer.Entry{
			FullPath: path,
			Chunks: []*filer_pb.FileChunk{
				{Fid: &filer_pb.FileId{VolumeId: 9, FileKey: 1, Cookie: 2}},
				{FileId: "not-a-file-id"},
			},
		}, 123, nil
	}, func(context.Context, []string) (map[string][]wdclient.Location, error) {
		return map[string][]wdclient.Location{}, nil
	})
	if err != nil {
		t.Fatalf("batch lookup: %v", err)
	}
	result := response.Results[0]
	if !result.Found || result.Entry == nil || !strings.Contains(result.Error, "invalid file id") {
		t.Fatalf("malformed entry was reported as a clean miss: %+v", result)
	}
}

func TestCacheBatchLookupSelectsAuthoritativeVolumeLocations(t *testing.T) {
	cachedCalls, authoritativeCalls := 0, 0
	cached := func(context.Context, []string) (map[string][]wdclient.Location, error) {
		cachedCalls++
		return map[string][]wdclient.Location{"9": {{Url: "retired-volume:8080"}}}, nil
	}
	authoritative := func(context.Context, []string) (map[string][]wdclient.Location, error) {
		authoritativeCalls++
		return nil, nil
	}

	lookup := selectFilerBatchVolumeLookup(&filer_pb.LookupDirectoryEntriesRequest{
		UnavailableVolumeIsMiss: true,
	}, cached, authoritative)
	locations, err := lookup(context.Background(), []string{"9"})
	if err != nil {
		t.Fatal(err)
	}
	if cachedCalls != 0 || authoritativeCalls != 1 || len(locations) != 0 {
		t.Fatalf("cache lookup used stale path: cached=%d authoritative=%d locations=%v", cachedCalls, authoritativeCalls, locations)
	}

	lookup = selectFilerBatchVolumeLookup(&filer_pb.LookupDirectoryEntriesRequest{}, cached, authoritative)
	if _, err := lookup(context.Background(), []string{"9"}); err != nil {
		t.Fatal(err)
	}
	if cachedCalls != 1 || authoritativeCalls != 1 {
		t.Fatalf("ordinary lookup did not keep cached path: cached=%d authoritative=%d", cachedCalls, authoritativeCalls)
	}
}

func repeatedBatchLookupRequest(count int, request *filer_pb.LookupDirectoryEntryRequest) *filer_pb.LookupDirectoryEntriesRequest {
	requests := make([]*filer_pb.LookupDirectoryEntryRequest, count)
	for index := range requests {
		requests[index] = &filer_pb.LookupDirectoryEntryRequest{
			Directory: request.Directory,
			Name:      request.Name,
		}
	}
	return &filer_pb.LookupDirectoryEntriesRequest{Requests: requests}
}
