package weed_server

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxFilerBatchLookupRequests = 4096
	maxFilerBatchLookupWorkers  = 32
)

type filerBatchEntryFinder func(context.Context, util.FullPath) (*filer.Entry, int64, error)
type filerBatchVolumeLookup func(context.Context, []string) (map[string][]wdclient.Location, error)

// LookupDirectoryEntries performs exact entry lookups concurrently while
// preserving request order, then resolves every referenced volume in one
// deduplicated master lookup.
func (fs *FilerServer) LookupDirectoryEntries(ctx context.Context, req *filer_pb.LookupDirectoryEntriesRequest) (*filer_pb.LookupDirectoryEntriesResponse, error) {
	lookupVolumes := selectFilerBatchVolumeLookup(req,
		fs.filer.MasterClient.LookupVolumeIdsWithFallback,
		fs.filer.MasterClient.LookupVolumeIdsAuthoritative)
	response, err := lookupDirectoryEntries(ctx, req, fs.filer.Signature, fs.fencedFindEntry, lookupVolumes)
	if err != nil {
		return nil, err
	}
	if fs.volumeGuard != nil {
		populateBatchReadAuth(response, fs.maybeGetVolumeReadJwtAuthorizationToken)
	}
	return response, nil
}

func selectFilerBatchVolumeLookup(req *filer_pb.LookupDirectoryEntriesRequest, cached, authoritative filerBatchVolumeLookup) filerBatchVolumeLookup {
	if req != nil && req.GetUnavailableVolumeIsMiss() {
		// DeletedVids is an asynchronous cache invalidation hint and its delivery
		// is deliberately best-effort. Cache reads must therefore confirm the
		// unique Volume IDs with Master before treating a location as live.
		return authoritative
	}
	return cached
}

func populateBatchReadAuth(response *filer_pb.LookupDirectoryEntriesResponse, mint func(string) string) {
	response.ReadAuth = make(map[string]string)
	for _, result := range response.Results {
		if result == nil || result.Entry == nil {
			continue
		}
		for _, chunk := range result.Entry.Chunks {
			fid := chunk.GetFileIdString()
			if fid == "" {
				continue
			}
			if token := mint(fid); token != "" {
				response.ReadAuth[fid] = token
			}
		}
	}
}

func lookupDirectoryEntries(
	ctx context.Context,
	req *filer_pb.LookupDirectoryEntriesRequest,
	logSignature int32,
	findEntry filerBatchEntryFinder,
	lookupVolumes filerBatchVolumeLookup,
) (*filer_pb.LookupDirectoryEntriesResponse, error) {
	if err := validateFilerBatchLookupRequest(req); err != nil {
		return nil, err
	}

	results := make([]*filer_pb.LookupDirectoryEntryResult, len(req.Requests))
	resultVolumeIDs := make([]map[string]struct{}, len(req.Requests))
	jobs := make(chan int)
	workerCount := min(len(req.Requests), maxFilerBatchLookupWorkers)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				request := req.Requests[index]
				entry, logTsNs, err := findEntry(ctx, util.JoinPath(request.Directory, request.Name))
				result := &filer_pb.LookupDirectoryEntryResult{
					LogTsNs:      logTsNs,
					LogSignature: logSignature,
				}
				switch {
				case err == filer_pb.ErrNotFound:
					results[index] = result
				case err != nil:
					result.Error = err.Error()
					results[index] = result
				case entry == nil:
					result.Error = "entry lookup returned no entry"
					results[index] = result
				default:
					pbEntry := entry.ToProtoEntry()
					volumeIDs, volumeErr := filerBatchEntryVolumeIDs(pbEntry)
					result.Found = true
					result.Entry = pbEntry
					if volumeErr != nil {
						result.Error = volumeErr.Error()
					}
					results[index] = result
					resultVolumeIDs[index] = volumeIDs
				}
			}
		}()
	}

	for index := range req.Requests {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	allVolumeIDs := make(map[string]struct{})
	for _, volumeIDs := range resultVolumeIDs {
		for volumeID := range volumeIDs {
			allVolumeIDs[volumeID] = struct{}{}
		}
	}
	volumeIDs := make([]string, 0, len(allVolumeIDs))
	for volumeID := range allVolumeIDs {
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Strings(volumeIDs)

	response := &filer_pb.LookupDirectoryEntriesResponse{
		Results:      results,
		LocationsMap: make(map[string]*filer_pb.Locations, len(volumeIDs)),
	}
	if len(volumeIDs) == 0 {
		return response, nil
	}

	locationsByVolume, lookupErr := lookupVolumes(ctx, volumeIDs)
	// A nil map means the provider never got an answer. Only a volume the
	// master itself left out may become a miss; anything else stays an error.
	missIsAuthoritative := req.UnavailableVolumeIsMiss && locationsByVolume != nil
	for _, volumeID := range volumeIDs {
		locations := locationsByVolume[volumeID]
		if len(locations) != 0 {
			response.LocationsMap[volumeID] = &filer_pb.Locations{
				Locations: wdclientLocationsToPb(locations),
			}
			continue
		}
		if !missIsAuthoritative {
			response.LocationsMap[volumeID] = &filer_pb.Locations{}
		}
		for index, entryVolumeIDs := range resultVolumeIDs {
			if _, affected := entryVolumeIDs[volumeID]; !affected {
				continue
			}
			if missIsAuthoritative && results[index].Error == "" {
				results[index].Found = false
				results[index].Entry = nil
				resultVolumeIDs[index] = nil
				continue
			}
			message := fmt.Sprintf("volume %s has no locations", volumeID)
			if lookupErr != nil {
				message += ": " + lookupErr.Error()
			}
			results[index].Error = appendFilerBatchLookupError(results[index].Error, message)
		}
	}

	return response, nil
}

func validateFilerBatchLookupRequest(req *filer_pb.LookupDirectoryEntriesRequest) error {
	if req == nil || len(req.Requests) == 0 {
		return status.Error(codes.InvalidArgument, "batch lookup requires at least one request")
	}
	if len(req.Requests) > maxFilerBatchLookupRequests {
		return status.Errorf(codes.ResourceExhausted, "batch lookup has %d requests; maximum is %d", len(req.Requests), maxFilerBatchLookupRequests)
	}
	for index, request := range req.Requests {
		if request == nil {
			return status.Errorf(codes.InvalidArgument, "batch lookup request %d is missing", index)
		}
		if request.Directory == "" || !strings.HasPrefix(request.Directory, "/") || strings.ContainsRune(request.Directory, '\x00') {
			return status.Errorf(codes.InvalidArgument, "batch lookup request %d has invalid directory", index)
		}
		if request.Name == "" || request.Name == "." || request.Name == ".." || strings.ContainsAny(request.Name, "/\x00") {
			return status.Errorf(codes.InvalidArgument, "batch lookup request %d has invalid name", index)
		}
	}
	return nil
}

func filerBatchEntryVolumeIDs(entry *filer_pb.Entry) (map[string]struct{}, error) {
	volumeIDs := make(map[string]struct{})
	for index, chunk := range entry.GetChunks() {
		if chunk == nil {
			return volumeIDs, fmt.Errorf("entry chunk %d is missing", index)
		}
		if chunk.Fid != nil {
			volumeIDs[strconv.FormatUint(uint64(chunk.Fid.VolumeId), 10)] = struct{}{}
			continue
		}
		if chunk.FileId == "" {
			return volumeIDs, fmt.Errorf("entry chunk %d has no file id", index)
		}
		fid, err := needle.ParseFileIdFromString(chunk.FileId)
		if err != nil {
			return volumeIDs, fmt.Errorf("entry chunk %d has invalid file id: %w", index, err)
		}
		volumeIDs[strconv.FormatUint(uint64(fid.VolumeId), 10)] = struct{}{}
	}
	return volumeIDs, nil
}

func appendFilerBatchLookupError(existing, next string) string {
	if existing == "" {
		return next
	}
	return existing + "; " + next
}
