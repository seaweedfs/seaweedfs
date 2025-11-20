package wdclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// UrlPreference controls which URL to use for volume access
type UrlPreference string

const (
	PreferUrl       UrlPreference = "url"       // Use private URL (default)
	PreferPublicUrl UrlPreference = "publicUrl" // Use public URL
)

// FilerClient provides volume location services by querying a filer
// It uses the shared vidMap cache for efficient lookups
type FilerClient struct {
	*vidMapClient
	filerAddress   pb.ServerAddress
	grpcDialOption grpc.DialOption
	urlPreference  UrlPreference
	grpcTimeout    time.Duration
	cacheSize      int // Number of historical vidMap snapshots to keep
}

// filerVolumeProvider implements VolumeLocationProvider by querying filer
type filerVolumeProvider struct {
	filerAddress   pb.ServerAddress
	grpcDialOption grpc.DialOption
	grpcTimeout    time.Duration
}

// FilerClientOption holds optional configuration for FilerClient
type FilerClientOption struct {
	GrpcTimeout   time.Duration
	UrlPreference UrlPreference
	CacheSize     int // Number of historical vidMap snapshots (0 = use default)
}

// NewFilerClient creates a new client that queries filer for volume locations
// Uses sensible defaults: 5-second gRPC timeout, PreferUrl, DefaultVidMapCacheSize
func NewFilerClient(filerAddress pb.ServerAddress, grpcDialOption grpc.DialOption, dataCenter string, opts ...*FilerClientOption) *FilerClient {
	// Apply defaults
	grpcTimeout := 5 * time.Second
	urlPref := PreferUrl
	cacheSize := DefaultVidMapCacheSize

	// Override with provided options
	if len(opts) > 0 && opts[0] != nil {
		opt := opts[0]
		if opt.GrpcTimeout > 0 {
			grpcTimeout = opt.GrpcTimeout
		}
		if opt.UrlPreference != "" {
			urlPref = opt.UrlPreference
		}
		if opt.CacheSize > 0 {
			cacheSize = opt.CacheSize
		}
	}

	provider := &filerVolumeProvider{
		filerAddress:   filerAddress,
		grpcDialOption: grpcDialOption,
		grpcTimeout:    grpcTimeout,
	}

	return &FilerClient{
		vidMapClient:   newVidMapClient(provider, dataCenter, cacheSize),
		filerAddress:   filerAddress,
		grpcDialOption: grpcDialOption,
		urlPreference:  urlPref,
		grpcTimeout:    grpcTimeout,
		cacheSize:      cacheSize,
	}
}

// GetLookupFileIdFunction returns a lookup function with URL preference handling
func (fc *FilerClient) GetLookupFileIdFunction() LookupFileIdFunctionType {
	if fc.urlPreference == PreferUrl {
		// Use the default implementation from vidMapClient
		return fc.vidMapClient.GetLookupFileIdFunction()
	}

	// Custom implementation that prefers PublicUrl
	return func(ctx context.Context, fileId string) (fullUrls []string, err error) {
		// Parse file ID to extract volume ID
		parts := strings.Split(fileId, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid fileId format: %s", fileId)
		}
		volumeIdStr := parts[0]

		// First try the cache using LookupVolumeIdsWithFallback
		vidLocations, err := fc.LookupVolumeIdsWithFallback(ctx, []string{volumeIdStr})
		if err != nil {
			return nil, fmt.Errorf("LookupVolume %s failed: %v", fileId, err)
		}

		locations, found := vidLocations[volumeIdStr]
		if !found || len(locations) == 0 {
			return nil, fmt.Errorf("volume %s not found for fileId %s", volumeIdStr, fileId)
		}

		// Build URLs with publicUrl preference
		for _, loc := range locations {
			url := loc.PublicUrl
			if url == "" {
				url = loc.Url
			}
			fullUrls = append(fullUrls, "http://"+url+"/"+fileId)
		}
		return fullUrls, nil
	}
}

// LookupVolumeIds queries the filer for volume locations
// Note: Unlike master's VolumeIdLocation, filer's Locations message doesn't currently have
// an Error field. This implementation handles the current structure while being prepared
// for future error reporting enhancements.
func (p *filerVolumeProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)

	// Create a timeout context for the gRPC call
	timeoutCtx, cancel := context.WithTimeout(ctx, p.grpcTimeout)
	defer cancel()

	// Convert grpcTimeout to milliseconds for the signature parameter
	timeoutMs := int32(p.grpcTimeout.Milliseconds())

	err := pb.WithGrpcFilerClient(false, timeoutMs, p.filerAddress, p.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupVolume(timeoutCtx, &filer_pb.LookupVolumeRequest{
			VolumeIds: volumeIds,
		})
		if err != nil {
			return fmt.Errorf("filer.LookupVolume failed: %w", err)
		}

		// Process each volume in the response
		for vid, locs := range resp.LocationsMap {
			// Convert locations from protobuf to internal format
			var locations []Location
			for _, loc := range locs.Locations {
				locations = append(locations, Location{
					Url:        loc.Url,
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.DataCenter,
					GrpcPort:   int(loc.GrpcPort),
				})
			}

			// Only add to result if we have locations
			// Empty locations with no gRPC error means "not found" (volume doesn't exist)
			if len(locations) > 0 {
				result[vid] = locations
				glog.V(4).Infof("FilerClient: volume %s found with %d location(s)", vid, len(locations))
			} else {
				glog.V(2).Infof("FilerClient: volume %s not found (no locations in response)", vid)
			}
		}

		// Check for volumes that weren't in the response at all
		// This could indicate a problem with the filer
		for _, vid := range volumeIds {
			if _, found := resp.LocationsMap[vid]; !found {
				glog.V(1).Infof("FilerClient: volume %s missing from filer response", vid)
			}
		}

		return nil
	})

	if err != nil {
		// gRPC error - this is a communication or server failure
		return nil, fmt.Errorf("filer volume lookup failed for %d volume(s): %w", len(volumeIds), err)
	}

	glog.V(3).Infof("FilerClient: looked up %d volumes, found %d", len(volumeIds), len(result))
	return result, nil
}
