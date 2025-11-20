package wdclient

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// UrlPreference controls which URL to use for volume access
type UrlPreference string

const (
	PreferUrl       UrlPreference = "url"       // Use private URL (default)
	PreferPublicUrl UrlPreference = "publicUrl" // Use public URL
)

// FilerClient provides volume location services by querying a filer
// It uses the shared vidMap cache for efficient lookups
// Supports multiple filer addresses with automatic failover for high availability
type FilerClient struct {
	*vidMapClient
	filerAddresses []pb.ServerAddress
	filerIndex     int32 // atomic: current filer index for round-robin
	grpcDialOption grpc.DialOption
	urlPreference  UrlPreference
	grpcTimeout    time.Duration
	cacheSize      int // Number of historical vidMap snapshots to keep
}

// filerVolumeProvider implements VolumeLocationProvider by querying filer
// Supports multiple filer addresses with automatic failover
type filerVolumeProvider struct {
	filerClient *FilerClient
}

// FilerClientOption holds optional configuration for FilerClient
type FilerClientOption struct {
	GrpcTimeout   time.Duration
	UrlPreference UrlPreference
	CacheSize     int // Number of historical vidMap snapshots (0 = use default)
}

// NewFilerClient creates a new client that queries filer(s) for volume locations
// Supports multiple filer addresses for high availability with automatic failover
// Uses sensible defaults: 5-second gRPC timeout, PreferUrl, DefaultVidMapCacheSize
func NewFilerClient(filerAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption, dataCenter string, opts ...*FilerClientOption) *FilerClient {
	if len(filerAddresses) == 0 {
		glog.Fatal("NewFilerClient requires at least one filer address")
	}

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

	fc := &FilerClient{
		filerAddresses: filerAddresses,
		filerIndex:     0,
		grpcDialOption: grpcDialOption,
		urlPreference:  urlPref,
		grpcTimeout:    grpcTimeout,
		cacheSize:      cacheSize,
	}

	// Create provider that references this FilerClient for failover support
	provider := &filerVolumeProvider{
		filerClient: fc,
	}

	fc.vidMapClient = newVidMapClient(provider, dataCenter, cacheSize)

	return fc
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
		
		// Check for partial results first (important for multi-volume batched lookups)
		locations, found := vidLocations[volumeIdStr]
		if !found || len(locations) == 0 {
			// Volume not found - return specific error with context from lookup if available
			if err != nil {
				return nil, fmt.Errorf("volume %s not found for fileId %s: %w", volumeIdStr, fileId, err)
			}
			return nil, fmt.Errorf("volume %s not found for fileId %s", volumeIdStr, fileId)
		}
		
		// Volume found successfully - ignore any errors about other volumes
		// (not relevant for single-volume lookup, but defensive for future batching)

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

// LookupVolumeIds queries the filer for volume locations with automatic failover
// Tries all configured filer addresses until one succeeds (high availability)
// Note: Unlike master's VolumeIdLocation, filer's Locations message doesn't currently have
// an Error field. This implementation handles the current structure while being prepared
// for future error reporting enhancements.
func (p *filerVolumeProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	fc := p.filerClient
	result := make(map[string][]Location)

	// Create a timeout context for the gRPC call
	timeoutCtx, cancel := context.WithTimeout(ctx, fc.grpcTimeout)
	defer cancel()

	// Convert grpcTimeout to milliseconds for the signature parameter
	timeoutMs := int32(fc.grpcTimeout.Milliseconds())

	// Try all filer addresses with round-robin starting from current index
	var lastErr error
	err := util.Retry("filer volume lookup", func() error {
		i := atomic.LoadInt32(&fc.filerIndex)
		n := int32(len(fc.filerAddresses))

		for x := int32(0); x < n; x++ {
			filerAddress := fc.filerAddresses[i]

			err := pb.WithGrpcFilerClient(false, timeoutMs, filerAddress, fc.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
				glog.V(1).Infof("FilerClient: filer %s lookup failed (attempt %d/%d): %v", filerAddress, x+1, n, err)
				lastErr = err
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			// Success - update the preferred filer index for next time
			atomic.StoreInt32(&fc.filerIndex, i)
			glog.V(3).Infof("FilerClient: looked up %d volumes on %s, found %d", len(volumeIds), filerAddress, len(result))
			return nil
		}

		// All filers failed
		return fmt.Errorf("all %d filer(s) failed, last error: %w", n, lastErr)
	})

	if err != nil {
		return nil, fmt.Errorf("filer volume lookup failed for %d volume(s): %w", len(volumeIds), err)
	}

	return result, nil
}
