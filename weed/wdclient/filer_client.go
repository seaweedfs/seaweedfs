package wdclient

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// filerHealth tracks the health status of a filer
type filerHealth struct {
	failureCount      int32 // atomic: consecutive failures
	lastFailureTimeNs int64 // atomic: last failure time in Unix nanoseconds
}

// FilerClient provides volume location services by querying a filer
// It uses the shared vidMap cache for efficient lookups
// Supports multiple filer addresses with automatic failover for high availability
// Tracks filer health to avoid repeatedly trying known-unhealthy filers
type FilerClient struct {
	*vidMapClient
	filerAddresses   []pb.ServerAddress
	filerIndex       int32          // atomic: current filer index for round-robin
	filerHealth      []*filerHealth // health status per filer (same order as filerAddresses)
	grpcDialOption   grpc.DialOption
	urlPreference    UrlPreference
	grpcTimeout      time.Duration
	cacheSize        int           // Number of historical vidMap snapshots to keep
	clientId         int32         // Unique client identifier for gRPC metadata
	failureThreshold int32         // Number of consecutive failures before circuit opens
	resetTimeout     time.Duration // Time to wait before re-checking unhealthy filer
}

// filerVolumeProvider implements VolumeLocationProvider by querying filer
// Supports multiple filer addresses with automatic failover
type filerVolumeProvider struct {
	filerClient *FilerClient
}

// FilerClientOption holds optional configuration for FilerClient
type FilerClientOption struct {
	GrpcTimeout      time.Duration
	UrlPreference    UrlPreference
	CacheSize        int           // Number of historical vidMap snapshots (0 = use default)
	FailureThreshold int32         // Circuit breaker: consecutive failures before skipping filer (0 = use default of 3)
	ResetTimeout     time.Duration // Circuit breaker: time before re-checking unhealthy filer (0 = use default of 30s)
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
	failureThreshold := int32(3)      // Default: 3 consecutive failures before circuit opens
	resetTimeout := 30 * time.Second  // Default: 30 seconds before re-checking unhealthy filer

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
		if opt.FailureThreshold > 0 {
			failureThreshold = opt.FailureThreshold
		}
		if opt.ResetTimeout > 0 {
			resetTimeout = opt.ResetTimeout
		}
	}

	// Initialize health tracking for each filer
	health := make([]*filerHealth, len(filerAddresses))
	for i := range health {
		health[i] = &filerHealth{}
	}

	fc := &FilerClient{
		filerAddresses:   filerAddresses,
		filerIndex:       0,
		filerHealth:      health,
		grpcDialOption:   grpcDialOption,
		urlPreference:    urlPref,
		grpcTimeout:      grpcTimeout,
		cacheSize:        cacheSize,
		clientId:         rand.Int31(), // Random client ID for gRPC metadata tracking
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
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

// isRetryableGrpcError checks if a gRPC error is transient and should be retried
//
// Note on codes.Aborted: While Aborted can indicate application-level conflicts
// (e.g., transaction failures), in the context of volume location lookups (which
// are simple read-only operations with no transactions), Aborted is more likely
// to indicate transient server issues during restart/recovery. We include it here
// for volume lookups but log it for visibility in case misclassification occurs.
func isRetryableGrpcError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable: // Server unavailable (temporary)
			return true
		case codes.DeadlineExceeded: // Request timeout
			return true
		case codes.ResourceExhausted: // Rate limited or overloaded
			return true
		case codes.Aborted:
			// Aborted during read-only volume lookups is likely transient
			// (e.g., filer restarting), but log for visibility
			glog.V(1).Infof("Treating Aborted as retryable for volume lookup: %v", err)
			return true
		}
	}

	// Fallback to string matching for non-gRPC errors (e.g., network errors)
	errStr := err.Error()
	return strings.Contains(errStr, "transport") ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "unavailable")
}

// shouldSkipUnhealthyFiler checks if we should skip a filer based on recent failures
// Circuit breaker pattern: skip filers with multiple recent consecutive failures
func (fc *FilerClient) shouldSkipUnhealthyFiler(index int32) bool {
	health := fc.filerHealth[index]
	failureCount := atomic.LoadInt32(&health.failureCount)

	// Check if failure count exceeds threshold
	if failureCount < fc.failureThreshold {
		return false
	}

	// Re-check unhealthy filers after reset timeout
	lastFailureNs := atomic.LoadInt64(&health.lastFailureTimeNs)
	if lastFailureNs == 0 {
		return false // Never failed, shouldn't skip
	}
	lastFailureTime := time.Unix(0, lastFailureNs)
	if time.Since(lastFailureTime) > fc.resetTimeout {
		return false // Time to re-check
	}

	return true // Skip this unhealthy filer
}

// recordFilerSuccess resets failure tracking for a successful filer
func (fc *FilerClient) recordFilerSuccess(index int32) {
	health := fc.filerHealth[index]
	atomic.StoreInt32(&health.failureCount, 0)
}

// recordFilerFailure increments failure count for an unhealthy filer
func (fc *FilerClient) recordFilerFailure(index int32) {
	health := fc.filerHealth[index]
	atomic.AddInt32(&health.failureCount, 1)
	atomic.StoreInt64(&health.lastFailureTimeNs, time.Now().UnixNano())
}

// LookupVolumeIds queries the filer for volume locations with automatic failover
// Tries all configured filer addresses until one succeeds (high availability)
// Retries transient gRPC errors (Unavailable, DeadlineExceeded, etc.) with exponential backoff
// Note: Unlike master's VolumeIdLocation, filer's Locations message doesn't currently have
// an Error field. This implementation handles the current structure while being prepared
// for future error reporting enhancements.
func (p *filerVolumeProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	fc := p.filerClient
	result := make(map[string][]Location)

	// Retry transient failures with exponential backoff
	var lastErr error
	waitTime := time.Second
	maxRetries := 3

	for retry := 0; retry < maxRetries; retry++ {
		// Try all filer addresses with round-robin starting from current index
		// Skip known-unhealthy filers (circuit breaker pattern)
		i := atomic.LoadInt32(&fc.filerIndex)
		n := int32(len(fc.filerAddresses))

		for x := int32(0); x < n; x++ {
			// Circuit breaker: skip unhealthy filers
			if fc.shouldSkipUnhealthyFiler(i) {
				glog.V(2).Infof("FilerClient: skipping unhealthy filer %s (consecutive failures: %d)",
					fc.filerAddresses[i], atomic.LoadInt32(&fc.filerHealth[i].failureCount))
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			filerAddress := fc.filerAddresses[i]

			// Create a fresh timeout context for each filer attempt
			// This ensures each retry gets the full grpcTimeout, not a diminishing deadline
			timeoutCtx, cancel := context.WithTimeout(ctx, fc.grpcTimeout)
			err := pb.WithGrpcFilerClient(false, fc.clientId, filerAddress, fc.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
			cancel() // Clean up timeout context immediately after call returns

			if err != nil {
				glog.V(1).Infof("FilerClient: filer %s lookup failed (attempt %d/%d, retry %d/%d): %v", filerAddress, x+1, n, retry+1, maxRetries, err)
				fc.recordFilerFailure(i)
				lastErr = err
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			// Success - update the preferred filer index and reset health tracking
			atomic.StoreInt32(&fc.filerIndex, i)
			fc.recordFilerSuccess(i)
			glog.V(3).Infof("FilerClient: looked up %d volumes on %s, found %d", len(volumeIds), filerAddress, len(result))
			return result, nil
		}

		// All filers failed on this attempt
		// Check if the error is retryable (transient gRPC error)
		if !isRetryableGrpcError(lastErr) {
			// Non-retryable error (e.g., NotFound, PermissionDenied) - fail immediately
			return nil, fmt.Errorf("all %d filer(s) failed with non-retryable error: %w", n, lastErr)
		}

		// Transient error - retry if we have attempts left
		if retry < maxRetries-1 {
			glog.V(1).Infof("FilerClient: all %d filer(s) failed with retryable error (attempt %d/%d), retrying in %v: %v",
				n, retry+1, maxRetries, waitTime, lastErr)
			time.Sleep(waitTime)
			waitTime = waitTime * 3 / 2 // Multiplicative backoff with 1.5x factor: 1s, 1.5s, 2.25s
		}
	}

	// All retries exhausted
	return nil, fmt.Errorf("all %d filer(s) failed after %d attempts, last error: %w", len(fc.filerAddresses), maxRetries, lastErr)
}
