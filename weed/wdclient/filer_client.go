package wdclient

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
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
// Can discover additional filers from master server when configured with filer group
type FilerClient struct {
	*vidMapClient
	filerAddresses     []pb.ServerAddress
	filerAddressesMu   sync.RWMutex   // Protects filerAddresses and filerHealth
	filerIndex         int32          // atomic: current filer index for round-robin
	filerHealth        []*filerHealth // health status per filer (same order as filerAddresses)
	grpcDialOption     grpc.DialOption
	urlPreference      UrlPreference
	grpcTimeout        time.Duration
	cacheSize          int           // Number of historical vidMap snapshots to keep
	clientId           int32         // Unique client identifier for gRPC metadata
	failureThreshold   int32         // Circuit breaker: consecutive failures before circuit opens
	resetTimeout       time.Duration // Circuit breaker: time before re-checking unhealthy filer
	maxRetries         int           // Retry: maximum retry attempts for transient failures
	initialRetryWait   time.Duration // Retry: initial wait time before first retry
	retryBackoffFactor float64       // Retry: backoff multiplier for wait time
	
	// Filer discovery fields
	masterClient       *MasterClient // Optional: for discovering filers in the same group
	filerGroup         string        // Optional: filer group for discovery
	discoveryInterval  time.Duration // How often to refresh filer list from master
	stopDiscovery      chan struct{} // Signal to stop discovery goroutine
	closeDiscoveryOnce sync.Once     // Ensures discovery channel is closed at most once
}

// filerVolumeProvider implements VolumeLocationProvider by querying filer
// Supports multiple filer addresses with automatic failover
type filerVolumeProvider struct {
	filerClient *FilerClient
}

// FilerClientOption holds optional configuration for FilerClient
type FilerClientOption struct {
	GrpcTimeout        time.Duration
	UrlPreference      UrlPreference
	CacheSize          int           // Number of historical vidMap snapshots (0 = use default)
	FailureThreshold   int32         // Circuit breaker: consecutive failures before skipping filer (0 = use default of 3)
	ResetTimeout       time.Duration // Circuit breaker: time before re-checking unhealthy filer (0 = use default of 30s)
	MaxRetries         int           // Retry: maximum retry attempts for transient failures (0 = use default of 3)
	InitialRetryWait   time.Duration // Retry: initial wait time before first retry (0 = use default of 1s)
	RetryBackoffFactor float64       // Retry: backoff multiplier for wait time (0 = use default of 1.5)
	
	// Filer discovery options
	MasterClient      *MasterClient // Optional: enables filer discovery from master
	FilerGroup        string        // Optional: filer group name for discovery (required if MasterClient is set)
	DiscoveryInterval time.Duration // Optional: how often to refresh filer list (0 = use default of 5 minutes)
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
	failureThreshold := int32(3)     // Default: 3 consecutive failures before circuit opens
	resetTimeout := 30 * time.Second // Default: 30 seconds before re-checking unhealthy filer
	maxRetries := 3                  // Default: 3 retry attempts for transient failures
	initialRetryWait := time.Second  // Default: 1 second initial retry wait
	retryBackoffFactor := 1.5        // Default: 1.5x backoff multiplier
	var masterClient *MasterClient
	var filerGroup string
	discoveryInterval := 5 * time.Minute // Default: refresh every 5 minutes

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
		if opt.MaxRetries > 0 {
			maxRetries = opt.MaxRetries
		}
		if opt.InitialRetryWait > 0 {
			initialRetryWait = opt.InitialRetryWait
		}
		if opt.RetryBackoffFactor > 0 {
			retryBackoffFactor = opt.RetryBackoffFactor
		}
		if opt.MasterClient != nil {
			masterClient = opt.MasterClient
			filerGroup = opt.FilerGroup
			if opt.DiscoveryInterval > 0 {
				discoveryInterval = opt.DiscoveryInterval
			}
		}
	}

	// Initialize health tracking for each filer
	health := make([]*filerHealth, len(filerAddresses))
	for i := range health {
		health[i] = &filerHealth{}
	}

	fc := &FilerClient{
		filerAddresses:     filerAddresses,
		filerIndex:         0,
		filerHealth:        health,
		grpcDialOption:     grpcDialOption,
		urlPreference:      urlPref,
		grpcTimeout:        grpcTimeout,
		cacheSize:          cacheSize,
		clientId:           rand.Int31(), // Random client ID for gRPC metadata tracking
		failureThreshold:   failureThreshold,
		resetTimeout:       resetTimeout,
		maxRetries:         maxRetries,
		initialRetryWait:   initialRetryWait,
		retryBackoffFactor: retryBackoffFactor,
		masterClient:       masterClient,
		filerGroup:         filerGroup,
		discoveryInterval:  discoveryInterval,
	}

	// Start filer discovery if master client is configured
	if masterClient != nil && filerGroup != "" {
		fc.stopDiscovery = make(chan struct{})
		go fc.discoverFilers()
		glog.V(0).Infof("FilerClient: started filer discovery for group '%s' (refresh interval: %v)", filerGroup, discoveryInterval)
	}

	// Create provider that references this FilerClient for failover support
	provider := &filerVolumeProvider{
		filerClient: fc,
	}

	fc.vidMapClient = newVidMapClient(provider, dataCenter, cacheSize)

	return fc
}

// GetCurrentFiler returns the currently active filer address
// This is the filer that was last successfully used or the one indicated by round-robin
// Returns empty string if no filers are configured
func (fc *FilerClient) GetCurrentFiler() pb.ServerAddress {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	if len(fc.filerAddresses) == 0 {
		return ""
	}
	
	// Get current index (atomically updated on successful operations)
	index := atomic.LoadInt32(&fc.filerIndex)
	if index >= int32(len(fc.filerAddresses)) {
		index = 0
	}
	
	return fc.filerAddresses[index]
}

// GetAllFilers returns a snapshot of all filer addresses
// Returns a copy to avoid concurrent modification issues
func (fc *FilerClient) GetAllFilers() []pb.ServerAddress {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	// Return a copy to avoid concurrent modification
	filers := make([]pb.ServerAddress, len(fc.filerAddresses))
	copy(filers, fc.filerAddresses)
	return filers
}

// SetCurrentFiler updates the current filer index to the specified address
// This is useful after successful failover to prefer the healthy filer for future requests
func (fc *FilerClient) SetCurrentFiler(addr pb.ServerAddress) {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	// Find the index of the specified filer address
	for i, filer := range fc.filerAddresses {
		if filer == addr {
			atomic.StoreInt32(&fc.filerIndex, int32(i))
			return
		}
	}
	// If address not found, leave index unchanged
}

// ShouldSkipUnhealthyFiler checks if a filer address should be skipped based on health tracking
// Returns true if the filer has exceeded failure threshold and reset timeout hasn't elapsed
func (fc *FilerClient) ShouldSkipUnhealthyFiler(addr pb.ServerAddress) bool {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	// Find the health for this filer address
	for i, filer := range fc.filerAddresses {
		if filer == addr {
			if i < len(fc.filerHealth) {
				return fc.shouldSkipUnhealthyFilerWithHealth(fc.filerHealth[i])
			}
			return false
		}
	}
	// If address not found, don't skip it
	return false
}

// RecordFilerSuccess resets failure tracking for a successful filer
func (fc *FilerClient) RecordFilerSuccess(addr pb.ServerAddress) {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	// Find the health for this filer address
	for i, filer := range fc.filerAddresses {
		if filer == addr {
			if i < len(fc.filerHealth) {
				fc.recordFilerSuccessWithHealth(fc.filerHealth[i])
			}
			return
		}
	}
}

// RecordFilerFailure increments failure count for an unhealthy filer
func (fc *FilerClient) RecordFilerFailure(addr pb.ServerAddress) {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	
	// Find the health for this filer address
	for i, filer := range fc.filerAddresses {
		if filer == addr {
			if i < len(fc.filerHealth) {
				fc.recordFilerFailureWithHealth(fc.filerHealth[i])
			}
			return
		}
	}
}

// Close stops the filer discovery goroutine if running
// Safe to call multiple times (idempotent)
func (fc *FilerClient) Close() {
	if fc.stopDiscovery != nil {
		fc.closeDiscoveryOnce.Do(func() {
			close(fc.stopDiscovery)
		})
	}
}

// discoverFilers periodically queries the master to discover filers in the same group
// and updates the filer list. This runs in a background goroutine.
func (fc *FilerClient) discoverFilers() {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("FilerClient: panic in filer discovery goroutine for group '%s': %v", fc.filerGroup, r)
		}
	}()
	
	// Do an initial discovery
	fc.refreshFilerList()
	
	ticker := time.NewTicker(fc.discoveryInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			fc.refreshFilerList()
		case <-fc.stopDiscovery:
			glog.V(0).Infof("FilerClient: stopping filer discovery for group '%s'", fc.filerGroup)
			return
		}
	}
}

// refreshFilerList queries the master for the current list of filers and updates the local list
func (fc *FilerClient) refreshFilerList() {
	if fc.masterClient == nil {
		return
	}
	
	// Get current master address
	currentMaster := fc.masterClient.GetMaster(context.Background())
	if currentMaster == "" {
		glog.V(1).Infof("FilerClient: no master available for filer discovery")
		return
	}
	
	// Query master for filers in our group
	updates := cluster.ListExistingPeerUpdates(currentMaster, fc.grpcDialOption, fc.filerGroup, cluster.FilerType)
	
	if len(updates) == 0 {
		glog.V(2).Infof("FilerClient: no filers found in group '%s'", fc.filerGroup)
		return
	}
	
	// Build new filer address list
	discoveredFilers := make(map[pb.ServerAddress]bool)
	for _, update := range updates {
		if update.Address != "" {
			discoveredFilers[pb.ServerAddress(update.Address)] = true
		}
	}
	
	// Thread-safe update of filer list
	fc.filerAddressesMu.Lock()
	defer fc.filerAddressesMu.Unlock()
	
	// Build a map of existing filers for efficient O(1) lookup
	existingFilers := make(map[pb.ServerAddress]struct{}, len(fc.filerAddresses))
	for _, f := range fc.filerAddresses {
		existingFilers[f] = struct{}{}
	}
	
	// Find new filers - O(N+M) instead of O(N*M)
	var newFilers []pb.ServerAddress
	for addr := range discoveredFilers {
		if _, found := existingFilers[addr]; !found {
			newFilers = append(newFilers, addr)
		}
	}
	
	// Add new filers
	if len(newFilers) > 0 {
		glog.V(0).Infof("FilerClient: discovered %d new filer(s) in group '%s': %v", len(newFilers), fc.filerGroup, newFilers)
		fc.filerAddresses = append(fc.filerAddresses, newFilers...)
		
		// Initialize health tracking for new filers
		for range newFilers {
			fc.filerHealth = append(fc.filerHealth, &filerHealth{})
		}
	}
	
	// Optionally, remove filers that are no longer in the cluster
	// For now, we keep all filers and rely on health checks to avoid dead ones
	// This prevents removing filers that might be temporarily unavailable
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

		// Build URLs with publicUrl preference, and also prefer same DC
		var sameDcUrls, otherDcUrls []string
		dataCenter := fc.GetDataCenter()
		for _, loc := range locations {
			url := loc.PublicUrl
			if url == "" {
				url = loc.Url
			}
			httpUrl := "http://" + url + "/" + fileId
			if dataCenter != "" && dataCenter == loc.DataCenter {
				sameDcUrls = append(sameDcUrls, httpUrl)
			} else {
				otherDcUrls = append(otherDcUrls, httpUrl)
			}
		}
		// Shuffle to distribute load across volume servers
		rand.Shuffle(len(sameDcUrls), func(i, j int) { sameDcUrls[i], sameDcUrls[j] = sameDcUrls[j], sameDcUrls[i] })
		rand.Shuffle(len(otherDcUrls), func(i, j int) { otherDcUrls[i], otherDcUrls[j] = otherDcUrls[j], otherDcUrls[i] })
		// Prefer same data center
		fullUrls = append(sameDcUrls, otherDcUrls...)
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
// shouldSkipUnhealthyFilerWithHealth checks if a filer should be skipped based on health
// Uses atomic operations only - safe to call without locks
func (fc *FilerClient) shouldSkipUnhealthyFilerWithHealth(health *filerHealth) bool {
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

// Deprecated: Use shouldSkipUnhealthyFilerWithHealth instead
// This function is kept for backward compatibility but requires array access
// Note: Accesses filerHealth without lock; safe only when discovery is disabled
func (fc *FilerClient) shouldSkipUnhealthyFiler(index int32) bool {
	fc.filerAddressesMu.RLock()
	if index >= int32(len(fc.filerHealth)) {
		fc.filerAddressesMu.RUnlock()
		return true // Invalid index - skip
	}
	health := fc.filerHealth[index]
	fc.filerAddressesMu.RUnlock()
	return fc.shouldSkipUnhealthyFilerWithHealth(health)
}

// recordFilerSuccessWithHealth resets failure tracking for a successful filer
func (fc *FilerClient) recordFilerSuccessWithHealth(health *filerHealth) {
	atomic.StoreInt32(&health.failureCount, 0)
}

// recordFilerSuccess resets failure tracking for a successful filer
func (fc *FilerClient) recordFilerSuccess(index int32) {
	fc.filerAddressesMu.RLock()
	if index >= int32(len(fc.filerHealth)) {
		fc.filerAddressesMu.RUnlock()
		return // Invalid index
	}
	health := fc.filerHealth[index]
	fc.filerAddressesMu.RUnlock()
	fc.recordFilerSuccessWithHealth(health)
}

// recordFilerFailureWithHealth increments failure count for an unhealthy filer
func (fc *FilerClient) recordFilerFailureWithHealth(health *filerHealth) {
	atomic.AddInt32(&health.failureCount, 1)
	atomic.StoreInt64(&health.lastFailureTimeNs, time.Now().UnixNano())
}

// recordFilerFailure increments failure count for an unhealthy filer
func (fc *FilerClient) recordFilerFailure(index int32) {
	fc.filerAddressesMu.RLock()
	if index >= int32(len(fc.filerHealth)) {
		fc.filerAddressesMu.RUnlock()
		return // Invalid index
	}
	health := fc.filerHealth[index]
	fc.filerAddressesMu.RUnlock()
	fc.recordFilerFailureWithHealth(health)
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

	// Retry transient failures with configurable backoff
	var lastErr error
	waitTime := fc.initialRetryWait
	maxRetries := fc.maxRetries

	for retry := 0; retry < maxRetries; retry++ {
		// Try all filer addresses with round-robin starting from current index
		// Skip known-unhealthy filers (circuit breaker pattern)
		i := atomic.LoadInt32(&fc.filerIndex)
		
		// Get filer count with read lock
		fc.filerAddressesMu.RLock()
		n := int32(len(fc.filerAddresses))
		fc.filerAddressesMu.RUnlock()

		for x := int32(0); x < n; x++ {
			// Get current filer address and health with read lock
			fc.filerAddressesMu.RLock()
			if len(fc.filerAddresses) == 0 {
				fc.filerAddressesMu.RUnlock()
				lastErr = fmt.Errorf("no filers available")
				break
			}
			if i >= int32(len(fc.filerAddresses)) {
				// Filer list changed, reset index
				i = 0
			}
			
			// Get health pointer while holding lock
			health := fc.filerHealth[i]
			filerAddress := fc.filerAddresses[i]
			fc.filerAddressesMu.RUnlock()
			
			// Circuit breaker: skip unhealthy filers (no lock needed - uses atomics)
			if fc.shouldSkipUnhealthyFilerWithHealth(health) {
				glog.V(2).Infof("FilerClient: skipping unhealthy filer %s (consecutive failures: %d)",
					filerAddress, atomic.LoadInt32(&health.failureCount))
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			// Use anonymous function to ensure defer cancel() is called per iteration, not accumulated
			err := func() error {
				// Create a fresh timeout context for each filer attempt
				// This ensures each retry gets the full grpcTimeout, not a diminishing deadline
				timeoutCtx, cancel := context.WithTimeout(ctx, fc.grpcTimeout)
				defer cancel() // Always clean up context, even on panic or early return

				return pb.WithGrpcFilerClient(false, fc.clientId, filerAddress, fc.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
			}()

			if err != nil {
				glog.V(1).Infof("FilerClient: filer %s lookup failed (attempt %d/%d, retry %d/%d): %v", filerAddress, x+1, n, retry+1, maxRetries, err)
				fc.recordFilerFailureWithHealth(health)
				lastErr = err
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			// Success - update the preferred filer index and reset health tracking
			atomic.StoreInt32(&fc.filerIndex, i)
			fc.recordFilerSuccessWithHealth(health)
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
			waitTime = time.Duration(float64(waitTime) * fc.retryBackoffFactor)
		}
	}

	// All retries exhausted
	fc.filerAddressesMu.RLock()
	totalFilers := len(fc.filerAddresses)
	fc.filerAddressesMu.RUnlock()
	return nil, fmt.Errorf("all %d filer(s) failed after %d attempts, last error: %w", totalFilers, maxRetries, lastErr)
}
