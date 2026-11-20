package wdclient

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
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
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// UrlPreference controls which URL to use for volume access
type UrlPreference string

const (
	PreferUrl       UrlPreference = "url"       // Use private URL (default)
	PreferPublicUrl UrlPreference = "publicUrl" // Use public URL
)

// filerHealth tracks the health status of a filer
type filerHealth struct {
	lastFailureTimeNs int64 // atomic: last failure time in Unix nanoseconds
	failureCount      int32 // atomic: consecutive failures
}

// FilerClient provides volume location services by querying a filer
// It uses the shared vidMap cache for efficient lookups
// Supports multiple filer addresses with automatic failover for high availability
// Tracks filer health to avoid repeatedly trying known-unhealthy filers
// Can discover additional filers from master server when configured with filer group
type FilerClient struct {
	*vidMapClient
	filerAddresses     []pb.ServerAddress
	filerAddressesMu   sync.RWMutex                  // Protects filerAddresses and filerHealth
	filerIndex         int32                         // atomic: current filer index for round-robin
	filerHealth        []*filerHealth                // health status per filer (same order as filerAddresses)
	peerUpdates        uint64                        // pushed filer updates applied; protected by filerAddressesMu
	deferredLeaves     map[pb.ServerAddress]struct{} // leaves suppressed while the filer was the last known; protected by filerAddressesMu
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

	// Home filer: the filer this client returns to whenever it is healthy,
	// e.g. the local filer of an embedded S3 gateway. Empty means no home.
	homeFiler pb.ServerAddress
	now       func() time.Time // clock; nil means time.Now

	homeMu         sync.Mutex    // serializes current-filer switches with the fields below
	homeBackoff    time.Duration // wait after leaving home before returning to it
	homeEligibleAt time.Time     // home is not returned to before this time
	lastReturnHome time.Time     // when the client last returned home
}

const (
	homeBackoffCap = 5 * time.Minute  // ceiling of the return-to-home backoff
	homeHoldReset  = 10 * time.Minute // a return that holds this long resets the backoff
)

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

	// HomeFiler, when set, is preferred whenever it is healthy: after a failover
	// the client returns to it once its circuit breaker resets, backing off if it
	// keeps failing. Embedded S3 gateways set it to their local filer.
	HomeFiler pb.ServerAddress
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
	var homeFiler pb.ServerAddress
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
		homeFiler = opt.HomeFiler
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
		homeFiler:          homeFiler,
		now:                time.Now,
		homeBackoff:        resetTimeout,
	}

	// Start filer discovery if master client is configured
	// Empty filerGroup is valid (represents default group)
	if masterClient != nil {
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

	return fc.filerAddresses[fc.currentIndexPreferHomeLocked()]
}

// currentIndexPreferHomeLocked returns the index to use next: the home filer
// when the current one is not home and home is eligible again (switching to
// it), else the current filer. The caller holds filerAddressesMu.
func (fc *FilerClient) currentIndexPreferHomeLocked() int32 {
	index := fc.loadIndexLocked()
	if index < 0 {
		return 0
	}
	home := fc.homeIndexLocked()
	if home < 0 || home == index {
		return index
	}
	fc.homeMu.Lock()
	defer fc.homeMu.Unlock()
	if index = fc.loadIndexLocked(); index < 0 {
		return 0
	}
	if index != home && fc.homeEligibleLocked(home) {
		fc.moveCurrentIndexLocked(fc.filerAddresses[index], home, switchReturnHome)
		return home
	}
	return index
}

// homeIndexLocked returns the position of the home filer in the list, or -1
// when there is no home or it is not listed. The caller holds filerAddressesMu.
func (fc *FilerClient) homeIndexLocked() int32 {
	if fc.homeFiler == "" {
		return -1
	}
	for i, f := range fc.filerAddresses {
		if sameFiler(f, fc.homeFiler) {
			return int32(i)
		}
	}
	return -1
}

// homeEligibleLocked reports whether the client may return to the home filer
// at index home: it is listed, its circuit breaker is closed or due for a
// re-check, and the return backoff has passed. The caller holds
// filerAddressesMu and homeMu.
func (fc *FilerClient) homeEligibleLocked(home int32) bool {
	if home < 0 || int(home) >= len(fc.filerHealth) {
		return false
	}
	if fc.shouldSkipUnhealthyFilerWithHealth(fc.filerHealth[home]) {
		return false
	}
	return !fc.clock().Before(fc.homeEligibleAt)
}

func (fc *FilerClient) clock() time.Time {
	if fc.now == nil {
		return time.Now()
	}
	return fc.now()
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

	if cur := fc.loadIndexLocked(); cur >= 0 {
		fc.switchCurrentFilerLocked(fc.filerAddresses[cur], addr, switchFailover)
	}
}

// SwitchCurrentFiler moves the client from `from` to `to` after a failover, but
// only while `from` is still the current filer. A caller that picked `from`
// before another request switched the client (for example back to its home
// filer) holds a stale view, and acting on it would undo that switch.
func (fc *FilerClient) SwitchCurrentFiler(from, to pb.ServerAddress) {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	fc.switchCurrentFilerLocked(from, to, switchFailover)
}

// switchCurrentFilerLocked switches from `from` to `to` if `from` is current.
// The caller holds filerAddressesMu (read or write).
func (fc *FilerClient) switchCurrentFilerLocked(from, to pb.ServerAddress, reason string) {
	fc.homeMu.Lock()
	defer fc.homeMu.Unlock()
	cur := fc.loadIndexLocked()
	if cur < 0 || !sameFiler(fc.filerAddresses[cur], from) {
		return
	}
	for i, f := range fc.filerAddresses {
		if sameFiler(f, to) {
			fc.moveCurrentIndexLocked(fc.filerAddresses[cur], int32(i), reason)
			return
		}
	}
}

// loadIndexLocked returns the current index, or -1 when it is out of range.
func (fc *FilerClient) loadIndexLocked() int32 {
	i := atomic.LoadInt32(&fc.filerIndex)
	if i < 0 || i >= int32(len(fc.filerAddresses)) {
		return -1
	}
	return i
}

// ShouldSkipUnhealthyFiler checks if a filer address should be skipped based on health tracking
// Returns true if the filer has exceeded failure threshold and reset timeout hasn't elapsed
func (fc *FilerClient) ShouldSkipUnhealthyFiler(addr pb.ServerAddress) bool {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()

	// Find the health for this filer address
	for i, filer := range fc.filerAddresses {
		if filerKey(filer) == filerKey(addr) {
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
		if filerKey(filer) == filerKey(addr) {
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
		if filerKey(filer) == filerKey(addr) {
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

	generation := fc.peerUpdateGeneration()

	// Query master for filers in our group
	updates, err := cluster.ListExistingPeerUpdates(context.Background(), currentMaster, fc.grpcDialOption, fc.filerGroup, cluster.FilerType)
	if err != nil {
		glog.V(1).Infof("FilerClient: failed to list filers in group '%s': %v", fc.filerGroup, err)
		return
	}

	if len(updates) == 0 {
		glog.V(2).Infof("FilerClient: no filers found in group '%s'", fc.filerGroup)
		return
	}

	// Build new filer address set
	discoveredFilers := make(map[pb.ServerAddress]struct{}, len(updates))
	for _, update := range updates {
		if update.Address != "" {
			discoveredFilers[pb.ServerAddress(update.Address)] = struct{}{}
		}
	}

	// Ignore snapshots whose addresses are all empty; reconciling against an
	// empty set would wipe the in-memory list.
	if len(discoveredFilers) == 0 {
		glog.V(1).Infof("FilerClient: discovery snapshot for group '%s' had no usable addresses, keeping existing list", fc.filerGroup)
		return
	}

	fc.applyDiscoverySnapshot(discoveredFilers, generation)
}

func (fc *FilerClient) peerUpdateGeneration() uint64 {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	return fc.peerUpdates
}

// applyDiscoverySnapshot drops a snapshot requested before a pushed update was
// applied: the push is newer, and the next poll reconciles anything it missed.
func (fc *FilerClient) applyDiscoverySnapshot(discoveredFilers map[pb.ServerAddress]struct{}, generation uint64) {
	fc.filerAddressesMu.Lock()
	defer fc.filerAddressesMu.Unlock()
	if fc.peerUpdates != generation {
		glog.V(1).Infof("FilerClient: discarding discovery snapshot for group '%s' superseded by pushed updates", fc.filerGroup)
		return
	}
	fc.deferredLeaves = nil
	fc.applyDiscoveredFilersLocked(discoveredFilers)
}

// applyDiscoveredFilers treats the master snapshot as authoritative: survivors
// keep their health counters, new addresses get fresh health, addresses missing
// from the snapshot are pruned so replaced pods (e.g. rolled K8s filer pods
// with new IPs) don't linger and get retried after the circuit-breaker reset.
func (fc *FilerClient) applyDiscoveredFilers(discoveredFilers map[pb.ServerAddress]struct{}) {
	fc.filerAddressesMu.Lock()
	defer fc.filerAddressesMu.Unlock()
	fc.applyDiscoveredFilersLocked(discoveredFilers)
}

// OnPeerUpdate applies a filer join/leave pushed by the master, so the list
// tracks a rolling restart instead of waiting for the next discovery poll. A
// leave that would empty the list is deferred until another filer exists to
// take over: an empty list fails every request, while a stale one still
// recovers through the poll.
func (fc *FilerClient) OnPeerUpdate(update *master_pb.ClusterNodeUpdate, _ time.Time) {
	if update.NodeType != cluster.FilerType || update.Address == "" {
		return
	}
	addr := pb.ServerAddress(update.Address)

	fc.filerAddressesMu.Lock()
	defer fc.filerAddressesMu.Unlock()

	changed := false
	filers := make(map[pb.ServerAddress]struct{}, len(fc.filerAddresses)+1)
	for _, f := range fc.filerAddresses {
		if f != addr && filerKey(f) == filerKey(addr) {
			f = addr
			changed = true
		}
		filers[f] = struct{}{}
	}

	if update.IsAdd {
		// A rejoin cancels its deferred leave; bumping the generation keeps an
		// in-flight snapshot that lacks the rejoined filer from pruning it.
		if _, ok := fc.deferredLeaves[addr]; ok {
			delete(fc.deferredLeaves, addr)
			changed = true
		}
		if _, ok := filers[addr]; !ok {
			filers[addr] = struct{}{}
			changed = true
		}
	} else {
		if _, ok := filers[addr]; ok {
			delete(filers, addr)
			changed = true
			if len(filers) == 0 {
				filers[addr] = struct{}{}
				if fc.deferredLeaves == nil {
					fc.deferredLeaves = make(map[pb.ServerAddress]struct{})
				}
				fc.deferredLeaves[addr] = struct{}{}
			}
		}
	}

	// A deferred leave can be honored once another filer exists to take over.
	for gone := range fc.deferredLeaves {
		if _, ok := filers[gone]; ok && len(filers) <= 1 {
			continue
		}
		delete(filers, gone)
		delete(fc.deferredLeaves, gone)
		changed = true
	}

	if !changed {
		return
	}
	fc.peerUpdates++
	fc.applyDiscoveredFilersLocked(filers)
}

// filerKey identifies a filer regardless of whether its address carries the
// gRPC port explicitly (host:port.grpcPort) or implies it (host:port).
func filerKey(addr pb.ServerAddress) string {
	return addr.ToGrpcAddress()
}

func (fc *FilerClient) applyDiscoveredFilersLocked(discoveredFilers map[pb.ServerAddress]struct{}) {
	discovered := make(map[string]pb.ServerAddress, len(discoveredFilers))
	for addr := range discoveredFilers {
		k := filerKey(addr)
		if prev, dup := discovered[k]; !dup || addr > prev {
			discovered[k] = addr
		}
	}

	// Survivors take the discovered address form and keep their health counters.
	changed := false
	kept := make(map[string]struct{}, len(fc.filerAddresses))
	var removedFilers []pb.ServerAddress
	newAddresses := make([]pb.ServerAddress, 0, len(discovered))
	newHealth := make([]*filerHealth, 0, len(discovered))
	for i, f := range fc.filerAddresses {
		k := filerKey(f)
		addr, found := discovered[k]
		if !found {
			removedFilers = append(removedFilers, f)
			continue
		}
		if _, dup := kept[k]; dup {
			changed = true
			continue
		}
		kept[k] = struct{}{}
		if addr != f {
			changed = true
		}
		newAddresses = append(newAddresses, addr)
		newHealth = append(newHealth, fc.filerHealth[i])
	}

	var newFilers []pb.ServerAddress
	for k, addr := range discovered {
		if _, found := kept[k]; !found {
			newFilers = append(newFilers, addr)
		}
	}

	if !changed && len(newFilers) == 0 && len(removedFilers) == 0 {
		return
	}

	// Map iteration order is random; sort so every rebuild lands the same way.
	slices.Sort(newFilers)

	// Remember the active filer so the round-robin pointer can follow it across the rebuild.
	currentIndex := atomic.LoadInt32(&fc.filerIndex)
	var currentFiler pb.ServerAddress
	if currentIndex >= 0 && currentIndex < int32(len(fc.filerAddresses)) {
		currentFiler = fc.filerAddresses[currentIndex]
	}

	for _, f := range newFilers {
		newAddresses = append(newAddresses, f)
		newHealth = append(newHealth, &filerHealth{})
	}

	fc.filerAddresses = newAddresses
	fc.filerHealth = newHealth

	newIndex := int32(-1)
	if currentFiler != "" {
		for i, f := range newAddresses {
			if sameFiler(f, currentFiler) {
				newIndex = int32(i)
				break
			}
		}
	}
	fc.homeMu.Lock()
	if newIndex < 0 {
		newIndex = 0
		if home := fc.homeIndexLocked(); home >= 0 && fc.homeEligibleLocked(home) {
			newIndex = home
		} else if home == 0 && len(newAddresses) > 1 {
			newIndex = 1 // home is still backing off; take the next filer
		}
	}
	fc.moveCurrentIndexLocked(currentFiler, newIndex, switchDiscovery)
	fc.homeMu.Unlock()

	if len(removedFilers) > 0 {
		glog.V(0).Infof("FilerClient: removed %d filer(s) no longer in group '%s': %v", len(removedFilers), fc.filerGroup, removedFilers)
	}
	if len(newFilers) > 0 {
		glog.V(0).Infof("FilerClient: discovered %d new filer(s) in group '%s': %v", len(newFilers), fc.filerGroup, newFilers)
	}
}

// Reasons a FilerClient changes its current filer, as logged on each switch.
const (
	switchFailover   = "failover"
	switchReturnHome = "return_home"
	switchDiscovery  = "discovery"
)

// sameFiler reports whether two addresses name the same filer, whether or not
// either spells out its gRPC port.
func sameFiler(a, b pb.ServerAddress) bool {
	return filerKey(a) == filerKey(b)
}

// moveCurrentIndexLocked makes newIndex current and, when that changes which
// filer is current, logs the switch and feeds the home-filer bookkeeping. The
// caller holds filerAddressesMu and homeMu, so the check that chose newIndex,
// the move and the bookkeeping form one step: a request that sees the new
// index also sees the return backoff it started, and concurrent failovers
// from one filer count once.
func (fc *FilerClient) moveCurrentIndexLocked(from pb.ServerAddress, newIndex int32, reason string) {
	atomic.StoreInt32(&fc.filerIndex, newIndex)
	if newIndex < 0 || newIndex >= int32(len(fc.filerAddresses)) {
		return
	}
	to := fc.filerAddresses[newIndex]
	if from == "" || sameFiler(from, to) {
		return
	}
	backoff := fc.noteSwitchLocked(from, to, reason)
	glog.V(0).Infof("FilerClient: current filer %s -> %s (%s), return backoff %v", from, to, reason, backoff)
}

// noteSwitchLocked records a change of the current filer for the home-filer
// return logic and reports the return backoff now in effect. Leaving home on a
// failover starts the backoff; leaving it again soon after a return doubles it,
// up to homeBackoffCap; a return that held for homeHoldReset resets it. The
// caller holds homeMu.
func (fc *FilerClient) noteSwitchLocked(from, to pb.ServerAddress, reason string) time.Duration {
	if fc.homeBackoff <= 0 {
		fc.homeBackoff = fc.resetTimeout
	}
	if fc.homeFiler == "" {
		return fc.homeBackoff
	}
	now := fc.clock()
	switch {
	case sameFiler(to, fc.homeFiler):
		fc.lastReturnHome = now
	case reason == switchFailover && sameFiler(from, fc.homeFiler) && !sameFiler(to, fc.homeFiler):
		if !fc.lastReturnHome.IsZero() {
			sinceReturn := now.Sub(fc.lastReturnHome)
			if sinceReturn >= homeHoldReset {
				fc.homeBackoff = fc.resetTimeout
			} else if sinceReturn < 2*fc.homeBackoff {
				fc.homeBackoff = min(2*fc.homeBackoff, homeBackoffCap)
			}
		}
		fc.homeEligibleAt = now.Add(fc.homeBackoff)
	}
	return fc.homeBackoff
}

// OnVolumeLocationChange applies master-pushed changes to the lookup-fed cache,
// so a retired server stops being tried before a read against it fails. An
// add lifts the removal fence and caches the volume where the master client
// now knows it to be, replacing what a lookup cached for it.
//
// The replacement matters after a leader change. The reconnect clears this
// cache, and lookups refill it from the filer, whose own master client can
// still answer from the previous leader's view until the new leader has heard
// from the volume servers. A server lost with the old leader is then cached
// again, and the new leader, which never knew it, never pushes its removal.
// Its adds for the volumes that server held are what correct the refill.
func (fc *FilerClient) OnVolumeLocationChange(loc Location, addedVids, deletedVids []uint32) {
	for _, vid := range deletedVids {
		fc.tombstone(vid, loc)
		fc.deleteLocation(vid, loc)
	}
	for _, vid := range addedVids {
		fc.untombstone(vid, loc)
		if fc.masterClient == nil {
			continue
		}
		if locations, found := fc.masterClient.vidMap.currentLocations(vid); found {
			fc.confirmLocations(vid, locations)
		}
	}
}

// OnVidMapReset drops the cache, since removals made while the master link
// was down are never pushed.
func (fc *FilerClient) OnVidMapReset() {
	fc.clearVidMap()
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
		localUrls := make(map[string]bool)
		dataCenter := fc.GetDataCenter()
		for _, loc := range locations {
			url := loc.PublicUrl
			if url == "" {
				url = loc.Url
			}
			httpUrl := "http://" + url + "/" + fileId
			glog.V(4).Infof("lookup %s => %s, data in remote storage tier: %v", fileId, url, loc.DataInRemote)
			if !loc.DataInRemote {
				localUrls[httpUrl] = true
			}
			if dataCenter != "" && dataCenter == loc.DataCenter {
				sameDcUrls = append(sameDcUrls, httpUrl)
			} else {
				otherDcUrls = append(otherDcUrls, httpUrl)
			}
		}
		// Shuffle to distribute load across volume servers
		rand.Shuffle(len(sameDcUrls), func(i, j int) { sameDcUrls[i], sameDcUrls[j] = sameDcUrls[j], sameDcUrls[i] })
		rand.Shuffle(len(otherDcUrls), func(i, j int) { otherDcUrls[i], otherDcUrls[j] = otherDcUrls[j], otherDcUrls[i] })
		// Local replicas go first inside each data center, but never ahead of
		// the data-center preference itself. Mirrors
		// vidMap.LookupVolumeServerUrl so all client lookup paths agree.
		if len(localUrls) > 0 {
			sameDcUrls = util.ReorderToFront(localUrls, sameDcUrls)
			otherDcUrls = util.ReorderToFront(localUrls, otherDcUrls)
		}
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

	// Fallback for non-gRPC errors (e.g. network errors). "connection" and
	// "timeout" are deliberately broader than the shared classifier: a volume
	// lookup is a cheap read-only call, so leaning towards a retry is fine.
	errStr := strings.ToLower(err.Error())
	return util.IsTransientError(err) ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout")
}

// jitter returns a duration in the range [d/2, d) using equal jitter.
// This prevents thundering herds when many clients retry simultaneously
// after a transient failure (e.g., network partition healing).
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)))
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
	if fc.clock().Sub(lastFailureTime) > fc.resetTimeout {
		return false // Time to re-check
	}

	return true // Skip this unhealthy filer
}

// allFilersUnhealthy reports whether the circuit breaker would skip every
// known filer.
func (fc *FilerClient) allFilersUnhealthy() bool {
	fc.filerAddressesMu.RLock()
	defer fc.filerAddressesMu.RUnlock()
	if len(fc.filerHealth) == 0 {
		return false
	}
	for _, health := range fc.filerHealth {
		if !fc.shouldSkipUnhealthyFilerWithHealth(health) {
			return false
		}
	}
	return true
}

// filerUnreachable reports whether a failed lookup means the filer could not
// be reached or could not serve the call, which is what the circuit breaker
// counts. An error the filer sent back as its answer, such as a volume the
// master does not know, says nothing about the filer's health.
func filerUnreachable(err error) bool {
	st, ok := util.ServerStatus(err)
	if !ok {
		// No status from a server: the call never got an answer.
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return false
}

// recordFilerSuccessWithHealth resets failure tracking for a successful filer
func (fc *FilerClient) recordFilerSuccessWithHealth(health *filerHealth) {
	atomic.StoreInt32(&health.failureCount, 0)
}

// recordFilerFailureWithHealth increments failure count for an unhealthy filer
func (fc *FilerClient) recordFilerFailureWithHealth(health *filerHealth) {
	atomic.AddInt32(&health.failureCount, 1)
	atomic.StoreInt64(&health.lastFailureTimeNs, fc.clock().UnixNano())
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
		// Get start index and filer count with read lock
		fc.filerAddressesMu.RLock()
		i := int32(0)
		var startFiler pb.ServerAddress
		if len(fc.filerAddresses) > 0 {
			i = fc.currentIndexPreferHomeLocked()
			startFiler = fc.filerAddresses[i]
		}
		n := int32(len(fc.filerAddresses))
		fc.filerAddressesMu.RUnlock()

		// The breaker steers lookups away from a failing filer towards the
		// others. With every filer in its cool-down there is nowhere to steer,
		// and skipping them all would fail the lookup without asking anyone.
		skipUnhealthy := !fc.allFilersUnhealthy()

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
			if skipUnhealthy && fc.shouldSkipUnhealthyFilerWithHealth(health) {
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
				// a caller that gave up says nothing about this filer; gRPC can
				// surface its own DeadlineExceeded a tick before ctx.Err()
				// becomes visible, so reinterpret only deadline errors this way
				ctxErr := ctx.Err()
				if ctxErr == nil && isDeadlineExceededErr(err) {
					if dl, ok := ctx.Deadline(); ok && !dl.After(time.Now()) {
						ctxErr = context.DeadlineExceeded
					}
				}
				if ctxErr != nil {
					if lastErr != nil {
						return nil, fmt.Errorf("%w (earlier filer error: %v)", ctxErr, lastErr)
					}
					return nil, ctxErr
				}
				glog.V(1).Infof("FilerClient: filer %s lookup failed (attempt %d/%d, retry %d/%d): %v", filerAddress, x+1, n, retry+1, maxRetries, err)
				if filerUnreachable(err) {
					fc.recordFilerFailureWithHealth(health)
				} else {
					// The filer answered, for example that a volume has no
					// location. Counting that against it would open the
					// breaker on a healthy filer and fail every other lookup.
					fc.recordFilerSuccessWithHealth(health)
				}
				lastErr = err
				i++
				if i >= n {
					i = 0
				}
				continue
			}

			// Success - update the preferred filer index and reset health tracking
			// Move only if the lookup had to leave the filer it started on, and
			// only while that filer is still current: a lookup in flight when the
			// client returned home must not count as leaving home.
			if filerAddress != startFiler {
				fc.SwitchCurrentFiler(startFiler, filerAddress)
			}
			fc.recordFilerSuccessWithHealth(health)
			glog.V(3).Infof("FilerClient: looked up %d volumes on %s, found %d", len(volumeIds), filerAddress, len(result))
			return result, nil
		}

		if lastErr == nil {
			// Every filer was skipped: one tripped after the pass chose to skip
			// unhealthy filers. The next pass asks them anyway.
			lastErr = status.Error(codes.Unavailable, "no filer was asked")
		}

		// All filers failed on this attempt
		// Check if the error is retryable (transient gRPC error)
		if !isRetryableGrpcError(lastErr) {
			// Non-retryable error (e.g., NotFound, PermissionDenied) - fail immediately
			return nil, fmt.Errorf("all %d filer(s) failed with non-retryable error: %w", n, lastErr)
		}

		// Transient error - retry if we have attempts left
		if retry < maxRetries-1 {
			jitteredWait := jitter(waitTime)
			glog.V(1).Infof("FilerClient: all %d filer(s) failed with retryable error (attempt %d/%d), retrying in %v: %v",
				n, retry+1, maxRetries, jitteredWait, lastErr)
			timer := time.NewTimer(jitteredWait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			waitTime = time.Duration(float64(waitTime) * fc.retryBackoffFactor)
		}
	}

	// All retries exhausted
	fc.filerAddressesMu.RLock()
	totalFilers := len(fc.filerAddresses)
	fc.filerAddressesMu.RUnlock()
	return nil, fmt.Errorf("all %d filer(s) failed after %d attempts, last error: %w", totalFilers, maxRetries, lastErr)
}
