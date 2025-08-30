package ml

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// PrefetchRequest represents a chunk prefetch request
type PrefetchRequest struct {
	FileId      string
	ChunkIndex  uint32
	Offset      uint64
	Size        uint64
	Priority    int
	Timestamp   time.Time
	Callback    func([]byte, error)
	ctx         context.Context
}

// PrefetchJob tracks an active prefetch operation
type PrefetchJob struct {
	request   *PrefetchRequest
	startTime time.Time
	cancelled int32
}

// PrefetchManager manages background chunk prefetching for ML workloads
type PrefetchManager struct {
	sync.RWMutex
	
	// Configuration
	maxWorkers   int
	queueSize    int
	jobTimeout   time.Duration
	enableMetrics bool
	
	// Worker management
	workers     chan *PrefetchRequest
	activeJobs  map[string]*PrefetchJob
	workerWg    sync.WaitGroup
	
	// Metrics
	totalRequests   int64
	successfulFetch int64
	failedFetch     int64
	duplicateReqs   int64
	timeoutReqs     int64
	
	// Shutdown
	shutdown chan struct{}
	done     chan struct{}
}

// NewPrefetchManager creates a new prefetch manager optimized for ML workloads
func NewPrefetchManager(maxWorkers int, queueSize int, timeout time.Duration) *PrefetchManager {
	if maxWorkers <= 0 {
		maxWorkers = 4 // Default suitable for ML workloads
	}
	if queueSize <= 0 {
		queueSize = 100
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	
	pm := &PrefetchManager{
		maxWorkers:    maxWorkers,
		queueSize:     queueSize,
		jobTimeout:    timeout,
		enableMetrics: true,
		workers:       make(chan *PrefetchRequest, queueSize),
		activeJobs:    make(map[string]*PrefetchJob),
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
	}
	
	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		pm.workerWg.Add(1)
		go pm.worker(i)
	}
	
	// Start cleanup goroutine for expired jobs
	go pm.cleanupWorker()
	
	glog.V(1).Infof("PrefetchManager started with %d workers, queue size %d", maxWorkers, queueSize)
	return pm
}

// Prefetch requests background fetching of a chunk
// Returns true if request was queued, false if duplicate or queue full
func (pm *PrefetchManager) Prefetch(ctx context.Context, fileId string, chunkIndex uint32, offset, size uint64, priority int, callback func([]byte, error)) bool {
	atomic.AddInt64(&pm.totalRequests, 1)
	
	// Create job key for deduplication
	jobKey := pm.makeJobKey(fileId, chunkIndex)
	
	pm.Lock()
	// Check for duplicate requests
	if _, exists := pm.activeJobs[jobKey]; exists {
		pm.Unlock()
		atomic.AddInt64(&pm.duplicateReqs, 1)
		glog.V(4).Infof("Duplicate prefetch request for %s chunk %d", fileId, chunkIndex)
		return false
	}
	
	request := &PrefetchRequest{
		FileId:     fileId,
		ChunkIndex: chunkIndex,
		Offset:     offset,
		Size:       size,
		Priority:   priority,
		Timestamp:  time.Now(),
		Callback:   callback,
		ctx:        ctx,
	}
	
	job := &PrefetchJob{
		request:   request,
		startTime: time.Now(),
	}
	
	pm.activeJobs[jobKey] = job
	pm.Unlock()
	
	// Try to queue the request
	select {
	case pm.workers <- request:
		glog.V(4).Infof("Queued prefetch for %s chunk %d (priority %d)", fileId, chunkIndex, priority)
		return true
	default:
		// Queue is full, remove from active jobs
		pm.Lock()
		delete(pm.activeJobs, jobKey)
		pm.Unlock()
		glog.V(3).Infof("Prefetch queue full, dropping request for %s chunk %d", fileId, chunkIndex)
		return false
	}
}

// worker processes prefetch requests
func (pm *PrefetchManager) worker(workerID int) {
	defer pm.workerWg.Done()
	
	glog.V(4).Infof("Prefetch worker %d started", workerID)
	
	for {
		select {
		case request := <-pm.workers:
			pm.processRequest(workerID, request)
		case <-pm.shutdown:
			glog.V(4).Infof("Prefetch worker %d shutting down", workerID)
			return
		}
	}
}

// processRequest handles a single prefetch request
func (pm *PrefetchManager) processRequest(workerID int, request *PrefetchRequest) {
	jobKey := pm.makeJobKey(request.FileId, request.ChunkIndex)
	startTime := time.Now()
	
	glog.V(4).Infof("Worker %d processing prefetch for %s chunk %d", workerID, request.FileId, request.ChunkIndex)
	
	// Check if job was cancelled
	pm.RLock()
	job, exists := pm.activeJobs[jobKey]
	pm.RUnlock()
	
	if !exists {
		glog.V(4).Infof("Job %s already cancelled or completed", jobKey)
		return
	}
	
	if atomic.LoadInt32(&job.cancelled) == 1 {
		glog.V(4).Infof("Job %s was cancelled", jobKey)
		pm.removeJob(jobKey)
		return
	}
	
	// Create timeout context
	ctx, cancel := context.WithTimeout(request.ctx, pm.jobTimeout)
	defer cancel()
	
	// TODO: Implement actual chunk fetching logic
	// For now, simulate the work and call the callback
	data, err := pm.fetchChunk(ctx, request)
	
	// Update metrics
	duration := time.Since(startTime)
	if err != nil {
		atomic.AddInt64(&pm.failedFetch, 1)
		if ctx.Err() == context.DeadlineExceeded {
			atomic.AddInt64(&pm.timeoutReqs, 1)
		}
		glog.V(3).Infof("Worker %d failed to prefetch %s chunk %d after %v: %v", workerID, request.FileId, request.ChunkIndex, duration, err)
	} else {
		atomic.AddInt64(&pm.successfulFetch, 1)
		glog.V(4).Infof("Worker %d successfully prefetched %s chunk %d in %v (%d bytes)", workerID, request.FileId, request.ChunkIndex, duration, len(data))
	}
	
	// Call the callback if provided
	if request.Callback != nil {
		request.Callback(data, err)
	}
	
	// Remove job from active jobs
	pm.removeJob(jobKey)
}

// fetchChunk performs the actual chunk fetch operation
// TODO: Integrate with existing SeaweedFS chunk reading logic
func (pm *PrefetchManager) fetchChunk(ctx context.Context, request *PrefetchRequest) ([]byte, error) {
	// This is a placeholder implementation
	// In the real implementation, this would:
	// 1. Use the existing chunk cache to check if chunk is already cached
	// 2. If not cached, fetch from volume servers using existing logic
	// 3. Store in cache for future use
	
	glog.V(4).Infof("Simulating fetch of %s chunk %d (offset %d, size %d)", 
		request.FileId, request.ChunkIndex, request.Offset, request.Size)
	
	// Simulate some work
	select {
	case <-time.After(10 * time.Millisecond):
		// Return empty data for now
		return make([]byte, request.Size), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel cancels a pending or active prefetch request
func (pm *PrefetchManager) Cancel(fileId string, chunkIndex uint32) bool {
	jobKey := pm.makeJobKey(fileId, chunkIndex)
	
	pm.RLock()
	job, exists := pm.activeJobs[jobKey]
	pm.RUnlock()
	
	if !exists {
		return false
	}
	
	atomic.StoreInt32(&job.cancelled, 1)
	glog.V(4).Infof("Cancelled prefetch for %s chunk %d", fileId, chunkIndex)
	return true
}

// cleanupWorker periodically removes expired jobs
func (pm *PrefetchManager) cleanupWorker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			pm.cleanup()
		case <-pm.shutdown:
			return
		}
	}
}

// cleanup removes expired jobs
func (pm *PrefetchManager) cleanup() {
	now := time.Now()
	expiredJobKeys := make([]string, 0)
	
	pm.RLock()
	for jobKey, job := range pm.activeJobs {
		if now.Sub(job.startTime) > pm.jobTimeout*2 { // Give extra time for cleanup
			expiredJobKeys = append(expiredJobKeys, jobKey)
		}
	}
	pm.RUnlock()
	
	if len(expiredJobKeys) > 0 {
		pm.Lock()
		for _, jobKey := range expiredJobKeys {
			delete(pm.activeJobs, jobKey)
		}
		pm.Unlock()
		
		glog.V(3).Infof("Cleaned up %d expired prefetch jobs", len(expiredJobKeys))
	}
}

// GetMetrics returns current prefetch metrics
func (pm *PrefetchManager) GetMetrics() PrefetchMetrics {
	pm.RLock()
	activeJobCount := len(pm.activeJobs)
	pm.RUnlock()
	
	return PrefetchMetrics{
		TotalRequests:   atomic.LoadInt64(&pm.totalRequests),
		SuccessfulFetch: atomic.LoadInt64(&pm.successfulFetch),
		FailedFetch:     atomic.LoadInt64(&pm.failedFetch),
		DuplicateReqs:   atomic.LoadInt64(&pm.duplicateReqs),
		TimeoutReqs:     atomic.LoadInt64(&pm.timeoutReqs),
		ActiveJobs:      int64(activeJobCount),
		Workers:         int64(pm.maxWorkers),
	}
}

// PrefetchMetrics holds prefetch performance metrics
type PrefetchMetrics struct {
	TotalRequests   int64
	SuccessfulFetch int64
	FailedFetch     int64
	DuplicateReqs   int64
	TimeoutReqs     int64
	ActiveJobs      int64
	Workers         int64
}

// Shutdown gracefully shuts down the prefetch manager
func (pm *PrefetchManager) Shutdown() {
	glog.V(1).Infof("Shutting down PrefetchManager...")
	
	close(pm.shutdown)
	
	// Wait for workers to finish
	pm.workerWg.Wait()
	
	// Clear active jobs
	pm.Lock()
	pm.activeJobs = make(map[string]*PrefetchJob)
	pm.Unlock()
	
	close(pm.done)
	glog.V(1).Infof("PrefetchManager shutdown complete")
}

// Helper methods

func (pm *PrefetchManager) makeJobKey(fileId string, chunkIndex uint32) string {
	return fileId + ":" + string(rune(chunkIndex))
}

func (pm *PrefetchManager) removeJob(jobKey string) {
	pm.Lock()
	delete(pm.activeJobs, jobKey)
	pm.Unlock()
}
