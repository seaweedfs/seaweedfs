package command

import (
	"path"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type syncJobPaths struct {
	path        util.FullPath
	newPath     util.FullPath // empty for non-renames
	isDirectory bool
}

type MetadataProcessor struct {
	activeJobs       map[int64]*syncJobPaths
	activeJobsLock   sync.Mutex
	activeJobsCond   *sync.Cond
	concurrencyLimit int
	fn               pb.ProcessMetadataFunc
	processedTsWatermark atomic.Int64

	// Indexes for O(depth) conflict detection, replacing O(n) linear scan.
	// activeFilePaths counts active file jobs at each exact path.
	activeFilePaths map[util.FullPath]int
	// activeDirPaths counts active directory jobs at each exact path.
	activeDirPaths map[util.FullPath]int
	// descendantCount counts active jobs (file or dir) strictly under each directory.
	descendantCount map[util.FullPath]int

	// minActiveTs tracks the smallest TsNs in activeJobs for O(1) watermark checks.
	minActiveTs int64
}

func NewMetadataProcessor(fn pb.ProcessMetadataFunc, concurrency int, offsetTsNs int64) *MetadataProcessor {
	t := &MetadataProcessor{
		fn:               fn,
		activeJobs:       make(map[int64]*syncJobPaths),
		concurrencyLimit: concurrency,
		activeFilePaths:  make(map[util.FullPath]int),
		activeDirPaths:   make(map[util.FullPath]int),
		descendantCount:  make(map[util.FullPath]int),
	}
	t.processedTsWatermark.Store(offsetTsNs)
	t.activeJobsCond = sync.NewCond(&t.activeJobsLock)
	return t
}

// pathAncestors returns all proper ancestor directories of p.
// For "/a/b/c", returns ["/a/b", "/a", "/"].
func pathAncestors(p util.FullPath) []util.FullPath {
	var ancestors []util.FullPath
	s := string(p)
	for {
		parent := path.Dir(s)
		if parent == s {
			break
		}
		ancestors = append(ancestors, util.FullPath(parent))
		s = parent
	}
	return ancestors
}

// addPathToIndex registers a path in the conflict detection indexes.
// Must be called under activeJobsLock.
func (t *MetadataProcessor) addPathToIndex(p util.FullPath, isDirectory bool) {
	if isDirectory {
		t.activeDirPaths[p]++
	} else {
		t.activeFilePaths[p]++
	}
	for _, ancestor := range pathAncestors(p) {
		t.descendantCount[ancestor]++
	}
}

// removePathFromIndex unregisters a path from the conflict detection indexes.
// Must be called under activeJobsLock.
func (t *MetadataProcessor) removePathFromIndex(p util.FullPath, isDirectory bool) {
	if isDirectory {
		if t.activeDirPaths[p] <= 1 {
			delete(t.activeDirPaths, p)
		} else {
			t.activeDirPaths[p]--
		}
	} else {
		if t.activeFilePaths[p] <= 1 {
			delete(t.activeFilePaths, p)
		} else {
			t.activeFilePaths[p]--
		}
	}
	for _, ancestor := range pathAncestors(p) {
		if t.descendantCount[ancestor] <= 1 {
			delete(t.descendantCount, ancestor)
		} else {
			t.descendantCount[ancestor]--
		}
	}
}

// pathConflicts checks if a single path conflicts with any active job.
// Conflict rules match pairShouldWaitFor:
//   - file vs file: exact same path
//   - file vs dir: file.IsUnder(dir)
//   - dir vs file: file.IsUnder(dir)
//   - dir vs dir: either IsUnder the other
func (t *MetadataProcessor) pathConflicts(p util.FullPath, isDirectory bool) bool {
	if isDirectory {
		// Any active job (file or dir) strictly under this directory?
		if t.descendantCount[p] > 0 {
			return true
		}
	} else {
		// Exact same file already active?
		if t.activeFilePaths[p] > 0 {
			return true
		}
	}
	// Any active directory that is a proper ancestor of p?
	for _, ancestor := range pathAncestors(p) {
		if t.activeDirPaths[ancestor] > 0 {
			return true
		}
	}
	return false
}

func (t *MetadataProcessor) conflictsWith(resp *filer_pb.SubscribeMetadataResponse) bool {
	p, newPath, isDirectory := extractPathsFromMetadata(resp)
	if t.pathConflicts(p, isDirectory) {
		return true
	}
	if newPath != "" && t.pathConflicts(newPath, isDirectory) {
		return true
	}
	return false
}

func (t *MetadataProcessor) AddSyncJob(resp *filer_pb.SubscribeMetadataResponse) {
	if filer_pb.IsEmpty(resp) {
		return
	}

	t.activeJobsLock.Lock()
	defer t.activeJobsLock.Unlock()

	for len(t.activeJobs) >= t.concurrencyLimit || t.conflictsWith(resp) {
		t.activeJobsCond.Wait()
	}

	p, newPath, isDirectory := extractPathsFromMetadata(resp)
	jobPaths := &syncJobPaths{path: p, newPath: newPath, isDirectory: isDirectory}

	t.activeJobs[resp.TsNs] = jobPaths
	t.addPathToIndex(p, isDirectory)
	if newPath != "" {
		t.addPathToIndex(newPath, isDirectory)
	}

	if t.minActiveTs == 0 || resp.TsNs < t.minActiveTs {
		t.minActiveTs = resp.TsNs
	}

	go func() {

		if err := util.Retry("metadata processor", func() error {
			return t.fn(resp)
		}); err != nil {
			glog.Errorf("process %v: %v", resp, err)
		}

		t.activeJobsLock.Lock()
		defer t.activeJobsLock.Unlock()

		delete(t.activeJobs, resp.TsNs)
		t.removePathFromIndex(jobPaths.path, jobPaths.isDirectory)
		if jobPaths.newPath != "" {
			t.removePathFromIndex(jobPaths.newPath, jobPaths.isDirectory)
		}

		// if is the oldest job, write down the watermark
		if resp.TsNs == t.minActiveTs {
			t.processedTsWatermark.Store(resp.TsNs)
			t.minActiveTs = 0
			for ts := range t.activeJobs {
				if t.minActiveTs == 0 || ts < t.minActiveTs {
					t.minActiveTs = ts
				}
			}
		}
		t.activeJobsCond.Signal()
	}()
}

func extractPathsFromMetadata(resp *filer_pb.SubscribeMetadataResponse) (p, newPath util.FullPath, isDirectory bool) {
	oldEntry := resp.EventNotification.OldEntry
	newEntry := resp.EventNotification.NewEntry
	// create
	if filer_pb.IsCreate(resp) {
		p = util.FullPath(resp.Directory).Child(newEntry.Name)
		isDirectory = newEntry.IsDirectory
		return
	}
	if filer_pb.IsDelete(resp) {
		p = util.FullPath(resp.Directory).Child(oldEntry.Name)
		isDirectory = oldEntry.IsDirectory
		return
	}
	if filer_pb.IsUpdate(resp) {
		p = util.FullPath(resp.Directory).Child(newEntry.Name)
		isDirectory = newEntry.IsDirectory
		return
	}
	// renaming
	p = util.FullPath(resp.Directory).Child(oldEntry.Name)
	isDirectory = oldEntry.IsDirectory
	newPath = util.FullPath(resp.EventNotification.NewParentPath).Child(newEntry.Name)
	return
}
