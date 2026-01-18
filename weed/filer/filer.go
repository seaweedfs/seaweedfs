package filer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer/empty_folder_cleanup"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	LogFlushInterval = time.Minute
	PaginationSize   = 1024
	FilerStoreId     = "filer.store.id"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type Filer struct {
	UniqueFilerId       int32
	UniqueFilerEpoch    int32
	Store               VirtualFilerStore
	MasterClient        *wdclient.MasterClient
	fileIdDeletionQueue *util.UnboundedQueue
	GrpcDialOption      grpc.DialOption
	DirBucketsPath      string
	Cipher              bool
	LocalMetaLogBuffer  *log_buffer.LogBuffer
	metaLogCollection   string
	metaLogReplication  string
	MetaAggregator      *MetaAggregator
	Signature           int32
	FilerConf           *FilerConf
	RemoteStorage       *FilerRemoteStorage
	Dlm                 *lock_manager.DistributedLockManager
	MaxFilenameLength   uint32
	deletionQuit        chan struct{}
	DeletionRetryQueue  *DeletionRetryQueue
	EmptyFolderCleaner  *empty_folder_cleanup.EmptyFolderCleaner
}

func NewFiler(masters pb.ServerDiscovery, grpcDialOption grpc.DialOption, filerHost pb.ServerAddress, filerGroup string, collection string, replication string, dataCenter string, maxFilenameLength uint32, notifyFn func()) *Filer {
	f := &Filer{
		MasterClient:        wdclient.NewMasterClient(grpcDialOption, filerGroup, cluster.FilerType, filerHost, dataCenter, "", masters),
		fileIdDeletionQueue: util.NewUnboundedQueue(),
		GrpcDialOption:      grpcDialOption,
		FilerConf:           NewFilerConf(),
		RemoteStorage:       NewFilerRemoteStorage(),
		UniqueFilerId:       util.RandomInt32(),
		Dlm:                 lock_manager.NewDistributedLockManager(filerHost),
		MaxFilenameLength:   maxFilenameLength,
		deletionQuit:        make(chan struct{}),
		DeletionRetryQueue:  NewDeletionRetryQueue(),
	}
	if f.UniqueFilerId < 0 {
		f.UniqueFilerId = -f.UniqueFilerId
	}

	f.LocalMetaLogBuffer = log_buffer.NewLogBuffer("local", LogFlushInterval, f.logFlushFunc, nil, notifyFn)
	f.metaLogCollection = collection
	f.metaLogReplication = replication

	go f.loopProcessingDeletion()

	return f
}

func (f *Filer) MaybeBootstrapFromOnePeer(self pb.ServerAddress, existingNodes []*master_pb.ClusterNodeUpdate, snapshotTime time.Time) (err error) {
	if len(existingNodes) == 0 {
		return
	}
	sort.Slice(existingNodes, func(i, j int) bool {
		return existingNodes[i].CreatedAtNs < existingNodes[j].CreatedAtNs
	})
	earliestNode := existingNodes[0]
	if earliestNode.Address == string(self) {
		return
	}

	glog.V(0).Infof("bootstrap from %v clientId:%d", earliestNode.Address, f.UniqueFilerId)

	return pb.WithFilerClient(false, f.UniqueFilerId, pb.ServerAddress(earliestNode.Address), f.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.StreamBfs(client, "/", snapshotTime.UnixNano(), func(parentPath util.FullPath, entry *filer_pb.Entry) error {
			return f.Store.InsertEntry(context.Background(), FromPbEntry(string(parentPath), entry))
		})
	})

}

func (f *Filer) AggregateFromPeers(self pb.ServerAddress, existingNodes []*master_pb.ClusterNodeUpdate, startFrom time.Time) {

	var snapshot []pb.ServerAddress
	for _, node := range existingNodes {
		address := pb.ServerAddress(node.Address)
		snapshot = append(snapshot, address)
	}
	f.Dlm.LockRing.SetSnapshot(snapshot)
	glog.V(0).Infof("%s aggregate from peers %+v", self, snapshot)

	// Initialize the empty folder cleaner using the same LockRing as Dlm for consistent hashing
	f.EmptyFolderCleaner = empty_folder_cleanup.NewEmptyFolderCleaner(f, f.Dlm.LockRing, self, f.DirBucketsPath)

	f.MetaAggregator = NewMetaAggregator(f, self, f.GrpcDialOption)
	f.MasterClient.SetOnPeerUpdateFn(func(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
		if update.NodeType != cluster.FilerType {
			return
		}
		address := pb.ServerAddress(update.Address)

		if update.IsAdd {
			f.Dlm.LockRing.AddServer(address)
		} else {
			f.Dlm.LockRing.RemoveServer(address)
		}
		f.MetaAggregator.OnPeerUpdate(update, startFrom)
	})

	for _, peerUpdate := range existingNodes {
		f.MetaAggregator.OnPeerUpdate(peerUpdate, startFrom)
	}

}

func (f *Filer) ListExistingPeerUpdates(ctx context.Context) (existingNodes []*master_pb.ClusterNodeUpdate) {
	return cluster.ListExistingPeerUpdates(f.GetMaster(ctx), f.GrpcDialOption, f.MasterClient.FilerGroup, cluster.FilerType)
}

func (f *Filer) SetStore(store FilerStore) (isFresh bool) {
	f.Store = NewFilerStoreWrapper(store)

	return f.setOrLoadFilerStoreSignature(store)
}

func (f *Filer) setOrLoadFilerStoreSignature(store FilerStore) (isFresh bool) {
	storeIdBytes, err := store.KvGet(context.Background(), []byte(FilerStoreId))
	if err == ErrKvNotFound || err == nil && len(storeIdBytes) == 0 {
		f.Signature = util.RandomInt32()
		storeIdBytes = make([]byte, 4)
		util.Uint32toBytes(storeIdBytes, uint32(f.Signature))
		if err = store.KvPut(context.Background(), []byte(FilerStoreId), storeIdBytes); err != nil {
			glog.Fatalf("set %s=%d : %v", FilerStoreId, f.Signature, err)
		}
		glog.V(0).Infof("create %s to %d", FilerStoreId, f.Signature)
		return true
	} else if err == nil && len(storeIdBytes) == 4 {
		f.Signature = int32(util.BytesToUint32(storeIdBytes))
		glog.V(0).Infof("existing %s = %d", FilerStoreId, f.Signature)
	} else {
		glog.Fatalf("read %v=%v : %v", FilerStoreId, string(storeIdBytes), err)
	}
	return false
}

func (f *Filer) GetStore() (store FilerStore) {
	return f.Store
}

func (fs *Filer) GetMaster(ctx context.Context) pb.ServerAddress {
	return fs.MasterClient.GetMaster(ctx)
}

func (f *Filer) BeginTransaction(ctx context.Context) (context.Context, error) {
	return f.Store.BeginTransaction(ctx)
}

func (f *Filer) CommitTransaction(ctx context.Context) error {
	return f.Store.CommitTransaction(ctx)
}

func (f *Filer) RollbackTransaction(ctx context.Context) error {
	return f.Store.RollbackTransaction(ctx)
}

func (f *Filer) CreateEntry(ctx context.Context, entry *Entry, o_excl bool, isFromOtherCluster bool, signatures []int32, skipCreateParentDir bool, maxFilenameLength uint32) error {

	if string(entry.FullPath) == "/" {
		return nil
	}

	if entry.FullPath.IsLongerFileName(maxFilenameLength) {
		return fmt.Errorf("entry name too long")
	}

	if entry.IsDirectory() {
		entry.Attr.TtlSec = 0
	}

	oldEntry, _ := f.FindEntry(ctx, entry.FullPath)

	/*
		if !hasWritePermission(lastDirectoryEntry, entry) {
			glog.V(0).Infof("directory %s: %v, entry: uid=%d gid=%d",
				lastDirectoryEntry.FullPath, lastDirectoryEntry.Attr, entry.Uid, entry.Gid)
			return fmt.Errorf("no write permission in folder %v", lastDirectoryEntry.FullPath)
		}
	*/

	if oldEntry == nil {

		if !skipCreateParentDir {
			dirParts := strings.Split(string(entry.FullPath), "/")
			if err := f.ensureParentDirectoryEntry(ctx, entry, dirParts, len(dirParts)-1, isFromOtherCluster); err != nil {
				return err
			}
		}

		glog.V(4).InfofCtx(ctx, "InsertEntry %s: new entry: %v", entry.FullPath, entry.Name())
		if err := f.Store.InsertEntry(ctx, entry); err != nil {
			glog.ErrorfCtx(ctx, "insert entry %s: %v", entry.FullPath, err)
			return fmt.Errorf("insert entry %s: %v", entry.FullPath, err)
		}
	} else {
		if o_excl {
			glog.V(3).InfofCtx(ctx, "EEXIST: entry %s already exists", entry.FullPath)
			return fmt.Errorf("EEXIST: entry %s already exists", entry.FullPath)
		}
		glog.V(4).InfofCtx(ctx, "UpdateEntry %s: old entry: %v", entry.FullPath, oldEntry.Name())
		if err := f.UpdateEntry(ctx, oldEntry, entry); err != nil {
			glog.ErrorfCtx(ctx, "update entry %s: %v", entry.FullPath, err)
			return fmt.Errorf("update entry %s: %v", entry.FullPath, err)
		}
	}

	f.NotifyUpdateEvent(ctx, oldEntry, entry, true, isFromOtherCluster, signatures)

	f.deleteChunksIfNotNew(ctx, oldEntry, entry)

	glog.V(4).InfofCtx(ctx, "CreateEntry %s: created", entry.FullPath)

	return nil
}

func (f *Filer) ensureParentDirectoryEntry(ctx context.Context, entry *Entry, dirParts []string, level int, isFromOtherCluster bool) (err error) {

	if level == 0 {
		return nil
	}

	dirPath := "/" + util.Join(dirParts[:level]...)
	// fmt.Printf("%d dirPath: %+v\n", level, dirPath)

	// check the store directly
	glog.V(4).InfofCtx(ctx, "find uncached directory: %s", dirPath)
	dirEntry, findErr := f.FindEntry(ctx, util.FullPath(dirPath))
	if findErr != nil && !errors.Is(findErr, filer_pb.ErrNotFound) {
		return findErr
	}

	// no such existing directory
	if dirEntry == nil {

		// fmt.Printf("dirParts: %v %v %v\n", dirParts[0], dirParts[1], dirParts[2])
		// dirParts[0] == "" and dirParts[1] == "buckets"
		if len(dirParts) >= 3 && dirParts[1] == "buckets" {
			if err := s3bucket.VerifyS3BucketName(dirParts[2]); err != nil {
				return fmt.Errorf("invalid bucket name %s: %v", dirParts[2], err)
			}
		}

		// ensure parent directory
		if err = f.ensureParentDirectoryEntry(ctx, entry, dirParts, level-1, isFromOtherCluster); err != nil {
			return err
		}

		// create the directory
		now := time.Now()

		dirEntry = &Entry{
			FullPath: util.FullPath(dirPath),
			Attr: Attr{
				Mtime:      now,
				Crtime:     now,
				Mode:       os.ModeDir | entry.Mode | 0111,
				Uid:        entry.Uid,
				Gid:        entry.Gid,
				UserName:   entry.UserName,
				GroupNames: entry.GroupNames,
			},
		}
		if len(dirParts) >= 3 && dirParts[1] == "buckets" {
			dirEntry.Extended = map[string][]byte{
				s3_constants.ExtS3ImplicitDir: []byte("true"),
			}
		}

		glog.V(2).InfofCtx(ctx, "create directory: %s %v", dirPath, dirEntry.Mode)
		mkdirErr := f.Store.InsertEntry(ctx, dirEntry)
		if mkdirErr != nil {
			if fEntry, err := f.FindEntry(ctx, util.FullPath(dirPath)); err == filer_pb.ErrNotFound || fEntry == nil {
				glog.V(3).InfofCtx(ctx, "mkdir %s: %v", dirPath, mkdirErr)
				return fmt.Errorf("mkdir %s: %v", dirPath, mkdirErr)
			}
		} else {
			if !strings.HasPrefix("/"+util.Join(dirParts[:]...), SystemLogDir) {
				f.NotifyUpdateEvent(ctx, nil, dirEntry, false, isFromOtherCluster, nil)
			}
		}

	} else if !dirEntry.IsDirectory() {
		glog.ErrorfCtx(ctx, "CreateEntry %s: %s should be a directory", entry.FullPath, dirPath)
		return fmt.Errorf("%s is a file", dirPath)
	}

	return nil
}

func (f *Filer) UpdateEntry(ctx context.Context, oldEntry, entry *Entry) (err error) {
	if oldEntry != nil {
		entry.Attr.Crtime = oldEntry.Attr.Crtime
		if oldEntry.IsDirectory() && !entry.IsDirectory() {
			glog.ErrorfCtx(ctx, "existing %s is a directory", oldEntry.FullPath)
			return fmt.Errorf("existing %s is a directory", oldEntry.FullPath)
		}
		if !oldEntry.IsDirectory() && entry.IsDirectory() {
			glog.ErrorfCtx(ctx, "existing %s is a file", oldEntry.FullPath)
			return fmt.Errorf("existing %s is a file", oldEntry.FullPath)
		}
	}
	return f.Store.UpdateEntry(ctx, entry)
}

var (
	Root = &Entry{
		FullPath: "/",
		Attr: Attr{
			Mtime:  time.Now(),
			Crtime: time.Now(),
			Mode:   os.ModeDir | 0755,
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
	}
)

func (f *Filer) FindEntry(ctx context.Context, p util.FullPath) (entry *Entry, err error) {

	if string(p) == "/" {
		return Root, nil
	}
	entry, err = f.Store.FindEntry(ctx, p)
	if entry != nil && entry.TtlSec > 0 {
		if entry.IsExpireS3Enabled() {
			if entry.GetS3ExpireTime().Before(time.Now()) && !entry.IsS3Versioning() {
				if delErr := f.doDeleteEntryMetaAndData(ctx, entry, true, false, nil); delErr != nil {
					glog.ErrorfCtx(ctx, "FindEntry doDeleteEntryMetaAndData %s failed: %v", entry.FullPath, delErr)
				}
				return nil, filer_pb.ErrNotFound
			}
		} else if entry.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
			f.Store.DeleteOneEntry(ctx, entry)
			return nil, filer_pb.ErrNotFound
		}
	}

	return entry, err
}

func (f *Filer) doListDirectoryEntries(ctx context.Context, p util.FullPath, startFileName string, inclusive bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (expiredCount int64, lastFileName string, err error) {
	// Collect expired entries during iteration to avoid deadlock with DB connection pool
	var expiredEntries []*Entry
	var s3ExpiredEntries []*Entry
	var hasValidEntries bool

	lastFileName, err = f.Store.ListDirectoryPrefixedEntries(ctx, p, startFileName, inclusive, limit, prefix, func(entry *Entry) (bool, error) {
		select {
		case <-ctx.Done():
			glog.Errorf("Context is done.")
			return false, fmt.Errorf("context canceled: %w", ctx.Err())
		default:
			if entry.TtlSec > 0 {
				if entry.IsExpireS3Enabled() {
					if entry.GetS3ExpireTime().Before(time.Now()) && !entry.IsS3Versioning() {
						// Collect for deletion after iteration completes to avoid DB deadlock
						s3ExpiredEntries = append(s3ExpiredEntries, entry)
						expiredCount++
						return true, nil
					}
				} else if entry.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
					// Collect for deletion after iteration completes to avoid DB deadlock
					expiredEntries = append(expiredEntries, entry)
					expiredCount++
					return true, nil
				}
			}
			// Track that we found at least one valid (non-expired) entry
			hasValidEntries = true
			return eachEntryFunc(entry)
		}
	})
	if err != nil {
		return expiredCount, lastFileName, err
	}

	// Delete expired entries after iteration completes to avoid DB connection deadlock
	if len(s3ExpiredEntries) > 0 || len(expiredEntries) > 0 {
		for _, entry := range s3ExpiredEntries {
			if delErr := f.doDeleteEntryMetaAndData(ctx, entry, true, false, nil); delErr != nil {
				glog.ErrorfCtx(ctx, "doListDirectoryEntries doDeleteEntryMetaAndData %s failed: %v", entry.FullPath, delErr)
			}
		}
		for _, entry := range expiredEntries {
			if delErr := f.Store.DeleteOneEntry(ctx, entry); delErr != nil {
				glog.ErrorfCtx(ctx, "doListDirectoryEntries DeleteOneEntry %s failed: %v", entry.FullPath, delErr)
			}
		}

		// After expiring entries, the directory might be empty.
		// Attempt to clean it up and any empty parent directories.
		if !hasValidEntries && p != "/" && startFileName == "" {
			stopAtPath := util.FullPath(f.DirBucketsPath)
			f.DeleteEmptyParentDirectories(ctx, p, stopAtPath)
		}
	}

	return
}

// DeleteEmptyParentDirectories recursively checks and deletes parent directories if they become empty.
// It stops at root "/" or at stopAtPath (if provided).
// This is useful for cleaning up directories after deleting files or expired entries.
//
// IMPORTANT: For safety, dirPath must be under stopAtPath (when stopAtPath is provided).
// This prevents accidental deletion of directories outside the intended scope (e.g., outside bucket paths).
//
// Example usage:
//
//	// After deleting /bucket/dir/subdir/file.txt, clean up empty parent directories
//	// but stop at the bucket path
//	parentPath := util.FullPath("/bucket/dir/subdir")
//	filer.DeleteEmptyParentDirectories(ctx, parentPath, util.FullPath("/bucket"))
//
// Example with gRPC client:
//
//	if err := pb_filer_client.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
//	    return filer_pb.Traverse(ctx, filer, parentPath, "", func(entry *filer_pb.Entry) error {
//	        // Process entries...
//	    })
//	}); err == nil {
//	    filer.DeleteEmptyParentDirectories(ctx, parentPath, stopPath)
//	}
func (f *Filer) DeleteEmptyParentDirectories(ctx context.Context, dirPath util.FullPath, stopAtPath util.FullPath) {
	if dirPath == "/" || dirPath == stopAtPath {
		return
	}

	// Safety check: if stopAtPath is provided, dirPath must be under it (root "/" allows everything)
	stopStr := string(stopAtPath)
	if stopAtPath != "" && stopStr != "/" && !strings.HasPrefix(string(dirPath)+"/", stopStr+"/") {
		glog.V(1).InfofCtx(ctx, "DeleteEmptyParentDirectories: %s is not under %s, skipping", dirPath, stopAtPath)
		return
	}

	// Additional safety: prevent deletion of bucket-level directories
	// This protects /buckets/mybucket from being deleted even if empty
	baseDepth := strings.Count(f.DirBucketsPath, "/")
	dirDepth := strings.Count(string(dirPath), "/")
	if dirDepth <= baseDepth+1 {
		glog.V(2).InfofCtx(ctx, "DeleteEmptyParentDirectories: skipping deletion of bucket-level directory %s", dirPath)
		return
	}

	// Check if directory is empty
	isEmpty, err := f.IsDirectoryEmpty(ctx, dirPath)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "DeleteEmptyParentDirectories: error checking %s: %v", dirPath, err)
		return
	}

	if !isEmpty {
		// Directory is not empty, stop checking upward
		glog.V(3).InfofCtx(ctx, "DeleteEmptyParentDirectories: directory %s is not empty, stopping cleanup", dirPath)
		return
	}

	// Directory is empty, try to delete it
	glog.V(2).InfofCtx(ctx, "DeleteEmptyParentDirectories: deleting empty directory %s", dirPath)
	parentDir, _ := dirPath.DirAndName()
	if dirEntry, findErr := f.FindEntry(ctx, dirPath); findErr == nil {
		if delErr := f.doDeleteEntryMetaAndData(ctx, dirEntry, false, false, nil); delErr == nil {
			// Successfully deleted, continue checking upwards
			f.DeleteEmptyParentDirectories(ctx, util.FullPath(parentDir), stopAtPath)
		} else {
			// Failed to delete, stop cleanup
			glog.V(3).InfofCtx(ctx, "DeleteEmptyParentDirectories: failed to delete %s: %v", dirPath, delErr)
		}
	}
}

// IsDirectoryEmpty checks if a directory contains any entries
func (f *Filer) IsDirectoryEmpty(ctx context.Context, dirPath util.FullPath) (bool, error) {
	isEmpty := true
	_, err := f.Store.ListDirectoryPrefixedEntries(ctx, dirPath, "", true, 1, "", func(entry *Entry) (bool, error) {
		isEmpty = false
		return false, nil // Stop after first entry
	})
	return isEmpty, err
}

func (f *Filer) Shutdown() {
	close(f.deletionQuit)
	if f.EmptyFolderCleaner != nil {
		f.EmptyFolderCleaner.Stop()
	}
	f.LocalMetaLogBuffer.ShutdownLogBuffer()
	f.Store.Shutdown()
}

func (f *Filer) GetEntryAttributes(ctx context.Context, p util.FullPath) (map[string][]byte, error) {
	entry, err := f.FindEntry(ctx, p)
	if err != nil {
		return nil, err
	}
	return entry.Extended, nil
}
