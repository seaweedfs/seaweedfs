package filer2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/karlseguin/ccache"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

const PaginationSize = 1024 * 256

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type Filer struct {
	store               *FilerStoreWrapper
	directoryCache      *ccache.Cache
	MasterClient        *wdclient.MasterClient
	fileIdDeletionQueue *util.UnboundedQueue
	GrpcDialOption      grpc.DialOption
	DirBucketsPath      string
	DirQueuesPath       string
	buckets             *FilerBuckets
	Cipher              bool
}

func NewFiler(masters []string, grpcDialOption grpc.DialOption, filerGrpcPort uint32) *Filer {
	f := &Filer{
		directoryCache:      ccache.New(ccache.Configure().MaxSize(1000).ItemsToPrune(100)),
		MasterClient:        wdclient.NewMasterClient(grpcDialOption, "filer", filerGrpcPort, masters),
		fileIdDeletionQueue: util.NewUnboundedQueue(),
		GrpcDialOption:      grpcDialOption,
	}

	go f.loopProcessingDeletion()

	return f
}

func (f *Filer) SetStore(store FilerStore) {
	f.store = NewFilerStoreWrapper(store)
}

func (f *Filer) DisableDirectoryCache() {
	f.directoryCache = nil
}

func (fs *Filer) GetMaster() string {
	return fs.MasterClient.GetMaster()
}

func (fs *Filer) KeepConnectedToMaster() {
	fs.MasterClient.KeepConnectedToMaster()
}

func (f *Filer) BeginTransaction(ctx context.Context) (context.Context, error) {
	return f.store.BeginTransaction(ctx)
}

func (f *Filer) CommitTransaction(ctx context.Context) error {
	return f.store.CommitTransaction(ctx)
}

func (f *Filer) RollbackTransaction(ctx context.Context) error {
	return f.store.RollbackTransaction(ctx)
}

func (f *Filer) CreateEntry(ctx context.Context, entry *Entry, o_excl bool) error {

	if string(entry.FullPath) == "/" {
		return nil
	}

	dirParts := strings.Split(string(entry.FullPath), "/")

	// fmt.Printf("directory parts: %+v\n", dirParts)

	var lastDirectoryEntry *Entry

	for i := 1; i < len(dirParts); i++ {
		dirPath := "/" + filepath.ToSlash(filepath.Join(dirParts[:i]...))
		// fmt.Printf("%d directory: %+v\n", i, dirPath)

		// first check local cache
		dirEntry := f.cacheGetDirectory(dirPath)

		// not found, check the store directly
		if dirEntry == nil {
			glog.V(4).Infof("find uncached directory: %s", dirPath)
			dirEntry, _ = f.FindEntry(ctx, FullPath(dirPath))
		} else {
			// glog.V(4).Infof("found cached directory: %s", dirPath)
		}

		// no such existing directory
		if dirEntry == nil {

			// create the directory
			now := time.Now()

			dirEntry = &Entry{
				FullPath: FullPath(dirPath),
				Attr: Attr{
					Mtime:       now,
					Crtime:      now,
					Mode:        os.ModeDir | 0770,
					Uid:         entry.Uid,
					Gid:         entry.Gid,
					Collection:  entry.Collection,
					Replication: entry.Replication,
				},
			}

			glog.V(2).Infof("create directory: %s %v", dirPath, dirEntry.Mode)
			mkdirErr := f.store.InsertEntry(ctx, dirEntry)
			if mkdirErr != nil {
				if _, err := f.FindEntry(ctx, FullPath(dirPath)); err == ErrNotFound {
					glog.V(3).Infof("mkdir %s: %v", dirPath, mkdirErr)
					return fmt.Errorf("mkdir %s: %v", dirPath, mkdirErr)
				}
			} else {
				f.maybeAddBucket(dirEntry)
				f.NotifyUpdateEvent(nil, dirEntry, false)
			}

		} else if !dirEntry.IsDirectory() {
			glog.Errorf("CreateEntry %s: %s should be a directory", entry.FullPath, dirPath)
			return fmt.Errorf("%s is a file", dirPath)
		}

		// cache the directory entry
		f.cacheSetDirectory(dirPath, dirEntry, i)

		// remember the direct parent directory entry
		if i == len(dirParts)-1 {
			lastDirectoryEntry = dirEntry
		}

	}

	if lastDirectoryEntry == nil {
		glog.Errorf("CreateEntry %s: lastDirectoryEntry is nil", entry.FullPath)
		return fmt.Errorf("parent folder not found: %v", entry.FullPath)
	}

	/*
		if !hasWritePermission(lastDirectoryEntry, entry) {
			glog.V(0).Infof("directory %s: %v, entry: uid=%d gid=%d",
				lastDirectoryEntry.FullPath, lastDirectoryEntry.Attr, entry.Uid, entry.Gid)
			return fmt.Errorf("no write permission in folder %v", lastDirectoryEntry.FullPath)
		}
	*/

	oldEntry, _ := f.FindEntry(ctx, entry.FullPath)

	glog.V(4).Infof("CreateEntry %s: old entry: %v exclusive:%v", entry.FullPath, oldEntry, o_excl)
	if oldEntry == nil {
		if err := f.store.InsertEntry(ctx, entry); err != nil {
			glog.Errorf("insert entry %s: %v", entry.FullPath, err)
			return fmt.Errorf("insert entry %s: %v", entry.FullPath, err)
		}
	} else {
		if o_excl {
			glog.V(3).Infof("EEXIST: entry %s already exists", entry.FullPath)
			return fmt.Errorf("EEXIST: entry %s already exists", entry.FullPath)
		}
		if err := f.UpdateEntry(ctx, oldEntry, entry); err != nil {
			glog.Errorf("update entry %s: %v", entry.FullPath, err)
			return fmt.Errorf("update entry %s: %v", entry.FullPath, err)
		}
	}

	f.maybeAddBucket(entry)
	f.NotifyUpdateEvent(oldEntry, entry, true)

	f.deleteChunksIfNotNew(oldEntry, entry)

	glog.V(4).Infof("CreateEntry %s: created", entry.FullPath)

	return nil
}

func (f *Filer) UpdateEntry(ctx context.Context, oldEntry, entry *Entry) (err error) {
	if oldEntry != nil {
		if oldEntry.IsDirectory() && !entry.IsDirectory() {
			glog.Errorf("existing %s is a directory", entry.FullPath)
			return fmt.Errorf("existing %s is a directory", entry.FullPath)
		}
		if !oldEntry.IsDirectory() && entry.IsDirectory() {
			glog.Errorf("existing %s is a file", entry.FullPath)
			return fmt.Errorf("existing %s is a file", entry.FullPath)
		}
	}
	return f.store.UpdateEntry(ctx, entry)
}

func (f *Filer) FindEntry(ctx context.Context, p FullPath) (entry *Entry, err error) {

	now := time.Now()

	if string(p) == "/" {
		return &Entry{
			FullPath: p,
			Attr: Attr{
				Mtime:  now,
				Crtime: now,
				Mode:   os.ModeDir | 0755,
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}, nil
	}
	return f.store.FindEntry(ctx, p)
}

func (f *Filer) ListDirectoryEntries(ctx context.Context, p FullPath, startFileName string, inclusive bool, limit int) ([]*Entry, error) {
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0 : len(p)-1]
	}
	return f.store.ListDirectoryEntries(ctx, p, startFileName, inclusive, limit)
}

func (f *Filer) cacheDelDirectory(dirpath string) {

	if dirpath == "/" {
		return
	}

	if f.directoryCache == nil {
		return
	}
	f.directoryCache.Delete(dirpath)
	return
}

func (f *Filer) cacheGetDirectory(dirpath string) *Entry {

	if f.directoryCache == nil {
		return nil
	}
	item := f.directoryCache.Get(dirpath)
	if item == nil {
		return nil
	}
	return item.Value().(*Entry)
}

func (f *Filer) cacheSetDirectory(dirpath string, dirEntry *Entry, level int) {

	if f.directoryCache == nil {
		return
	}

	minutes := 60
	if level < 10 {
		minutes -= level * 6
	}

	f.directoryCache.Set(dirpath, dirEntry, time.Duration(minutes)*time.Minute)
}
