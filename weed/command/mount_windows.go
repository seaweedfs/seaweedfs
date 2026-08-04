package command

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/mount/winfsp"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// ownedByMounter reports the mount root as belonging to whoever started it,
// matching the uid=-1 option handed to WinFsp. It is a display value only:
// WinFsp substitutes the calling user, and it must never be persisted, since
// every other client would read the entry as owned by uid 4294967295.
const ownedByMounter = ^uint32(0)

// windowsAttrTimeoutSec bounds how long WinFsp may serve cached attributes.
// Nothing invalidates that cache from this side, so it stays short rather than
// borrowing an unrelated flag's value.
const windowsAttrTimeoutSec = 1.0

func RunMount(option *MountOptions, umask os.FileMode) bool {
	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.\n")
		return false
	}

	dir := *option.dir
	if dir == "" {
		fmt.Printf("Please specify the mount point via \"-dir\", for example -dir=S:\n")
		return false
	}
	// A drive letter or a not-yet-existing directory is what WinFsp wants, so
	// the mount point deliberately goes through none of the unix preparation:
	// no ResolvePath, no auto-create, no stat.
	if err := checkWindowsMountPoint(dir); err != nil {
		fmt.Printf("%v\n", err)
		return false
	}

	filerAddresses, grpcDialOption, cipher, bucketRootPath, ok := connectToFiler(option)
	if !ok {
		return true
	}

	if *option.localSocket == "" {
		mountDirHash := util.HashToInt32([]byte(dir))
		if mountDirHash < 0 {
			mountDirHash = -mountDirHash
		}
		*option.localSocket = filepath.Join(os.TempDir(), fmt.Sprintf("seaweedfs-mount-%d.sock", mountDirHash))
	}
	if err := os.Remove(*option.localSocket); err != nil && !os.IsNotExist(err) {
		glog.Fatalf("Failed to remove %s, error: %s", *option.localSocket, err.Error())
	}
	mountSocketListener, err := net.Listen("unix", *option.localSocket)
	if err != nil {
		glog.Fatalf("Failed to listen on %s: %v", *option.localSocket, err)
	}

	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	mountRoot := resolveMountRoot(*option.filerMountRootPath)
	cacheDirForRead, cacheDirForWrite := resolveCacheDirs(option)

	seaweedFileSystem := buildSeaweedFileSystem(option, fileSystemParams{
		dir:              dir,
		mountRoot:        mountRoot,
		filerAddresses:   filerAddresses,
		grpcDialOption:   grpcDialOption,
		cipher:           cipher,
		uidGidMapper:     uidGidMapper,
		uid:              uint32(*option.windowsUid),
		gid:              uint32(*option.windowsGid),
		mountMode:        os.ModeDir | 0777,
		mountCtime:       time.Now(),
		umask:            umask,
		chunkSizeLimitMB: chunkSizeLimitMB,
		cacheDirForRead:  cacheDirForRead,
		cacheDirForWrite: cacheDirForWrite,
	})

	if !createMountRoot(seaweedFileSystem, mountRoot, bucketRootPath, filerAddresses) {
		return false
	}

	host := winfsp.New(seaweedFileSystem, winfsp.Options{
		VolumeName:   strings.ReplaceAll(*option.filer, ",", "+"),
		Uid:          ownedByMounter,
		Gid:          ownedByMounter,
		AttrTimeout:  windowsAttrTimeoutSec,
		ReadOnly:     *option.readOnly,
		Debug:        *option.debugFuse,
		ExtraOptions: option.extraOptions,
	})

	grace.OnInterrupt(func() {
		// The signal handler exits the process as soon as the hooks return, so
		// anything still queued has to be flushed here rather than after Serve.
		// WaitForAsyncFlush is idempotent, so the post-Serve call below is
		// harmless if both run.
		host.Unmount()
		seaweedFileSystem.WaitForAsyncFlush()
	})

	serveMountGrpc(seaweedFileSystem, mountSocketListener)

	if err := seaweedFileSystem.StartBackgroundTasks(); err != nil {
		fmt.Printf("failed to start background tasks: %v\n", err)
		return false
	}

	glog.V(0).Infof("mounting %s%s to %v", *option.filer, mountRoot, dir)
	glog.V(0).Infof("This is SeaweedFS version %s %s %s", version.Version(), runtime.GOOS, runtime.GOARCH)
	glog.V(0).Infof("Windows mount is beta: hard links are unavailable and byte-range locks are not shared across mounts")

	if err := host.Serve(windowsMountPoint(dir)); err != nil {
		glog.Errorf("%v", err)
		return false
	}

	seaweedFileSystem.WaitForAsyncFlush()
	seaweedFileSystem.ClearCacheDir()

	return true
}

// checkWindowsMountPoint rejects the mount points WinFsp cannot take, which is
// worth doing up front because its own failure is a bare false.
func checkWindowsMountPoint(dir string) error {
	if isDriveLetter(dir) {
		if _, err := os.Stat(strings.TrimRight(dir, `\/`) + `\`); err == nil {
			return fmt.Errorf("drive %s is already in use", dir)
		}
		return nil
	}
	if strings.HasPrefix(dir, `\\`) {
		return nil
	}

	// WinFsp creates the directory itself, with FILE_CREATE, and deletes it
	// again when the filesystem goes away. An existing one — empty or not —
	// fails with "mount point in use".
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("mount point %s already exists; WinFsp creates the directory itself, so give it a path that does not exist yet", dir)
	} else if !os.IsNotExist(err) {
		return err
	}
	if _, err := os.Stat(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("parent of mount point %s does not exist", dir)
	}
	return nil
}

// windowsMountPoint drops a trailing separator from a drive letter. WinFsp
// recognises a drive only as exactly two characters; "S:\\" falls through to
// its directory handling and the mount fails.
func windowsMountPoint(dir string) string {
	if isDriveLetter(dir) {
		return strings.TrimRight(dir, `\/`)
	}
	return dir
}

// isDriveLetter accepts both "S:" and "S:\\"; the trailing separator is how
// the drive is usually written, and windowsMountPoint normalises it.
func isDriveLetter(dir string) bool {
	trimmed := strings.TrimRight(dir, `\/`)
	if len(trimmed) != 2 || trimmed[1] != ':' {
		return false
	}
	c := trimmed[0]
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}
