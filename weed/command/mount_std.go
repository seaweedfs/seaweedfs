//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package command

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/types"

	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/filesys"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
)

func runMount(cmd *Command, args []string) bool {

	if *mountOptions.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *mountOptions.debugPort), nil)
	}

	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)
	if *mountReadRetryTime < time.Second {
		*mountReadRetryTime = time.Second
	}
	util.RetryWaitTime = *mountReadRetryTime

	umask, umaskErr := strconv.ParseUint(*mountOptions.umaskString, 8, 64)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mountOptions.umaskString)
		return false
	}

	if len(args) > 0 {
		return false
	}

	return RunMount(&mountOptions, os.FileMode(umask))
}

func getParentInode(mountDir string) (uint64, error) {
	parentDir := filepath.Clean(filepath.Join(mountDir, ".."))
	fi, err := os.Stat(parentDir)
	if err != nil {
		return 0, err
	}

	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, nil
	}

	return stat.Ino, nil
}

func RunMount(option *MountOptions, umask os.FileMode) bool {

	filerAddresses := pb.ServerAddresses(*option.filer).ToAddresses()

	util.LoadConfiguration("security", false)
	// try to connect to filer, filerBucketsPath may be useful later
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	var cipher bool
	var err error
	for i := 0; i < 10; i++ {
		err = pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer grpc address %v configuration: %v", filerAddresses, err)
			}
			cipher = resp.Cipher
			return nil
		})
		if err != nil {
			glog.V(0).Infof("failed to talk to filer %v: %v", filerAddresses, err)
			glog.V(0).Infof("wait for %d seconds ...", i+1)
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	if err != nil {
		glog.Errorf("failed to talk to filer %v: %v", filerAddresses, err)
		return true
	}

	filerMountRootPath := *option.filerMountRootPath
	dir := util.ResolvePath(*option.dir)
	parentInode, err := getParentInode(dir)
	if err != nil {
		glog.Errorf("failed to retrieve inode for parent directory of %s: %v", dir, err)
		return true
	}

	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.Version(), runtime.GOOS, runtime.GOARCH)
	if dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	fuse.Unmount(dir)

	// detect mount folder mode
	if *option.dirAutoCreate {
		os.MkdirAll(dir, os.FileMode(0777)&^umask)
	}
	fileInfo, err := os.Stat(dir)

	uid, gid := uint32(0), uint32(0)
	mountMode := os.ModeDir | 0777
	if err == nil {
		mountMode = os.ModeDir | os.FileMode(0777)&^umask
		uid, gid = util.GetFileUidGid(fileInfo)
		fmt.Printf("mount point owner uid=%d gid=%d mode=%s\n", uid, gid, mountMode)
	} else {
		fmt.Printf("can not stat %s\n", dir)
		return false
	}

	if uid == 0 {
		if u, err := user.Current(); err == nil {
			if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
				uid = uint32(parsedId)
			}
			if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
				gid = uint32(parsedId)
			}
			fmt.Printf("current uid=%d gid=%d\n", uid, gid)
		}
	}

	// mapping uid, gid
	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	// Ensure target mount point availability
	if isValid := checkMountPointAvailable(dir); !isValid {
		glog.Fatalf("Expected mount to still be active, target mount point: %s, please check!", dir)
		return true
	}

	mountName := path.Base(dir)

	options := []fuse.MountOption{
		fuse.VolumeName(mountName),
		fuse.FSName(*option.filer + ":" + filerMountRootPath),
		fuse.Subtype("seaweedfs"),
		// fuse.NoAppleDouble(), // include .DS_Store, otherwise can not delete non-empty folders
		fuse.NoAppleXattr(),
		fuse.ExclCreate(),
		fuse.DaemonTimeout("3600"),
		fuse.AllowDev(),
		fuse.AllowSUID(),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(1024 * 512),
		fuse.AsyncRead(),
		// fuse.WritebackCache(),
		// fuse.MaxBackground(1024),
		// fuse.CongestionThreshold(1024),
	}

	options = append(options, osSpecificMountOptions()...)
	if *option.allowOthers {
		options = append(options, fuse.AllowOther())
	}
	if *option.nonempty {
		options = append(options, fuse.AllowNonEmptyMount())
	}
	if *option.readOnly {
		options = append(options, fuse.ReadOnly())
	}

	// find mount point
	mountRoot := filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}

	diskType := types.ToDiskType(*option.diskType)

	seaweedFileSystem := filesys.NewSeaweedFileSystem(&filesys.Option{
		MountDirectory:     dir,
		FilerAddresses:     filerAddresses,
		GrpcDialOption:     grpcDialOption,
		FilerMountRootPath: mountRoot,
		Collection:         *option.collection,
		Replication:        *option.replication,
		TtlSec:             int32(*option.ttlSec),
		DiskType:           diskType,
		ChunkSizeLimit:     int64(chunkSizeLimitMB) * 1024 * 1024,
		ConcurrentWriters:  *option.concurrentWriters,
		CacheDir:           *option.cacheDir,
		CacheSizeMB:        *option.cacheSizeMB,
		DataCenter:         *option.dataCenter,
		MountUid:           uid,
		MountGid:           gid,
		MountMode:          mountMode,
		MountCtime:         fileInfo.ModTime(),
		MountMtime:         time.Now(),
		MountParentInode:   parentInode,
		Umask:              umask,
		VolumeServerAccess: *mountOptions.volumeServerAccess,
		Cipher:             cipher,
		UidGidMapper:       uidGidMapper,
	})

	// mount
	c, err := fuse.Mount(dir, options...)
	if err != nil {
		glog.V(0).Infof("mount: %v", err)
		return true
	}
	defer fuse.Unmount(dir)

	grace.OnInterrupt(func() {
		fuse.Unmount(dir)
		c.Close()
	})

	glog.V(0).Infof("mounted %s%s to %v", *option.filer, mountRoot, dir)
	server := fs.New(c, nil)
	seaweedFileSystem.Server = server
	seaweedFileSystem.StartBackgroundTasks()
	err = server.Serve(seaweedFileSystem)

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.V(0).Infof("mount process: %v", err)
		return true
	}

	return true
}
