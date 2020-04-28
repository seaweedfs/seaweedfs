// +build linux darwin freebsd

package command

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filesys"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func runMount(cmd *Command, args []string) bool {

	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)

	umask, umaskErr := strconv.ParseUint(*mountOptions.umaskString, 8, 64)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mountOptions.umaskString)
		return false
	}

	return RunMount(&mountOptions, os.FileMode(umask))
}

func RunMount(option *MountOptions, umask os.FileMode) bool {

	filer := *option.filer
	// parse filer grpc address
	filerGrpcAddress, err := pb.ParseFilerGrpcAddress(filer)
	if err != nil {
		glog.V(0).Infof("ParseFilerGrpcAddress: %v", err)
		return true
	}

	// try to connect to filer, filerBucketsPath may be useful later
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	var cipher bool
	err = pb.WithGrpcFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer grpc address %s configuration: %v", filerGrpcAddress, err)
		}
		cipher = resp.Cipher
		return nil
	})
	if err != nil {
		glog.Infof("failed to talk to filer %s: %v", filerGrpcAddress, err)
		return true
	}

	filerMountRootPath := *option.filerMountRootPath
	dir := *option.dir
	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB

	util.LoadConfiguration("security", false)

	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	fuse.Unmount(dir)

	uid, gid := uint32(0), uint32(0)

	// detect mount folder mode
	mountMode := os.ModeDir | 0755
	fileInfo, err := os.Stat(dir)
	if err == nil {
		mountMode = os.ModeDir | fileInfo.Mode()
		uid, gid = util.GetFileUidGid(fileInfo)
		fmt.Printf("mount point owner uid=%d gid=%d mode=%s\n", uid, gid, fileInfo.Mode())
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

	// Ensure target mount point availability
	if isValid := checkMountPointAvailable(dir); !isValid {
		glog.Fatalf("Expected mount to still be active, target mount point: %s, please check!", dir)
		return true
	}

	mountName := path.Base(dir)

	options := []fuse.MountOption{
		fuse.VolumeName(mountName),
		fuse.FSName(filer + ":" + filerMountRootPath),
		fuse.Subtype("seaweedfs"),
		// fuse.NoAppleDouble(), // include .DS_Store, otherwise can not delete non-empty folders
		fuse.NoAppleXattr(),
		fuse.NoBrowse(),
		fuse.AutoXattr(),
		fuse.ExclCreate(),
		fuse.DaemonTimeout("3600"),
		fuse.AllowSUID(),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(1024 * 128),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
	}

	options = append(options, osSpecificMountOptions()...)
	if *option.allowOthers {
		options = append(options, fuse.AllowOther())
	}
	if *option.nonempty {
		options = append(options, fuse.AllowNonEmptyMount())
	}

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

	// find mount point
	mountRoot := filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}

	err = fs.Serve(c, filesys.NewSeaweedFileSystem(&filesys.Option{
		FilerGrpcAddress:            filerGrpcAddress,
		GrpcDialOption:              grpcDialOption,
		FilerMountRootPath:          mountRoot,
		Collection:                  *option.collection,
		Replication:                 *option.replication,
		TtlSec:                      int32(*option.ttlSec),
		ChunkSizeLimit:              int64(chunkSizeLimitMB) * 1024 * 1024,
		CacheDir:                    *option.cacheDir,
		CacheSizeMB:                 *option.cacheSizeMB,
		DataCenter:                  *option.dataCenter,
		DirListCacheLimit:           *option.dirListCacheLimit,
		EntryCacheTtl:               3 * time.Second,
		MountUid:                    uid,
		MountGid:                    gid,
		MountMode:                   mountMode,
		MountCtime:                  fileInfo.ModTime(),
		MountMtime:                  time.Now(),
		Umask:                       umask,
		OutsideContainerClusterMode: *mountOptions.outsideContainerClusterMode,
		AsyncMetaDataCaching:        *mountOptions.asyncMetaDataCaching,
		Cipher:                      cipher,
	}))

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.V(0).Infof("mount process: %v", err)
		return true
	}

	return true
}
