package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"code.google.com/p/weed-fs/go/filer"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
	"fmt"
	"os"
	"os/signal"
	"runtime"
)

type MountOptions struct {
	filer *string
	dir   *string
}

var (
	mountOptions MountOptions
)

func init() {
	cmdMount.Run = runMount // break init cycle
	cmdMount.IsDebug = cmdMount.Flag.Bool("debug", false, "verbose debug information")
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "weedfs filer location")
	mountOptions.dir = cmdMount.Flag.String("dir", "", "mount weed filer to this directory")
}

var cmdMount = &Command{
	UsageLine: "mount -filer=localhost:8888 -dir=/some/dir",
	Short:     "mount weed filer to a directory as file system in userspace(FUSE)",
	Long: `mount weed file system to userspace.
  
  Pre-requisites:
  1) have a weed file system running
  2) have a "weed filer" running
  These 2 requirements can be achieved with one command "weed server -filer=true"

  This uses bazil.org/fuse, whichenables writing FUSE file systems on
  FreeBSD, Linux, and OS X.

  On OS X, it requires OSXFUSE (http://osxfuse.github.com/).

  `,
}

func runMount(cmd *Command, args []string) bool {
	fmt.Printf("This is Weed File System version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *mountOptions.dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	c, err := fuse.Mount(*mountOptions.dir)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			// sig is a ^C, handle it
			fuse.Unmount(*mountOptions.dir)
			c.Close()
			os.Exit(0)
		}
	}()

	err = fs.Serve(c, WFS{})
	if err != nil {
		fuse.Unmount(*mountOptions.dir)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.Fatal(err)
	}

	return true
}

type File struct {
	FileId filer.FileId
	Name   string
}

func (File) Attr() fuse.Attr {
	return fuse.Attr{Mode: 0444}
}
func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return []byte("hello, world\n"), nil
}

type Dir struct {
	DirectoryId filer.DirectoryId
	Name        string
}

func (dir Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: 1, Mode: os.ModeDir | 0555}
}

func (dir Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	files_result, e := filer.ListFiles(*mountOptions.filer, dir.DirectoryId, name)
	if e != nil {
		return nil, fuse.ENOENT
	}
	if len(files_result.Files) > 0 {
		return File{files_result.Files[0].Id, files_result.Files[0].Name}, nil
	}
	return nil, fmt.Errorf("File Not Found for %s", name)
}

type WFS struct{}

func (WFS) Root() (fs.Node, fuse.Error) {
	return Dir{}, nil
}

func (dir *Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	ret := make([]fuse.Dirent, 0)
	if dirs, e := filer.ListDirectories(*mountOptions.filer, dir.DirectoryId); e == nil {
		for _, d := range dirs.Directories {
			dirId := uint64(d.Id)
			ret = append(ret, fuse.Dirent{Inode: dirId, Name: d.Name, Type: fuse.DT_Dir})
		}
	}
	if files, e := filer.ListFiles(*mountOptions.filer, dir.DirectoryId, ""); e == nil {
		for _, f := range files.Files {
			if fileId, e := storage.ParseFileId(string(f.Id)); e == nil {
				fileInode := uint64(fileId.VolumeId)<<32 + fileId.Key
				ret = append(ret, fuse.Dirent{Inode: fileInode, Name: f.Name, Type: fuse.DT_File})
			}

		}
	}
	return ret, nil
}
