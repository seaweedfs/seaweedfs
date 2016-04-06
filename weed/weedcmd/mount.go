package weedcmd

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
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "weed filer location")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "mount weed filer to this directory")
}

var cmdMount = &Command{
	UsageLine: "mount -filer=localhost:8888 -dir=/some/dir",
	Short:     "mount weed filer to a directory as file system in userspace(FUSE)",
	Long: `mount weed filer to userspace.

  Pre-requisites:
  1) have SeaweedFS master and volume servers running
  2) have a "weed filer" running
  These 2 requirements can be achieved with one command "weed server -filer=true"

  This uses bazil.org/fuse, whichenables writing FUSE file systems on
  Linux, and OS X.

  On OS X, it requires OSXFUSE (http://osxfuse.github.com/).

  `,
}
