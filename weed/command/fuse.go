package command

import (
	"fmt"
	"strings"
	"strconv"
	"time"
	"os"
)

func init() {
	cmdFuse.Run = runFuse // break init cycle
}

func runFuse(cmd *Command, args []string) bool {
	argsLen := len(args)
	options := []string{}

	// at least target mount path should be passed
	if argsLen < 1 {
		return false
	}

	// first option is always target mount path
	mountOptions.dir = &args[0]

	// scan parameters looking for one or more -o options
	// -o options receive parameters on format key=value[,key=value]...
	for i := 0; i < argsLen; i++ {
		if args[i] == "-o" && i+1 <= argsLen {
			options = strings.Split(args[i+1], ",")
			i++
		}
	}

	// for each option passed with -o
	for _, option := range options {
		// split just first = character
		parts := strings.SplitN(option, "=", 2)

		// if doesn't key and value skip
		if len(parts) != 2 {
			continue
		}

		// switch key keeping "weed mount" parameters
		switch parts[0] {
			case "filer":
				mountOptions.filer = &parts[1]
			case "filer.path":
				mountOptions.filerMountRootPath = &parts[1]
			case "dirAutoCreate":
				if value, err := strconv.ParseBool(parts[1]); err != nil {
					mountOptions.dirAutoCreate = &value
				} else {
					panic(fmt.Errorf("dirAutoCreate: %s", err))
				}
			case "collection":
				mountOptions.collection = &parts[1]
			case "replication":
				mountOptions.replication = &parts[1]
			case "disk":
				mountOptions.diskType = &parts[1]
			case "ttl":
				if value, err := strconv.ParseInt(parts[1], 0, 32); err != nil {
					intValue := int(value)
					mountOptions.ttlSec = &intValue
				} else {
					panic(fmt.Errorf("ttl: %s", err))
				}
			case "chunkSizeLimitMB":
				if value, err := strconv.ParseInt(parts[1], 0, 32); err != nil {
					intValue := int(value)
					mountOptions.chunkSizeLimitMB = &intValue
				} else {
					panic(fmt.Errorf("chunkSizeLimitMB: %s", err))
				}
			case "concurrentWriters":
				if value, err := strconv.ParseInt(parts[1], 0, 32); err != nil {
					intValue := int(value)
					mountOptions.concurrentWriters = &intValue
				} else {
					panic(fmt.Errorf("concurrentWriters: %s", err))
				}
			case "cacheDir":
				mountOptions.cacheDir = &parts[1]
			case "cacheCapacityMB":
				if value, err := strconv.ParseInt(parts[1], 0, 64); err != nil {
					mountOptions.cacheSizeMB = &value
				} else {
					panic(fmt.Errorf("cacheCapacityMB: %s", err))
				}
			case "dataCenter":
				mountOptions.dataCenter = &parts[1]
			case "allowOthers":
				if value, err := strconv.ParseBool(parts[1]); err != nil {
					mountOptions.allowOthers = &value
				} else {
					panic(fmt.Errorf("allowOthers: %s", err))
				}
			case "umask":
				mountOptions.umaskString = &parts[1]
			case "nonempty":
				if value, err := strconv.ParseBool(parts[1]); err != nil {
					mountOptions.nonempty = &value
				} else {
					panic(fmt.Errorf("nonempty: %s", err))
				}
			case "volumeServerAccess":
				mountOptions.volumeServerAccess = &parts[1]
			case "map.uid":
				mountOptions.uidMap = &parts[1]
			case "map.gid":
				mountOptions.gidMap = &parts[1]
			case "readOnly":
				if value, err := strconv.ParseBool(parts[1]); err != nil {
					mountOptions.readOnly = &value
				} else {
					panic(fmt.Errorf("readOnly: %s", err))
				}
			case "cpuprofile":
				mountCpuProfile = &parts[1]
			case "memprofile":
				mountMemProfile = &parts[1]
			case "readRetryTime":
				if value, err := time.ParseDuration(parts[1]); err != nil {
					mountReadRetryTime = &value
				} else {
					panic(fmt.Errorf("readRetryTime: %s", err))
				}
		}
	}

	// I don't know why PATH environment variable is lost
	if err := os.Setenv("PATH", "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"); err != nil {
		panic(fmt.Errorf("setenv: %s", err))
	}

	// just call "weed mount" command
	return runMount(cmdMount, []string{})
}

var cmdFuse = &Command{
	UsageLine: "fuse /mnt/mount/point -o \"filer=localhost:8888,filer.remote=/\"",
	Short: "Allow use weed with linux's mount command",
	Long: `Allow use weed with linux's mount command

  You can use -t weed on mount command:
  mv weed /sbin/mount.weed
  mount -t weed fuse /mnt -o "filer=localhost:8888,filer.remote=/"

  Or you can use -t fuse on mount command:
  mv weed /sbin/weed
  mount -t fuse.weed fuse /mnt -o "filer=localhost:8888,filer.remote=/"
  mount -t fuse "weed#fuse" /mnt -o "filer=localhost:8888,filer.remote=/"

  To use without mess with your /sbin:
  mount -t fuse./home/user/bin/weed fuse /mnt -o "filer=localhost:8888,filer.remote=/"
  mount -t fuse "/home/user/bin/weed#fuse" /mnt -o "filer=localhost:8888,filer.remote=/"

  To check valid options look "weed mount --help"
  `,
}
