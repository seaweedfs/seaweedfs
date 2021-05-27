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

		key, value := parts[0], parts[1]

		// switch key keeping "weed mount" parameters
		switch key {
			case "filer":
				mountOptions.filer = &value
			case "filer.path":
				mountOptions.filerMountRootPath = &value
			case "dirAutoCreate":
				if parsed, err := strconv.ParseBool(value); err != nil {
					mountOptions.dirAutoCreate = &parsed
				} else {
					panic(fmt.Errorf("dirAutoCreate: %s", err))
				}
			case "collection":
				mountOptions.collection = &value
			case "replication":
				mountOptions.replication = &value
			case "disk":
				mountOptions.diskType = &value
			case "ttl":
				if parsed, err := strconv.ParseInt(value, 0, 32); err != nil {
					intValue := int(parsed)
					mountOptions.ttlSec = &intValue
				} else {
					panic(fmt.Errorf("ttl: %s", err))
				}
			case "chunkSizeLimitMB":
				if parsed, err := strconv.ParseInt(value, 0, 32); err != nil {
					intValue := int(parsed)
					mountOptions.chunkSizeLimitMB = &intValue
				} else {
					panic(fmt.Errorf("chunkSizeLimitMB: %s", err))
				}
			case "concurrentWriters":
				if parsed, err := strconv.ParseInt(value, 0, 32); err != nil {
					intValue := int(parsed)
					mountOptions.concurrentWriters = &intValue
				} else {
					panic(fmt.Errorf("concurrentWriters: %s", err))
				}
			case "cacheDir":
				mountOptions.cacheDir = &value
			case "cacheCapacityMB":
				if parsed, err := strconv.ParseInt(value, 0, 64); err != nil {
					mountOptions.cacheSizeMB = &parsed
				} else {
					panic(fmt.Errorf("cacheCapacityMB: %s", err))
				}
			case "dataCenter":
				mountOptions.dataCenter = &value
			case "allowOthers":
				if parsed, err := strconv.ParseBool(value); err != nil {
					mountOptions.allowOthers = &parsed
				} else {
					panic(fmt.Errorf("allowOthers: %s", err))
				}
			case "umask":
				mountOptions.umaskString = &value
			case "nonempty":
				if parsed, err := strconv.ParseBool(value); err != nil {
					mountOptions.nonempty = &parsed
				} else {
					panic(fmt.Errorf("nonempty: %s", err))
				}
			case "volumeServerAccess":
				mountOptions.volumeServerAccess = &value
			case "map.uid":
				mountOptions.uidMap = &value
			case "map.gid":
				mountOptions.gidMap = &value
			case "readOnly":
				if parsed, err := strconv.ParseBool(value); err != nil {
					mountOptions.readOnly = &parsed
				} else {
					panic(fmt.Errorf("readOnly: %s", err))
				}
			case "cpuprofile":
				mountCpuProfile = &value
			case "memprofile":
				mountMemProfile = &value
			case "readRetryTime":
				if parsed, err := time.ParseDuration(value); err != nil {
					mountReadRetryTime = &parsed
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
	UsageLine: "fuse /mnt/mount/point -o \"filer=localhost:8888,filer.path=/\"",
	Short: "Allow use weed with linux's mount command",
	Long: `Allow use weed with linux's mount command

  You can use -t weed on mount command:
  mv weed /sbin/mount.weed
  mount -t weed fuse /mnt -o "filer=localhost:8888,filer.path=/"

  Or you can use -t fuse on mount command:
  mv weed /sbin/weed
  mount -t fuse.weed fuse /mnt -o "filer=localhost:8888,filer.path=/"
  mount -t fuse "weed#fuse" /mnt -o "filer=localhost:8888,filer.path=/"

  To use without mess with your /sbin:
  mount -t fuse./home/user/bin/weed fuse /mnt -o "filer=localhost:8888,filer.path=/"
  mount -t fuse "/home/user/bin/weed#fuse" /mnt -o "filer=localhost:8888,filer.path=/"

  To check valid options look "weed mount --help"
  `,
}
