//go:build linux || darwin
// +build linux darwin

package command

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type parameter struct {
	name  string
	value string
}

func runFuse(cmd *Command, args []string) bool {
	rawArgs := strings.Join(args, " ")
	rawArgsLen := len(rawArgs)
	option := strings.Builder{}
	options := []parameter{}
	masterProcess := true
	fusermountPath := ""

	// first parameter
	i := 0
	for i = 0; i < rawArgsLen && rawArgs[i] != ' '; i++ {
		option.WriteByte(rawArgs[i])
	}
	options = append(options, parameter{"arg0", option.String()})
	option.Reset()

	for i++; i < rawArgsLen; i++ {

		// space separator check for filled option
		if rawArgs[i] == ' ' {
			if option.Len() > 0 {
				options = append(options, parameter{option.String(), "true"})
				option.Reset()
			}

			// dash separator read option until next space
		} else if rawArgs[i] == '-' {
			for i++; i < rawArgsLen && rawArgs[i] != ' '; i++ {
				option.WriteByte(rawArgs[i])
			}
			// ignore "-o"
			if option.String() != "o" {
				options = append(options, parameter{option.String(), "true"})
			}
			option.Reset()

			// equal separator start option with pending value
		} else if rawArgs[i] == '=' {
			name := option.String()
			option.Reset()

			for i++; i < rawArgsLen && rawArgs[i] != ',' && rawArgs[i] != ' '; i++ {
				// double quote separator read option until next double quote
				if rawArgs[i] == '"' {
					for i++; i < rawArgsLen && rawArgs[i] != '"'; i++ {
						option.WriteByte(rawArgs[i])
					}

					// single quote separator read option until next single quote
				} else if rawArgs[i] == '\'' {
					for i++; i < rawArgsLen && rawArgs[i] != '\''; i++ {
						option.WriteByte(rawArgs[i])
					}

					// add chars before comma
				} else if rawArgs[i] != ' ' {
					option.WriteByte(rawArgs[i])
				}
			}

			options = append(options, parameter{name, option.String()})
			option.Reset()

			// comma separator just read current option
		} else if rawArgs[i] == ',' {
			options = append(options, parameter{option.String(), "true"})
			option.Reset()

			// what is not a separator fill option buffer
		} else {
			option.WriteByte(rawArgs[i])
		}
	}

	// get residual option data
	if option.Len() > 0 {
		// add value to pending option
		options = append(options, parameter{option.String(), "true"})
		option.Reset()
	}

	// scan each parameter
	for i := 0; i < len(options); i++ {
		parameter := options[i]

		switch parameter.name {
		case "child":
			masterProcess = false
			if parsed, err := strconv.ParseInt(parameter.value, 10, 64); err == nil {
				if parsed > math.MaxInt || parsed <= 0 {
					panic(fmt.Errorf("parent PID %d is invalid", parsed))
				}
				mountOptions.fuseCommandPid = int(parsed)
			} else {
				panic(fmt.Errorf("parent PID %s is invalid: %w", parameter.value, err))
			}
		case "arg0":
			mountOptions.dir = &parameter.value
		case "filer":
			mountOptions.filer = &parameter.value
		case "filer.path":
			mountOptions.filerMountRootPath = &parameter.value
		case "dirAutoCreate":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.dirAutoCreate = &parsed
			} else {
				panic(fmt.Errorf("dirAutoCreate: %s", err))
			}
		case "collection":
			mountOptions.collection = &parameter.value
		case "replication":
			mountOptions.replication = &parameter.value
		case "disk":
			mountOptions.diskType = &parameter.value
		case "ttl":
			if parsed, err := strconv.ParseInt(parameter.value, 0, 32); err == nil {
				intValue := int(parsed)
				mountOptions.ttlSec = &intValue
			} else {
				panic(fmt.Errorf("ttl: %s", err))
			}
		case "chunkSizeLimitMB":
			if parsed, err := strconv.ParseInt(parameter.value, 0, 32); err == nil {
				intValue := int(parsed)
				mountOptions.chunkSizeLimitMB = &intValue
			} else {
				panic(fmt.Errorf("chunkSizeLimitMB: %s", err))
			}
		case "cacheMetaTtlSec":
			if parsed, err := strconv.ParseInt(parameter.value, 0, 32); err == nil {
				intValue := int(parsed)
				mountOptions.cacheMetaTtlSec = &intValue
			} else {
				panic(fmt.Errorf("cacheMetaTtlSec: %s", err))
			}
		case "concurrentWriters":
			i++
			if parsed, err := strconv.ParseInt(parameter.value, 0, 32); err == nil {
				intValue := int(parsed)
				mountOptions.concurrentWriters = &intValue
			} else {
				panic(fmt.Errorf("concurrentWriters: %s", err))
			}
		case "concurrentReaders":
			if parsed, err := strconv.ParseInt(parameter.value, 0, 32); err == nil {
				intValue := int(parsed)
				mountOptions.concurrentReaders = &intValue
			} else {
				panic(fmt.Errorf("concurrentReaders: %s", err))
			}
		case "cacheDir":
			mountOptions.cacheDirForRead = &parameter.value
		case "cacheCapacityMB":
			if parsed, err := strconv.ParseInt(parameter.value, 0, 64); err == nil {
				mountOptions.cacheSizeMBForRead = &parsed
			} else {
				panic(fmt.Errorf("cacheCapacityMB: %s", err))
			}
		case "cacheDirWrite":
			mountOptions.cacheDirForWrite = &parameter.value
		case "dataCenter":
			mountOptions.dataCenter = &parameter.value
		case "allowOthers":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.allowOthers = &parsed
			} else {
				panic(fmt.Errorf("allowOthers: %s", err))
			}
		case "umask":
			mountOptions.umaskString = &parameter.value
		case "nonempty":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.nonempty = &parsed
			} else {
				panic(fmt.Errorf("nonempty: %s", err))
			}
		case "volumeServerAccess":
			mountOptions.volumeServerAccess = &parameter.value
		case "map.uid":
			mountOptions.uidMap = &parameter.value
		case "map.gid":
			mountOptions.gidMap = &parameter.value
		case "readOnly":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.readOnly = &parsed
			} else {
				panic(fmt.Errorf("readOnly: %s", err))
			}
		case "disableXAttr":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {

				mountOptions.disableXAttr = &parsed
			} else {
				panic(fmt.Errorf("disableXAttr: %s", err))
			}
		case "cpuprofile":
			mountCpuProfile = &parameter.value
		case "memprofile":
			mountMemProfile = &parameter.value
		case "readRetryTime":
			if parsed, err := time.ParseDuration(parameter.value); err == nil {
				mountReadRetryTime = &parsed
			} else {
				panic(fmt.Errorf("readRetryTime: %s", err))
			}
		case "fusermount.path":
			fusermountPath = parameter.value
		case "config_dir":
			util.ConfigurationFileDirectory.Set(parameter.value)
		// FUSE performance options
		case "writebackCache":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.writebackCache = &parsed
			} else {
				fmt.Fprintf(os.Stderr, "failed to parse 'writebackCache' value %q: %v\n", parameter.value, err)
				return false
			}
		case "asyncDio":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.asyncDio = &parsed
			} else {
				fmt.Fprintf(os.Stderr, "failed to parse 'asyncDio' value %q: %v\n", parameter.value, err)
				return false
			}
		case "cacheSymlink":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.cacheSymlink = &parsed
			} else {
				fmt.Fprintf(os.Stderr, "failed to parse 'cacheSymlink' value %q: %v\n", parameter.value, err)
				return false
			}
		// macOS-specific FUSE options
		case "sys.novncache":
			if parsed, err := strconv.ParseBool(parameter.value); err == nil {
				mountOptions.novncache = &parsed
			} else {
				fmt.Fprintf(os.Stderr, "failed to parse 'sys.novncache' value %q: %v\n", parameter.value, err)
				return false
			}
		default:
			t := parameter.name
			if parameter.value != "true" {
				t = fmt.Sprintf("%s=%s", parameter.name, parameter.value)
			}
			mountOptions.extraOptions = append(mountOptions.extraOptions, t)
		}
	}

	util_http.InitGlobalHttpClient()

	// the master start the child, release it then finish himself
	if masterProcess {
		arg0, err := os.Executable()
		if err != nil {
			panic(err)
		}

		// pass our PID to the child process
		pid := os.Getpid()
		argv := append(os.Args, "-o", "child="+strconv.Itoa(pid))

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)

		attr := os.ProcAttr{}
		attr.Env = os.Environ()

		child, err := os.StartProcess(arg0, argv, &attr)

		if err != nil {
			panic(fmt.Errorf("master process can not start child process: %s", err))
		}

		err = child.Release()

		if err != nil {
			panic(fmt.Errorf("master process can not release child process: %s", err))
		}

		select {
		case <-c:
			return true
		}
	}

	if fusermountPath != "" {
		if err := os.Setenv("PATH", fusermountPath); err != nil {
			panic(fmt.Errorf("setenv: %s", err))
		}
	} else if os.Getenv("PATH") == "" {
		if err := os.Setenv("PATH", "/bin:/sbin:/usr/bin:/usr/sbin"); err != nil {
			panic(fmt.Errorf("setenv: %s", err))
		}
	}

	// just call "weed mount" command
	return runMount(cmdMount, []string{})
}
