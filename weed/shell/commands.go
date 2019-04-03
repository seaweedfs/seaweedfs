package shell

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

type ShellOptions struct {
	Masters        *string
	GrpcDialOption grpc.DialOption
	// shell transient context
	FilerHost string
	FilerPort int64
	Directory string
}

type commandEnv struct {
	env          map[string]string
	masterClient *wdclient.MasterClient
	option       ShellOptions
}

type command interface {
	Name() string
	Help() string
	Do([]string, *commandEnv, io.Writer) error
}

var (
	commands = []command{}
)

func (ce *commandEnv) parseUrl(input string) (filerServer string, filerPort int64, path string, err error) {
	if strings.HasPrefix(input, "http") {
		return parseFilerUrl(input)
	}
	if !strings.HasPrefix(input, "/") {
		input = filepath.ToSlash(filepath.Join(ce.option.Directory, input))
	}
	return ce.option.FilerHost, ce.option.FilerPort, input, err
}

func parseFilerUrl(entryPath string) (filerServer string, filerPort int64, path string, err error) {
	if strings.HasPrefix(entryPath, "http") {
		var u *url.URL
		u, err = url.Parse(entryPath)
		if err != nil {
			return
		}
		filerServer = u.Hostname()
		portString := u.Port()
		if portString != "" {
			filerPort, err = strconv.ParseInt(portString, 10, 32)
		}
		path = u.Path
	} else {
		err = fmt.Errorf("path should have full url http://<filer_server>:<port>/path/to/dirOrFile : %s", entryPath)
	}
	return
}
