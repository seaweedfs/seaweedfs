package shell

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"github.com/chrislusf/seaweedfs/weed/wdclient/exclusive_locks"
)

type ShellOptions struct {
	Masters        *string
	GrpcDialOption grpc.DialOption
	// shell transient context
	FilerHost    string
	FilerPort    int64
	FilerAddress pb.ServerAddress
	Directory    string
}

type CommandEnv struct {
	env          map[string]string
	MasterClient *wdclient.MasterClient
	option       *ShellOptions
	locker       *exclusive_locks.ExclusiveLocker
}

type command interface {
	Name() string
	Help() string
	Do([]string, *CommandEnv, io.Writer) error
}

var (
	Commands = []command{}
)

func NewCommandEnv(options *ShellOptions) *CommandEnv {
	ce := &CommandEnv{
		env:          make(map[string]string),
		MasterClient: wdclient.NewMasterClient(options.GrpcDialOption, pb.AdminShellClient, "", "", pb.ServerAddresses(*options.Masters).ToAddresses()),
		option:       options,
	}
	ce.locker = exclusive_locks.NewExclusiveLocker(ce.MasterClient, "admin")
	return ce
}

func (ce *CommandEnv) parseUrl(input string) (path string, err error) {
	if strings.HasPrefix(input, "http") {
		err = fmt.Errorf("http://<filer>:<port> prefix is not supported any more")
		return
	}
	if !strings.HasPrefix(input, "/") {
		input = util.Join(ce.option.Directory, input)
	}
	return input, err
}

func (ce *CommandEnv) isDirectory(path string) bool {

	return ce.checkDirectory(path) == nil

}

func (ce *CommandEnv) confirmIsLocked(args []string) error {

	if ce.locker.IsLocking() {
		return nil
	}
	ce.locker.SetMessage(fmt.Sprintf("%v", args))

	return fmt.Errorf("need to run \"lock\" first to continue")

}

func (ce *CommandEnv) checkDirectory(path string) error {

	dir, name := util.FullPath(path).DirAndName()

	exists, err := filer_pb.Exists(ce, dir, name, true)

	if !exists {
		return fmt.Errorf("%s is not a directory", path)
	}

	return err

}

var _ = filer_pb.FilerClient(&CommandEnv{})

func (ce *CommandEnv) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcFilerClient(streamingMode, ce.option.FilerAddress, ce.option.GrpcDialOption, fn)

}

func (ce *CommandEnv) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
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
		err = fmt.Errorf("path should have full url /path/to/dirOrFile : %s", entryPath)
	}
	return
}

func findInputDirectory(args []string) (input string) {
	input = "."
	if len(args) > 0 {
		input = args[len(args)-1]
		if strings.HasPrefix(input, "-") {
			input = "."
		}
	}
	return input
}
