package shell

import (
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"io"
)

type ShellOptions struct {
	Masters        *string
	GrpcDialOption grpc.DialOption
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
