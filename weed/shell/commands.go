package shell

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/HZ89/seaweedfs/weed/filer2"
	"github.com/HZ89/seaweedfs/weed/pb/filer_pb"
	"github.com/HZ89/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
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

func (ce *commandEnv) isDirectory(ctx context.Context, filerServer string, filerPort int64, path string) bool {

	return ce.checkDirectory(ctx, filerServer, filerPort, path) == nil

}

func (ce *commandEnv) checkDirectory(ctx context.Context, filerServer string, filerPort int64, path string) error {

	dir, name := filer2.FullPath(path).DirAndName()

	return ce.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		resp, listErr := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             name,
			StartFromFileName:  name,
			InclusiveStartFrom: true,
			Limit:              1,
		})
		if listErr != nil {
			return listErr
		}

		if len(resp.Entries) == 0 {
			return fmt.Errorf("entry not found")
		}

		if resp.Entries[0].Name != name {
			return fmt.Errorf("not a valid directory, found %s", resp.Entries[0].Name)
		}

		if !resp.Entries[0].IsDirectory {
			return fmt.Errorf("not a directory")
		}

		return nil
	})

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
