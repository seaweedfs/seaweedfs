package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
	"net/url"
	"strconv"
	"strings"
)

func init() {
	commands = append(commands, &commandFsDu{})
}

type commandFsDu struct {
}

func (c *commandFsDu) Name() string {
	return "fs.du"
}

func (c *commandFsDu) Help() string {
	return `show disk usage

	fs.du http://<filer_server>:<port>/dir
	fs.du http://<filer_server>:<port>/dir/file_name
	fs.du http://<filer_server>:<port>/dir/file_prefix
`
}

func (c *commandFsDu) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, path, err := parseFilerUrl(args[0])
	if err != nil {
		return err
	}

	dir, name := filer2.FullPath(path).DirAndName()
	if strings.HasSuffix(path, "/") {
		if path == "/"{
			dir, name = "/", ""
		}else{
			dir, name = path[0 : len(path)-1], ""
		}
	}

	ctx := context.Background()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		_, _, err = paginateDirectory(ctx, writer, client, dir, name, 1000)

		return err

	})

}

func paginateDirectory(ctx context.Context, writer io.Writer, client filer_pb.SeaweedFilerClient, dir, name string, paginateSize int) (blockCount uint64, byteCount uint64, err error) {

	paginatedCount := -1
	startFromFileName := ""

	for paginatedCount == -1 || paginatedCount == paginateSize {
		resp, listErr := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             name,
			StartFromFileName:  startFromFileName,
			InclusiveStartFrom: false,
			Limit:              uint32(paginateSize),
		})
		if listErr != nil {
			err = listErr
			return
		}

		paginatedCount = len(resp.Entries)

		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				subDir := fmt.Sprintf("%s/%s", dir, entry.Name)
				if dir == "/" {
					subDir = "/" + entry.Name
				}
				numBlock, numByte, err := paginateDirectory(ctx, writer, client, subDir, "", paginateSize)
				if err == nil {
					blockCount += numBlock
					byteCount += numByte
				}
			} else {
				blockCount += uint64(len(entry.Chunks))
				byteCount += filer2.TotalSize(entry.Chunks)
			}
			startFromFileName = entry.Name

			if name != "" && !entry.IsDirectory {
				fmt.Fprintf(writer, "block:%4d\tbyte:%10d\t%s/%s\n", blockCount, byteCount, dir, name)
			}
		}
	}

	if name == "" {
		fmt.Fprintf(writer, "block:%4d\tbyte:%10d\t%s\n", blockCount, byteCount, dir)
	}

	return

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

func (env *commandEnv) withFilerClient(ctx context.Context, filerServer string, filerPort int64, fn func(filer_pb.SeaweedFilerClient) error) error {

	filerGrpcAddress := fmt.Sprintf("%s:%d", filerServer, filerPort+10000)
	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerGrpcAddress, env.option.GrpcDialOption)

}
