package shell

import (
	"context"
	"fmt"
	"github.com/joeslay/seaweedfs/weed/filer2"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"io"
	"strings"
)

func init() {
	Commands = append(Commands, &commandFsTree{})
}

type commandFsTree struct {
}

func (c *commandFsTree) Name() string {
	return "fs.tree"
}

func (c *commandFsTree) Help() string {
	return `recursively list all files under a directory

	fs.tree http://<filer_server>:<port>/dir/
`
}

func (c *commandFsTree) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	filerServer, filerPort, path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	dir, name := filer2.FullPath(path).DirAndName()

	ctx := context.Background()

	return commandEnv.withFilerClient(ctx, filerServer, filerPort, func(client filer_pb.SeaweedFilerClient) error {

		dirCount, fCount, terr := treeTraverseDirectory(ctx, writer, client, dir, name, newPrefix(), -1)

		if terr == nil {
			fmt.Fprintf(writer, "%d directories, %d files\n", dirCount, fCount)
		}

		return terr

	})

}
func treeTraverseDirectory(ctx context.Context, writer io.Writer, client filer_pb.SeaweedFilerClient, dir, name string, prefix *Prefix, level int) (directoryCount, fileCount int64, err error) {

	paginatedCount := -1
	startFromFileName := ""
	paginateSize := 1000

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
		if paginatedCount > 0 {
			prefix.addMarker(level)
		}

		for i, entry := range resp.Entries {

			if level < 0 && name != "" {
				if entry.Name != name {
					break
				}
			}

			// 0.1% wrong prefix here, but fixing it would need to paginate to the next batch first
			isLast := paginatedCount < paginateSize && i == paginatedCount-1
			fmt.Fprintf(writer, "%s%s\n", prefix.getPrefix(level, isLast), entry.Name)

			if entry.IsDirectory {
				directoryCount++
				subDir := fmt.Sprintf("%s/%s", dir, entry.Name)
				if dir == "/" {
					subDir = "/" + entry.Name
				}
				dirCount, fCount, terr := treeTraverseDirectory(ctx, writer, client, subDir, "", prefix, level+1)
				directoryCount += dirCount
				fileCount += fCount
				err = terr
			} else {
				fileCount++
			}
			startFromFileName = entry.Name

		}
	}

	return

}

type Prefix struct {
	markers map[int]bool
}

func newPrefix() *Prefix {
	return &Prefix{
		markers: make(map[int]bool),
	}
}
func (p *Prefix) addMarker(marker int) {
	p.markers[marker] = true
}
func (p *Prefix) removeMarker(marker int) {
	delete(p.markers, marker)
}
func (p *Prefix) getPrefix(level int, isLastChild bool) string {
	var sb strings.Builder
	if level < 0 {
		return ""
	}
	for i := 0; i < level; i++ {
		if _, ok := p.markers[i]; ok {
			sb.WriteString("│")
		} else {
			sb.WriteString(" ")
		}
		sb.WriteString("   ")
	}
	if isLastChild {
		sb.WriteString("└──")
		p.removeMarker(level)
	} else {
		sb.WriteString("├──")
	}
	return sb.String()
}
