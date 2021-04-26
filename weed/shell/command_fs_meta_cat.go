package shell

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"sort"

	"github.com/golang/protobuf/jsonpb"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaCat{})
}

type commandFsMetaCat struct {
}

func (c *commandFsMetaCat) Name() string {
	return "fs.meta.cat"
}

func (c *commandFsMetaCat) Help() string {
	return `print out the meta data content for a file or directory

	fs.meta.cat /dir/
	fs.meta.cat /dir/file_name
`
}

func (c *commandFsMetaCat) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	dir, name := util.FullPath(path).DirAndName()

	return commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}

		m := jsonpb.Marshaler{
			EmitDefaults: true,
			Indent:       "  ",
		}

		sort.Slice(respLookupEntry.Entry.Chunks, func(i, j int) bool {
			if respLookupEntry.Entry.Chunks[i].Offset == respLookupEntry.Entry.Chunks[j].Offset {
				return respLookupEntry.Entry.Chunks[i].Mtime < respLookupEntry.Entry.Chunks[j].Mtime
			}
			return respLookupEntry.Entry.Chunks[i].Offset < respLookupEntry.Entry.Chunks[j].Offset
		})

		text, marshalErr := m.MarshalToString(respLookupEntry.Entry)
		if marshalErr != nil {
			return fmt.Errorf("marshal meta: %v", marshalErr)
		}

		fmt.Fprintf(writer, "%s\n", text)

		bytes, _ := proto.Marshal(respLookupEntry.Entry)
		gzippedBytes, _ := util.GzipData(bytes)
		// zstdBytes, _ := util.ZstdData(bytes)
		// fmt.Fprintf(writer, "chunks %d meta size: %d gzip:%d zstd:%d\n", len(respLookupEntry.Entry.Chunks), len(bytes), len(gzippedBytes), len(zstdBytes))
		fmt.Fprintf(writer, "chunks %d meta size: %d gzip:%d\n", len(respLookupEntry.Entry.Chunks), len(bytes), len(gzippedBytes))

		return nil

	})

}
