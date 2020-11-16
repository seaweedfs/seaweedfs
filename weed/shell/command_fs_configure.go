package shell

import (
	"bytes"
	"flag"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandFsConfigure{})
}

type commandFsConfigure struct {
}

func (c *commandFsConfigure) Name() string {
	return "fs.configure"
}

func (c *commandFsConfigure) Help() string {
	return `configure and apply storage options for each location

	fs.configure -locationPrfix=/my/folder -

`
}

func (c *commandFsConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	locationPrefix := fsConfigureCommand.String("locationPrefix", "", "path prefix")
	collection := fsConfigureCommand.String("collection", "", "assign writes to this colletion")
	replication := fsConfigureCommand.String("replication", "", "assign writes with this replication")
	ttl := fsConfigureCommand.String("ttl", "", "assign writes with this ttl")
	fsync := fsConfigureCommand.Bool("fsync", false, "fsync for the writes")
	apply := fsConfigureCommand.Bool("apply", false, "update and apply filer configuration")
	if err = fsConfigureCommand.Parse(args); err != nil {
		return nil
	}

	var buf bytes.Buffer
	if err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: filer.DirectoryEtc,
			Name:      filer.FilerConfName,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}

		return filer.StreamContent(commandEnv.MasterClient, &buf, respLookupEntry.Entry.Chunks, 0, math.MaxInt64)

	}); err != nil {
		return err
	}

	fc := filer.NewFilerConf()
	if err = fc.LoadFromBytes(buf.Bytes()); err != nil {
		return err
	}

	if *locationPrefix != "" {
		locConf := &filer_pb.FilerConf_PathConf{
			LocationPrefix: *locationPrefix,
			Collection:     *collection,
			Replication:    *replication,
			Ttl:            *ttl,
			Fsync:          *fsync,
		}
		fc.AddLocationConf(locConf)
	}

	fc.ToText(writer)

	if *apply {

	}

	return nil

}
