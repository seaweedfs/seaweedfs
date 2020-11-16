package shell

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	locationPrefix := fsConfigureCommand.String("locationPrefix", "", "path prefix, required to update the path-specific configuration")
	collection := fsConfigureCommand.String("collection", "", "assign writes to this colletion")
	replication := fsConfigureCommand.String("replication", "", "assign writes with this replication")
	ttl := fsConfigureCommand.String("ttl", "", "assign writes with this ttl")
	fsync := fsConfigureCommand.Bool("fsync", false, "fsync for the writes")
	isDelete := fsConfigureCommand.Bool("delete", false, "delete the configuration by locationPrefix")
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
		if *isDelete {
			fc.DeleteLocationConf(*locationPrefix)
		} else {
			fc.AddLocationConf(locConf)
		}
	}

	buf.Reset()
	fc.ToText(&buf)

	fmt.Fprintf(writer, string(buf.Bytes()))

	if *apply {

		target := fmt.Sprintf("http://%s:%d%s/%s", commandEnv.option.FilerHost, commandEnv.option.FilerPort, filer.DirectoryEtc, filer.FilerConfName)

		// set the HTTP method, url, and request body
		req, err := http.NewRequest(http.MethodPut, target, &buf)
		if err != nil {
			return err
		}

		// set the request header Content-Type for json
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		util.CloseResponse(resp)

	}

	return nil

}
