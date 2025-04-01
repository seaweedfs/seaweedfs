package shell

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
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

	# see the current configuration file content
	fs.configure

	# trying the changes and see the possible configuration file content
	fs.configure -locationPrefix=/my/folder -collection=abc
	fs.configure -locationPrefix=/my/folder -collection=abc -ttl=7d

	# example: configure adding only 1 physical volume for each bucket collection
	fs.configure -locationPrefix=/buckets/ -volumeGrowthCount=1

	# apply the changes
	fs.configure -locationPrefix=/my/folder -collection=abc -apply

	# delete the changes
	fs.configure -locationPrefix=/my/folder -delete -apply

`
}

func (c *commandFsConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	locationPrefix := fsConfigureCommand.String("locationPrefix", "", "path prefix, required to update the path-specific configuration")
	collection := fsConfigureCommand.String("collection", "", "assign writes to this collection")
	replication := fsConfigureCommand.String("replication", "", "assign writes with this replication")
	ttl := fsConfigureCommand.String("ttl", "", "assign writes with this ttl (e.g., 1m, 1h, 1d, 1w, 1y)")
	diskType := fsConfigureCommand.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	fsync := fsConfigureCommand.Bool("fsync", false, "fsync for the writes")
	isReadOnly := fsConfigureCommand.Bool("readOnly", false, "disable writes")
	worm := fsConfigureCommand.Bool("worm", false, "write-once-read-many, written files are readonly")
	wormGracePeriod := fsConfigureCommand.Uint64("wormGracePeriod", 0, "grace period before worm is enforced, in seconds")
	wormRetentionTime := fsConfigureCommand.Uint64("wormRetentionTime", 0, "retention time for a worm enforced file, in seconds")
	maxFileNameLength := fsConfigureCommand.Uint("maxFileNameLength", 0, "file name length limits in bytes for compatibility with Unix-based systems")
	dataCenter := fsConfigureCommand.String("dataCenter", "", "assign writes to this dataCenter")
	rack := fsConfigureCommand.String("rack", "", "assign writes to this rack")
	dataNode := fsConfigureCommand.String("dataNode", "", "assign writes to this dataNode")
	volumeGrowthCount := fsConfigureCommand.Int("volumeGrowthCount", 0, "the number of physical volumes to add if no writable volumes")
	isDelete := fsConfigureCommand.Bool("delete", false, "delete the configuration by locationPrefix")
	apply := fsConfigureCommand.Bool("apply", false, "update and apply filer configuration")
	if err = fsConfigureCommand.Parse(args); err != nil {
		return nil
	}

	fc, err := filer.ReadFilerConf(commandEnv.option.FilerAddress, commandEnv.option.GrpcDialOption, commandEnv.MasterClient)
	if err != nil {
		return err
	}

	if *locationPrefix != "" {
		infoAboutSimulationMode(writer, *apply, "-apply")
		locConf := &filer_pb.FilerConf_PathConf{
			LocationPrefix:           *locationPrefix,
			Collection:               *collection,
			Replication:              *replication,
			Ttl:                      *ttl,
			Fsync:                    *fsync,
			MaxFileNameLength:        uint32(*maxFileNameLength),
			DiskType:                 *diskType,
			VolumeGrowthCount:        uint32(*volumeGrowthCount),
			ReadOnly:                 *isReadOnly,
			DataCenter:               *dataCenter,
			Rack:                     *rack,
			DataNode:                 *dataNode,
			Worm:                     *worm,
			WormGracePeriodSeconds:   *wormGracePeriod,
			WormRetentionTimeSeconds: *wormRetentionTime,
		}

		// check collection
		if *collection != "" && strings.HasPrefix(*locationPrefix, "/buckets/") {
			return fmt.Errorf("one s3 bucket goes to one collection and not customizable")
		}

		// check replication
		if *replication != "" {
			rp, err := super_block.NewReplicaPlacementFromString(*replication)
			if err != nil {
				return fmt.Errorf("parse replication %s: %v", *replication, err)
			}
			if *volumeGrowthCount%rp.GetCopyCount() != 0 {
				return fmt.Errorf("volumeGrowthCount %d should be divided by replication copy count %d", *volumeGrowthCount, rp.GetCopyCount())
			}
		}

		// check ttl
		if *ttl != "" {
			regex := "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)[mhdwMy]$"
			match, _ := regexp.MatchString(regex, *ttl)

			if !match {
				return fmt.Errorf("ttl should be of the following format [1 to 255][unit] (e.g., 5m, 2h, 180d, 1w, 2y)")
			}
		}

		// save it
		if *isDelete {
			fc.DeleteLocationConf(*locationPrefix)
		} else {
			fc.AddLocationConf(locConf)
		}
	}

	var buf2 bytes.Buffer
	fc.ToText(&buf2)

	fmt.Fprint(writer, buf2.String())
	fmt.Fprintln(writer)

	if *apply {

		if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf2.Bytes())
		}); err != nil && err != filer_pb.ErrNotFound {
			return err
		}

	}

	return nil

}

func infoAboutSimulationMode(writer io.Writer, forceMode bool, forceModeOption string) {
	if forceMode {
		return
	}
	fmt.Fprintf(writer, "Running in simulation mode. Use \"%s\" option to apply the changes.\n", forceModeOption)
}
