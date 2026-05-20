package shell

import (
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandCollectionMark{})
}

type commandCollectionMark struct {
}

func (c *commandCollectionMark) Name() string {
	return "collection.mark"
}

func (c *commandCollectionMark) Help() string {
	return `Mark all existing normal volume replicas in one collection writable or readonly

	collection.mark -collection <collection_name> -readonly -apply
	collection.mark -collection <collection_name> -writable -apply

Use '_default' or '_default_' for the empty-named collection.
Without -apply, this command only prints the volume replicas that would be marked.
`
}

func (c *commandCollectionMark) HasTag(CommandTag) bool {
	return false
}

type collectionMarkOptions struct {
	collection   string
	markWritable bool
	apply        bool
}

type collectionMarkTarget struct {
	volumeId   needle.VolumeId
	collection string
	server     pb.ServerAddress
	readOnly   bool
}

func (c *commandCollectionMark) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	opts, err := parseCollectionMarkOptions(args)
	if err != nil {
		return err
	}

	infoAboutSimulationMode(writer, opts.apply, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	targets, skippedEC := collectCollectionMarkTargets(topologyInfo, opts.collection)
	action := "readonly"
	if opts.markWritable {
		action = "writable"
	}

	for _, target := range targets {
		fmt.Fprintf(writer, "volume %d collection %q on %s currentReadonly:%t will be marked %s\n",
			target.volumeId, target.collection, target.server, target.readOnly, action)
	}
	if skippedEC > 0 {
		fmt.Fprintf(writer, "Skipped %d EC shard volume(s); collection.mark only marks normal volumes.\n", skippedEC)
	}

	if len(targets) == 0 {
		fmt.Fprintf(writer, "No normal volume replicas found for collection %q.\n", opts.collection)
		return nil
	}

	if !opts.apply {
		fmt.Fprintf(writer, "Found %d normal volume replica(s) for collection %q. Use -apply to mark them %s.\n",
			len(targets), opts.collection, action)
		return nil
	}

	for _, target := range targets {
		if err := markVolumeWritable(commandEnv.option.GrpcDialOption, target.volumeId, target.server, opts.markWritable, true); err != nil {
			return fmt.Errorf("mark volume %d on %s as %s: %w", target.volumeId, target.server, action, err)
		}
	}

	fmt.Fprintf(writer, "Marked %d normal volume replica(s) in collection %q as %s.\n", len(targets), opts.collection, action)
	return nil
}

func parseCollectionMarkOptions(args []string) (*collectionMarkOptions, error) {
	collectionMarkCommand := flag.NewFlagSet("collection.mark", flag.ContinueOnError)
	collectionName := collectionMarkCommand.String("collection", "", "collection to mark. Use '_default' or '_default_' for the empty-named collection.")
	writable := collectionMarkCommand.Bool("writable", false, "collection mark writable")
	readonly := collectionMarkCommand.Bool("readonly", false, "collection mark readonly")
	apply := collectionMarkCommand.Bool("apply", false, "apply the collection mark")

	if err := collectionMarkCommand.Parse(args); err != nil {
		return nil, err
	}
	if *collectionName == "" {
		return nil, fmt.Errorf("empty collection name is not allowed")
	}
	if (*writable && *readonly) || (!*writable && !*readonly) {
		return nil, fmt.Errorf("use -readonly or -writable")
	}

	return &collectionMarkOptions{
		collection:   normalizeCollectionMarkName(*collectionName),
		markWritable: *writable,
		apply:        *apply,
	}, nil
}

func normalizeCollectionMarkName(collectionName string) string {
	if collectionName == CollectionDefault || collectionName == "_default_" {
		return ""
	}
	return collectionName
}

func collectCollectionMarkTargets(topo *master_pb.TopologyInfo, collection string) (targets []collectionMarkTarget, skippedEC int) {
	if topo == nil {
		return nil, 0
	}
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, dn := range rack.GetDataNodeInfos() {
				server := pb.NewServerAddressFromDataNode(dn)
				for _, diskType := range sortMapKey(dn.GetDiskInfos()) {
					diskInfo := dn.GetDiskInfos()[diskType]
					for _, vi := range diskInfo.GetVolumeInfos() {
						if vi.GetCollection() != collection {
							continue
						}
						targets = append(targets, collectionMarkTarget{
							volumeId:   needle.VolumeId(vi.GetId()),
							collection: vi.GetCollection(),
							server:     server,
							readOnly:   vi.GetReadOnly(),
						})
					}
					for _, ecShardInfo := range diskInfo.GetEcShardInfos() {
						if ecShardInfo.GetCollection() == collection {
							skippedEC++
						}
					}
				}
			}
		}
	}
	return targets, skippedEC
}
