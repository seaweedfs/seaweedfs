package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcVerify{})
}

type commandEcVerify struct {
}

func (c *commandEcVerify) Name() string {
	return "ec.verify"
}

func (c *commandEcVerify) Help() string {
	return `verify the integrity of erasure coded volumes

	ec.verify [-collection=<collection_name>] [-volumeId=<volume_id>] [-diskType=<disk_type>]

	This command reads EC shards and uses Reed-Solomon verification to detect
	corruption. It can handle distributed shards by asking the volume server
	to fetch remote shards as needed.

	When shards pass verification but some needles are corrupted, it identifies
	the shards containing those bad needles for further investigation.

	Options:
	  -collection: verify all EC volumes in this collection
	  -volumeId: verify a specific volume ID
	  -diskType: disk type (hdd, ssd)

`
}

func (c *commandEcVerify) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcVerify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	verifyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := verifyCommand.Int("volumeId", 0, "the volume id")
	collection := verifyCommand.String("collection", "", "the collection name")
	diskTypeStr := verifyCommand.String("diskType", "", "disk type")

	if err = verifyCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)
	diskType := types.ToDiskType(*diskTypeStr)

	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	var volumeIds []needle.VolumeId
	if vid != 0 {
		volumeIds = []needle.VolumeId{vid}
	} else {
		volumeIds, err = collectEcShardIds(topologyInfo, *collection, diskType)
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(writer, "Verifying %d EC volume(s)...\n\n", len(volumeIds))

	passCount := 0
	failCount := 0

	for _, volumeId := range volumeIds {
		verified, suspects, needlesVerified, badNeedleCount, badNeedleIds, badNeedleCookies, corruptedShards, volumeErr := doEcVerify(commandEnv, topologyInfo, *collection, volumeId, diskType, writer)
		if volumeErr != nil {
			fmt.Fprintf(writer, "  ✗ Volume %d: ERROR - %v\n", volumeId, volumeErr)
			failCount++
			continue
		}

		if verified && badNeedleCount == 0 {
			fmt.Fprintf(writer, "  ✓ Volume %d: PASS (shards and needles)\n", volumeId)
			passCount++
		} else {
			status := "FAIL"
			if verified && badNeedleCount > 0 {
				status = "PARTIAL (shards pass)"
			} else if !verified && badNeedleCount == 0 {
				status = "PARTIAL (needles pass)"
			}
			fmt.Fprintf(writer, "  ✗ Volume %d: %s", volumeId, status)
			if len(suspects) > 0 {
				fmt.Fprintf(writer, " (suspect shards: %v)", suspects)
			}
			if badNeedleCount > 0 {
				badNeedleStrings := make([]string, len(badNeedleIds))
				for i, needleId := range badNeedleIds {
					badNeedleStrings[i] = needle.NewFileId(needle.VolumeId(volumeId), needleId, badNeedleCookies[i]).String()
				}
				fmt.Fprintf(writer, " (%d bad needles: %v)", badNeedleCount, badNeedleStrings)
			}
			if len(corruptedShards) > 0 {
				fmt.Fprintf(writer, " (shards with bad needles: %v)", corruptedShards)
			}
			if !needlesVerified {
				fmt.Fprintf(writer, " (needle verification failed)")
			}
			fmt.Fprintf(writer, "\n")
			failCount++
		}
	}

	fmt.Fprintf(writer, "\nSummary: %d volume(s) checked, %d passed, %d failed\n", len(volumeIds), passCount, failCount)
	return nil
}

func doEcVerify(commandEnv *CommandEnv, topoInfo *master_pb.TopologyInfo, collection string, vid needle.VolumeId, diskType types.DiskType, writer io.Writer) (verified bool, suspectShardIds []uint32, needlesVerified bool, badNeedleCount uint64, badNeedleIds []uint64, badNeedleCookies []uint32, corruptedShards []uint32, err error) {
	fmt.Fprintf(writer, "Verifying EC volume %d...\n", vid)

	// Pick ANY node that has at least one shard of this volume to act as the coordinator.
	// Ideally picking one that has data shards.
	nodeToEcIndexBits := collectEcNodeShardBits(topoInfo, vid, diskType)
	var targetNode pb.ServerAddress
	found := false

	// Try to find a node with .ecx file? Or just any node.
	// Since verification handles remote reading, any node with the EC volume mounted (even partial)
	// should theoretically be able to coordinate, provided it has the index.
	// But `VerifyEcShards` opens .ecx to get config. So we need a node that has the .ecx/.vif.
	// In SeaweedFS, .ecx is usually replicated to all nodes holding shards? No.
	// Actually `VolumeEcShardsGenerate` distributes .ecx.

	// Let's pick the node with the most shards, just to be safe.
	maxShards := 0
	for node, shardBits := range nodeToEcIndexBits {
		count := shardBits.ShardIdCount()
		if count > maxShards {
			maxShards = count
			targetNode = node
			found = true
		}
	}

	if !found {
		return false, nil, false, 0, nil, nil, nil, fmt.Errorf("no nodes found with shards for volume %d", vid)
	}

	fmt.Fprintf(writer, "  Coordinator node: %s (has %d shards)\n", targetNode, maxShards)

	return verifyEcVolume(commandEnv.option.GrpcDialOption, collection, vid, targetNode)
}

func verifyEcVolume(grpcDialOption grpc.DialOption, collection string, vid needle.VolumeId, sourceLocation pb.ServerAddress) (verified bool, suspectShardIds []uint32, needlesVerified bool, badNeedleCount uint64, badNeedleIds []uint64, badNeedleCookies []uint32, corruptedShards []uint32, err error) {
	err = operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, verifyErr := volumeServerClient.VolumeEcShardsVerify(context.Background(), &volume_server_pb.VolumeEcShardsVerifyRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
		})
		if verifyErr != nil {
			return verifyErr
		}

		verified = resp.Verified
		suspectShardIds = resp.SuspectShardIds
		needlesVerified = resp.NeedlesVerified
		badNeedleCount = uint64(len(resp.BadNeedleIds))
		badNeedleIds = resp.BadNeedleIds
		badNeedleCookies = resp.BadNeedleCookies
		corruptedShards = resp.CorruptedShards
		if resp.ErrorMessage != "" {
			return fmt.Errorf("%s", resp.ErrorMessage)
		}

		return nil
	})

	return verified, suspectShardIds, needlesVerified, badNeedleCount, badNeedleIds, badNeedleCookies, corruptedShards, err
}
