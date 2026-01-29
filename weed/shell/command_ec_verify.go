package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

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

	ec.verify [-collection=<collection_name>] [-volumeId=<volume_id>] [-diskType=<disk_type>] [-timeout=<duration>]

	This command reads EC shards and uses Reed-Solomon verification to detect
	corruption. It can handle distributed shards by asking the volume server
	to fetch remote shards as needed.

	When shards pass verification but some needles are corrupted, it identifies
	the shards containing those bad needles for further investigation.

	Options:
	  -collection: verify all EC volumes in this collection
	  -volumeId: verify a specific volume ID
	  -diskType: disk type (hdd, ssd)
	  -timeout: overall timeout per volume (default: 5m)

	Examples:
	  ec.verify -collection=photos          # Verify all EC volumes in collection
	  ec.verify -volumeId=123              # Verify specific volume
	  ec.verify -volumeId=123 -timeout=10m # Verify with custom timeout

`
}

func (c *commandEcVerify) HasTag(CommandTag) bool {
	return false
}

type VerificationResult struct {
	VolumeID         needle.VolumeId
	Verified         bool
	SuspectShards    []uint32
	NeedlesVerified  bool
	BadNeedleCount   uint64
	BadNeedleIds     []uint64
	BadNeedleCookies []uint32
	CorruptedShards  []uint32
	Error            error
}

func (c *commandEcVerify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	verifyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := verifyCommand.Int("volumeId", 0, "the volume id")
	collection := verifyCommand.String("collection", "", "the collection name")
	diskTypeStr := verifyCommand.String("diskType", "", "disk type")
	timeoutPtr := verifyCommand.Duration("timeout", 5*time.Minute, "overall timeout per volume")

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

	results := []VerificationResult{}

	for i, volumeId := range volumeIds {
		fmt.Fprintf(writer, "[%d/%d] Verifying volume %d...\n", i+1, len(volumeIds), volumeId)

		verified, suspects, needlesVerified, badNeedleCount, badNeedleIds, badNeedleCookies, corruptedShards, volumeErr := doEcVerify(commandEnv, topologyInfo, *collection, volumeId, diskType, writer, *timeoutPtr)

		result := VerificationResult{
			VolumeID:         volumeId,
			Verified:         verified,
			SuspectShards:    suspects,
			NeedlesVerified:  needlesVerified,
			BadNeedleCount:   badNeedleCount,
			BadNeedleIds:     badNeedleIds,
			BadNeedleCookies: badNeedleCookies,
			CorruptedShards:  corruptedShards,
			Error:            volumeErr,
		}
		results = append(results, result)

		if volumeErr != nil {
			fmt.Fprintf(writer, "  ✗ Volume %d: ERROR - %v\n", volumeId, volumeErr)
			continue
		}

		if verified && badNeedleCount == 0 {
			fmt.Fprintf(writer, "  ✓ Volume %d: PASS (shards and needles)\n", volumeId)
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
				fmt.Fprintf(writer, " (shards containing corrupted needles: %v)", corruptedShards)
			}
			if !needlesVerified {
				fmt.Fprintf(writer, " (needle verification failed)")
			}
			fmt.Fprintf(writer, "\n")
		}
	}

	// Calculate summary statistics
	passCount := 0
	failCount := 0
	errorCount := 0
	totalBadNeedles := uint64(0)

	for _, result := range results {
		if result.Error != nil {
			errorCount++
		} else if result.Verified && result.BadNeedleCount == 0 {
			passCount++
		} else {
			failCount++
			totalBadNeedles += result.BadNeedleCount
		}
	}

	fmt.Fprintf(writer, "\nSummary: %d volume(s) checked, %d passed, %d failed, %d errors\n", len(volumeIds), passCount, failCount, errorCount)

	if totalBadNeedles > 0 {
		fmt.Fprintf(writer, "Total bad needles across all volumes: %d\n", totalBadNeedles)
	}

	// Show failed volumes in a consolidated view
	if failCount > 0 || errorCount > 0 {
		fmt.Fprintf(writer, "\nFailed Volumes:\n")
		for _, result := range results {
			if result.Error != nil {
				fmt.Fprintf(writer, "  - Volume %d: ERROR - %v\n", result.VolumeID, result.Error)
			} else if !result.Verified || result.BadNeedleCount > 0 {
				fmt.Fprintf(writer, "  - Volume %d:", result.VolumeID)
				if !result.Verified {
					fmt.Fprintf(writer, " shards failed")
				}
				if result.BadNeedleCount > 0 {
					if !result.Verified {
						fmt.Fprintf(writer, ", ")
					}
					fmt.Fprintf(writer, "%d bad needles", result.BadNeedleCount)
				}
				fmt.Fprintf(writer, "\n")
			}
		}
	}

	return nil
}

func doEcVerify(commandEnv *CommandEnv, topoInfo *master_pb.TopologyInfo, collection string, vid needle.VolumeId, diskType types.DiskType, writer io.Writer, timeout time.Duration) (verified bool, suspectShardIds []uint32, needlesVerified bool, badNeedleCount uint64, badNeedleIds []uint64, badNeedleCookies []uint32, corruptedShards []uint32, err error) {
	fmt.Fprintf(writer, "Verifying EC volume %d...\n", vid)

	nodeToEcIndexBits := collectEcNodeShardsInfo(topoInfo, vid, diskType)
	var targetNode pb.ServerAddress
	found := false

	// Let's pick the node with the most shards to coordinate
	maxShards := 0
	for node, shardsInfo := range nodeToEcIndexBits {
		count := shardsInfo.Count()
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

	return verifyEcVolume(commandEnv.option.GrpcDialOption, collection, vid, targetNode, timeout)
}

func verifyEcVolume(grpcDialOption grpc.DialOption, collection string, vid needle.VolumeId, sourceLocation pb.ServerAddress, timeout time.Duration) (verified bool, suspectShardIds []uint32, needlesVerified bool, badNeedleCount uint64, badNeedleIds []uint64, badNeedleCookies []uint32, corruptedShards []uint32, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, verifyErr := volumeServerClient.VolumeEcShardsVerify(ctx, &volume_server_pb.VolumeEcShardsVerifyRequest{
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
