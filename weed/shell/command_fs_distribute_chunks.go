package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func init() {
	Commands = append(Commands, &commandFsDistributeChunks{})
}

type commandFsDistributeChunks struct{}

func (c *commandFsDistributeChunks) Name() string {
	return "fs.distributeChunks"
}

func (c *commandFsDistributeChunks) Help() string {
	return `redistribute file chunks evenly across volume server nodes

	Modes:
	  primary      (default) balance chunk count per node using topology-based ownership.
	               Note: with replication, ownership is derived from volume ID hashing
	               (vid %% node count), which may not reflect the actual Assign target.
	               Results can be approximate in replicated environments.

	  replica      balance both ownership and replica copies across nodes.
	               Step 1: owner-based balancing (same as primary mode).
	               Step 2: additional moves to balance total copies (including replicas).
	               Same ownership limitation as primary mode applies.

	  round-robin  assign chunks to nodes by file offset order (chunk[0]->A, chunk[1]->B,
	               chunk[2]->C, ...). Best mode for sequential read performance — ensures
	               consecutive chunks are on different nodes for I/O pipelining.
	               Recommended for replication environments as it does not depend on
	               ownership calculation.

	Files using chunk manifests (very large files) are handled by resolving manifests
	to the underlying data chunks, redistributing those, then re-manifestizing before
	updating the filer. Old manifest + data chunks are garbage-collected by the filer.

	# analyze current distribution (dry-run, primary mode)
	fs.distributeChunks -path=/buckets/my-bucket/large-file.dat

	# apply redistribution
	fs.distributeChunks -path=/buckets/my-bucket/large-file.dat -apply

	# distribute across 5 nodes (instead of all)
	fs.distributeChunks -path=/buckets/my-bucket/large-file.dat -nodes=5 -apply

	# balance including replica copies
	fs.distributeChunks -path=/buckets/my-bucket/large-file.dat -mode=replica -apply

	# round-robin for sequential read performance (recommended with replication)
	fs.distributeChunks -path=/buckets/my-bucket/large-file.dat -mode=round-robin -apply
`
}

func (c *commandFsDistributeChunks) HasTag(CommandTag) bool {
	return false
}

type chunkMove struct {
	chunkIndex int
	chunk      *filer_pb.FileChunk
	fromNode   string
	toNode     string
}


func (c *commandFsDistributeChunks) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsDistributeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	filePath := fsDistributeCommand.String("path", "", "file path to redistribute chunks")
	apply := fsDistributeCommand.Bool("apply", false, "apply the redistribution")
	targetNodeCount := fsDistributeCommand.Int("nodes", 0, "number of nodes to distribute across (0 = all nodes)")
	mode := fsDistributeCommand.String("mode", "primary", "distribution mode: primary, replica, round-robin")
	if err = fsDistributeCommand.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return fmt.Errorf("-path is required")
	}
	if *mode != "primary" && *mode != "replica" && *mode != "round-robin" {
		return fmt.Errorf("-mode must be one of: primary, replica, round-robin")
	}

	infoAboutSimulationMode(writer, *apply, "-apply")

	path, err := commandEnv.parseUrl(*filePath)
	if err != nil {
		return err
	}
	dir, name := util.FullPath(path).DirAndName()

	entry, err := lookupFileEntry(commandEnv, dir, name)
	if err != nil {
		return fmt.Errorf("lookup %s: %v", path, err)
	}

	chunks, hadManifest, err := resolveEntryDataChunks(commandEnv, entry)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		fmt.Fprintf(writer, "File has no chunks (possibly stored inline)\n")
		return nil
	}
	if hadManifest {
		fmt.Fprintf(writer, "Resolved chunk manifests: %d top-level chunk(s) -> %d data chunks.\n", len(entry.GetChunks()), len(chunks))
	}

	volumeInfoMap, volumeNodesList, volumeToOwner, nodeList, err := collectVolumeTopology(commandEnv)
	if err != nil {
		return err
	}

	if len(nodeList) < 2 {
		return fmt.Errorf("need at least 2 volume nodes, found %d", len(nodeList))
	}
	if *targetNodeCount < 0 {
		return fmt.Errorf("-nodes must be >= 0 (0 = all nodes)")
	}
	if *targetNodeCount > len(nodeList) {
		return fmt.Errorf("-nodes=%d exceeds available nodes (%d)", *targetNodeCount, len(nodeList))
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	if err != nil {
		return err
	}

	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, *targetNodeCount)

	totalChunks := len(chunks)
	totalNodes := len(activeNodeList)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	fmt.Fprintf(writer, "\nFile: %s\n", path)
	fmt.Fprintf(writer, "Total chunks: %d, Total nodes: %d, Mode: %s\n", totalChunks, totalNodes, *mode)
	printCurrentDistribution(writer, nodeList, ownerCount, copiesCount, activeSet, *targetNodeCount, *mode, totalChunks, totalNodes, totalCopies)

	moves, ownerTarget := planDistribution(*mode, writer, activeNodeList, nodeList, activeSet, ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList, totalChunks, totalNodes, totalCopies)

	if len(moves) == 0 {
		fmt.Fprintf(writer, "\nAlready well distributed. No moves needed.\n")
		return nil
	}

	allRelevantNodes := relevantNodes(nodeList, activeSet, ownerCount)
	printRedistributionPlan(writer, allRelevantNodes, ownerCount, ownerTarget, len(moves))

	if !*apply {
		fmt.Fprintf(writer, "\nTo apply, add -apply flag\n")
		return nil
	}

	fmt.Fprintf(writer, "\nExecuting redistribution...\n")
	defer util_http.GetGlobalHttpClient().CloseIdleConnections()

	movedCount, err := executeChunkMoves(commandEnv, writer, moves, volumeInfoMap)
	if err != nil {
		return err
	}
	if movedCount == 0 {
		fmt.Fprintf(writer, "\nNo chunks were moved successfully.\n")
		return nil
	}

	finalChunks := chunks
	if hadManifest {
		remanifested, manErr := filer.MaybeManifestize(newShellSaveAsChunk(commandEnv), chunks)
		if manErr != nil {
			fmt.Fprintf(writer, "WARNING: re-manifestize failed: %v. Writing flat chunk list.\n", manErr)
		} else {
			finalChunks = remanifested
			fmt.Fprintf(writer, "Re-manifestized %d data chunks into %d top-level chunk(s).\n", len(chunks), len(finalChunks))
		}
	}
	entry.Chunks = finalChunks

	fmt.Fprintf(writer, "\nUpdating filer metadata (%d chunks moved)...\n", movedCount)
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
	})
	if err != nil {
		fmt.Fprintf(writer, "FAILED to update filer metadata: %v\n", err)
		fmt.Fprintf(writer, "WARNING: New chunks were uploaded but metadata was not updated.\n")
		fmt.Fprintf(writer, "The original file is still intact. New chunks are orphaned.\n")
		return err
	}
	fmt.Fprintf(writer, "Filer metadata updated successfully.\n")

	fmt.Fprintf(writer, "\nRedistribution complete. %d chunks moved.\n", movedCount)
	return nil
}

// lookupFileEntry fetches the filer entry for the given directory and name.
func lookupFileEntry(commandEnv *CommandEnv, dir, name string) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("lookup for %s/%s returned no entry", dir, name)
	}
	if entry.IsDirectory {
		return nil, fmt.Errorf("%s/%s is a directory", dir, name)
	}
	return entry, nil
}

// resolveEntryDataChunks returns the flattened data chunks of an entry. If the
// entry contains manifest chunks, their contents are resolved. hadManifest
// signals whether any manifest chunks were resolved, so the caller knows to
// re-manifestize after redistribution.
func resolveEntryDataChunks(commandEnv *CommandEnv, entry *filer_pb.Entry) (dataChunks []*filer_pb.FileChunk, hadManifest bool, err error) {
	chunks := entry.GetChunks()
	if !filer.HasChunkManifest(chunks) {
		return chunks, false, nil
	}
	dataChunks, _, err = filer.ResolveChunkManifest(context.Background(), filer.LookupFn(commandEnv), chunks, 0, math.MaxInt64)
	if err != nil {
		return nil, true, fmt.Errorf("resolve chunk manifest: %v", err)
	}
	return dataChunks, true, nil
}

// collectVolumeTopology queries the master for cluster topology and returns per-volume
// metadata, per-volume node lists, per-volume ownership, and the sorted node list.
func collectVolumeTopology(commandEnv *CommandEnv) (
	volumeInfoMap map[uint32]*master_pb.VolumeInformationMessage,
	volumeNodesList map[uint32][]string,
	volumeToOwner map[uint32]string,
	nodeList []string,
	err error,
) {
	topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("collect topology: %v", err)
	}

	volumeInfoMap = make(map[uint32]*master_pb.VolumeInformationMessage)
	volumeNodesList = make(map[uint32][]string)
	allNodes := make(map[string]bool)

	for _, dc := range topoInfo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, dn := range rack.GetDataNodeInfos() {
				nodeId := dn.GetId()
				allNodes[nodeId] = true
				for _, disk := range dn.GetDiskInfos() {
					for _, vol := range disk.GetVolumeInfos() {
						volumeNodesList[vol.GetId()] = append(volumeNodesList[vol.GetId()], nodeId)
						if _, exists := volumeInfoMap[vol.GetId()]; !exists {
							volumeInfoMap[vol.GetId()] = vol
						}
					}
				}
			}
		}
	}

	volumeToOwner = make(map[uint32]string)
	for vid, nodes := range volumeNodesList {
		sort.Strings(nodes)
		volumeToOwner[vid] = nodes[int(vid)%len(nodes)]
	}

	nodeList = make([]string, 0, len(allNodes))
	for node := range allNodes {
		nodeList = append(nodeList, node)
	}
	sort.Strings(nodeList)

	return volumeInfoMap, volumeNodesList, volumeToOwner, nodeList, nil
}

// buildDistributionCounts maps each chunk to its owning node and tallies owner/copy counts.
func buildDistributionCounts(
	chunks []*filer_pb.FileChunk,
	volumeToOwner map[uint32]string,
	volumeNodesList map[uint32][]string,
) (chunkToNode map[int]string, ownerCount map[string]int, copiesCount map[string]int, err error) {
	chunkToNode = make(map[int]string)
	ownerCount = make(map[string]int)
	copiesCount = make(map[string]int)

	for i, chunk := range chunks {
		var vid uint32
		if chunk.Fid == nil {
			fileId, parseErr := needle.ParseFileIdFromString(chunk.GetFileIdString())
			if parseErr != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse file id for chunk %d: %v", i, parseErr)
			}
			vid = uint32(fileId.VolumeId)
		} else {
			vid = chunk.Fid.GetVolumeId()
		}
		owner, ok := volumeToOwner[vid]
		if !ok {
			return nil, nil, nil, fmt.Errorf("volume %d not found in topology", vid)
		}
		chunkToNode[i] = owner
		ownerCount[owner]++
		for _, node := range volumeNodesList[vid] {
			copiesCount[node]++
		}
	}
	return chunkToNode, ownerCount, copiesCount, nil
}

// selectActiveNodes picks up to targetNodeCount nodes to participate in redistribution.
// When targetNodeCount is 0 all nodes are active.
func selectActiveNodes(nodeList []string, ownerCount map[string]int, targetNodeCount int) (activeNodeList []string, activeSet map[string]bool) {
	activeNodeList = nodeList
	if targetNodeCount > 0 && targetNodeCount < len(nodeList) {
		var withChunks, withoutChunks []string
		for _, node := range nodeList {
			if ownerCount[node] > 0 {
				withChunks = append(withChunks, node)
			} else {
				withoutChunks = append(withoutChunks, node)
			}
		}
		n := targetNodeCount
		if len(withChunks) >= n {
			sort.Slice(withChunks, func(i, j int) bool {
				return ownerCount[withChunks[i]] > ownerCount[withChunks[j]]
			})
			activeNodeList = withChunks[:n]
		} else {
			activeNodeList = append(withChunks, withoutChunks[:n-len(withChunks)]...)
		}
		sort.Strings(activeNodeList)
	}

	activeSet = make(map[string]bool, len(activeNodeList))
	for _, node := range activeNodeList {
		activeSet[node] = true
	}
	return activeNodeList, activeSet
}

// printCurrentDistribution writes the per-node chunk/copy table to writer.
func printCurrentDistribution(
	writer io.Writer,
	nodeList []string,
	ownerCount, copiesCount map[string]int,
	activeSet map[string]bool,
	targetNodeCount int,
	mode string,
	totalChunks, totalNodes, totalCopies int,
) {
	maxOwner := 0
	for _, cnt := range ownerCount {
		if cnt > maxOwner {
			maxOwner = cnt
		}
	}

	fmt.Fprintf(writer, "\nCurrent distribution:\n")
	for _, node := range nodeList {
		oc := ownerCount[node]
		cc := copiesCount[node]
		bar := strings.Repeat("#", int(math.Ceil(float64(oc)*30/math.Max(float64(maxOwner), 1))))
		marker := " "
		if targetNodeCount > 0 && !activeSet[node] {
			marker = "-"
		}
		if mode == "replica" {
			fmt.Fprintf(writer, "  %s %-20s %4d chunks  %4d copies  %-30s\n", marker, shortName(node), oc, cc, bar)
		} else {
			fmt.Fprintf(writer, "  %s %-20s %4d chunks  %-30s\n", marker, shortName(node), oc, bar)
		}
	}
	fmt.Fprintf(writer, "  Ideal: ~%.0f chunks per node", math.Ceil(float64(totalChunks)/float64(totalNodes)))
	if mode == "replica" {
		fmt.Fprintf(writer, ", ~%.0f copies per node", math.Ceil(float64(totalCopies)/float64(totalNodes)))
	}
	fmt.Fprintln(writer)
}

// planDistribution computes the set of chunk moves required for the chosen mode.
func planDistribution(
	mode string,
	writer io.Writer,
	activeNodeList, nodeList []string,
	activeSet map[string]bool,
	ownerCount, copiesCount map[string]int,
	chunkToNode map[int]string,
	chunks []*filer_pb.FileChunk,
	volumeNodesList map[uint32][]string,
	totalChunks, totalNodes, totalCopies int,
) (moves []chunkMove, ownerTarget map[string]int) {
	type nodeExcess struct {
		node   string
		excess int
	}

	allRelevantNodes := relevantNodes(nodeList, activeSet, ownerCount)
	ownerTarget = make(map[string]int)

	switch mode {
	case "primary":
		ownerTarget = computeOwnerTarget(activeNodeList, nodeList, activeSet, totalChunks, totalNodes)
		moves = planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, allRelevantNodes)

	case "replica":
		ownerTarget = computeOwnerTarget(activeNodeList, nodeList, activeSet, totalChunks, totalNodes)
		moves = planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, allRelevantNodes)

		copiesMin := totalCopies / totalNodes
		copiesRemainder := totalCopies % totalNodes
		copiesTarget := make(map[string]int)
		for i, node := range activeNodeList {
			if i < copiesRemainder {
				copiesTarget[node] = copiesMin + 1
			} else {
				copiesTarget[node] = copiesMin
			}
		}

		alreadyMoving := make(map[int]bool)
		for _, mv := range moves {
			alreadyMoving[mv.chunkIndex] = true
		}

		// local copy so the planner does not mutate the caller's copiesCount
		localCopiesCount := maps.Clone(copiesCount)

		var copiesOver, copiesUnder []nodeExcess
		for _, node := range activeNodeList {
			diff := localCopiesCount[node] - copiesTarget[node]
			if diff > 0 {
				copiesOver = append(copiesOver, nodeExcess{node, diff})
			} else if diff < 0 {
				copiesUnder = append(copiesUnder, nodeExcess{node, -diff})
			}
		}
		sort.Slice(copiesOver, func(i, j int) bool {
			return copiesOver[i].excess > copiesOver[j].excess
		})

		for oi := range copiesOver {
			over := &copiesOver[oi]
			if over.excess <= 0 {
				continue
			}
			for idx, chunk := range chunks {
				if over.excess <= 0 {
					break
				}
				if alreadyMoving[idx] {
					continue
				}
				vid, vidErr := chunkVolumeId(chunk)
				if vidErr != nil {
					continue
				}
				if !slices.Contains(volumeNodesList[vid], over.node) {
					continue
				}
				sort.Slice(copiesUnder, func(i, j int) bool {
					return copiesUnder[i].excess > copiesUnder[j].excess
				})
				for j := range copiesUnder {
					if copiesUnder[j].excess > 0 {
						moves = append(moves, chunkMove{
							chunkIndex: idx,
							chunk:      chunk,
							fromNode:   chunkToNode[idx],
							toNode:     copiesUnder[j].node,
						})
						alreadyMoving[idx] = true
						for _, srcNode := range volumeNodesList[vid] {
							localCopiesCount[srcNode]--
							for k := range copiesOver {
								if copiesOver[k].node == srcNode {
									copiesOver[k].excess--
								}
							}
						}
						copiesUnder[j].excess--
						localCopiesCount[copiesUnder[j].node]++
						over.excess--
						break
					}
				}
			}
		}

	case "round-robin":
		type chunkWithIndex struct {
			index  int
			offset int64
		}
		sortedChunks := make([]chunkWithIndex, totalChunks)
		for i, chunk := range chunks {
			sortedChunks[i] = chunkWithIndex{index: i, offset: chunk.GetOffset()}
		}
		sort.Slice(sortedChunks, func(i, j int) bool {
			return sortedChunks[i].offset < sortedChunks[j].offset
		})

		for i, sc := range sortedChunks {
			targetNode := activeNodeList[i%totalNodes]
			currentOwner := chunkToNode[sc.index]
			ownerTarget[targetNode]++
			if currentOwner != targetNode {
				moves = append(moves, chunkMove{
					chunkIndex: sc.index,
					chunk:      chunks[sc.index],
					fromNode:   currentOwner,
					toNode:     targetNode,
				})
			}
		}

		fmt.Fprintf(writer, "\nRound-robin assignment (sequential read optimized):\n")
		fmt.Fprintf(writer, "  First assignments: ")
		for pos := range sortedChunks {
			if pos >= 12 {
				fmt.Fprintf(writer, "... ")
				break
			}
			fmt.Fprintf(writer, "%s ", shortName(activeNodeList[pos%totalNodes]))
		}
		fmt.Fprintf(writer, "\n  Pattern: ")
		for i := 0; i < totalNodes && i < 6; i++ {
			fmt.Fprintf(writer, "%s ", shortName(activeNodeList[i]))
		}
		if totalNodes > 6 {
			fmt.Fprintf(writer, "...")
		}
		fmt.Fprintf(writer, "(repeating)\n")
	}

	return moves, ownerTarget
}

// printRedistributionPlan writes the before/after chunk count table for each node.
func printRedistributionPlan(writer io.Writer, allRelevantNodes []string, ownerCount, ownerTarget map[string]int, numMoves int) {
	fmt.Fprintf(writer, "\nRedistribution plan: %d chunks to move\n", numMoves)
	for _, node := range allRelevantNodes {
		current := ownerCount[node]
		target := ownerTarget[node]
		diff := target - current
		sign := ""
		if diff > 0 {
			sign = "+"
		}
		fmt.Fprintf(writer, "  %-25s %4d -> %4d  (%s%d)\n", shortName(node), current, target, sign, diff)
	}
}

// relevantNodes returns nodes that are either active or currently hold chunks.
func relevantNodes(nodeList []string, activeSet map[string]bool, ownerCount map[string]int) []string {
	relevant := make([]string, 0, len(nodeList))
	for _, node := range nodeList {
		if activeSet[node] || ownerCount[node] > 0 {
			relevant = append(relevant, node)
		}
	}
	return relevant
}

// chunkVolumeId extracts the volume ID from a chunk, falling back to parsing
// the legacy FileId string when Fid is nil.
func chunkVolumeId(chunk *filer_pb.FileChunk) (uint32, error) {
	if chunk.Fid != nil {
		return chunk.Fid.GetVolumeId(), nil
	}
	fileId, err := needle.ParseFileIdFromString(chunk.GetFileIdString())
	if err != nil {
		return 0, fmt.Errorf("failed to parse file id %q: %v", chunk.GetFileIdString(), err)
	}
	return uint32(fileId.VolumeId), nil
}

// shortName returns the hostname portion (before the first ".") of a node ID.
func shortName(nodeID string) string {
	return strings.Split(nodeID, ".")[0]
}

// computeOwnerTarget returns the target chunk count per node for owner-based balancing.
func computeOwnerTarget(activeNodeList, nodeList []string, activeSet map[string]bool, totalChunks, totalNodes int) map[string]int {
	target := make(map[string]int)
	perNode := totalChunks / totalNodes
	remainder := totalChunks % totalNodes
	for i, node := range activeNodeList {
		if i < remainder {
			target[node] = perNode + 1
		} else {
			target[node] = perNode
		}
	}
	for _, node := range nodeList {
		if !activeSet[node] {
			target[node] = 0
		}
	}
	return target
}

// executeChunkMoves runs all moves concurrently and returns the number of successfully moved chunks.
// Old chunk cleanup is handled automatically by the filer: filer.UpdateEntry calls
// deleteChunksIfNotNew which removes chunks present in the old entry but not the new one.
func executeChunkMoves(
	commandEnv *CommandEnv,
	writer io.Writer,
	moves []chunkMove,
	volumeInfoMap map[uint32]*master_pb.VolumeInformationMessage,
) (int, error) {
	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, fmt.Errorf("create uploader: %v", err)
	}

	var mu sync.Mutex
	movedCount := 0
	ewg := NewErrorWaitGroup(DefaultMaxParallelization)

	for i, mv := range moves {
		ewg.Add(func() error {
			chunk := mv.chunk
			oldFidStr := chunk.GetFileIdString()
			prefix := fmt.Sprintf("  [%d/%d] Moving %s from %s to %s",
				i+1, len(moves), oldFidStr,
				shortName(mv.fromNode), shortName(mv.toNode),
			)

			fail := func(msg string) {
				mu.Lock()
				fmt.Fprintf(writer, "%s ... FAILED (%s)\n", prefix, msg)
				mu.Unlock()
			}

			oldFid, parseErr := needle.ParseFileIdFromString(oldFidStr)
			if parseErr != nil {
				fail(fmt.Sprintf("parse fid: %v", parseErr))
				return nil
			}

			downloadURLs, lookupErr := commandEnv.MasterClient.LookupVolumeServerUrl(oldFid.VolumeId.String())
			if lookupErr != nil || len(downloadURLs) == 0 {
				fail(fmt.Sprintf("lookup source: %v", lookupErr))
				return nil
			}

			// dlCancel must be called after reader.Close() — the response body is
			// streamed into the uploader, so cancelling the context before the
			// body is fully consumed causes "context canceled" upload failures.
			dlCtx, dlCancel := context.WithTimeout(context.Background(), 5*time.Minute)
			var resp *http.Response
			var reader io.ReadCloser
			var readErr error
			for _, serverURL := range downloadURLs {
				var dlReq *http.Request
				dlReq, readErr = http.NewRequestWithContext(dlCtx, http.MethodGet, fmt.Sprintf("http://%s/%s", serverURL, oldFidStr), nil)
				if readErr != nil {
					continue
				}
				dlReq.Header.Add("Accept-Encoding", "gzip")
				resp, readErr = util_http.GetGlobalHttpClient().Do(dlReq)
				if readErr == nil && resp.StatusCode >= 400 {
					util_http.CloseResponse(resp)
					readErr = fmt.Errorf("download %s: %s", serverURL, resp.Status)
				}
				if readErr == nil {
					reader = resp.Body
					break
				}
			}
			if readErr != nil {
				dlCancel()
				fail(fmt.Sprintf("download: %v", readErr))
				return nil
			}

			contentType := resp.Header.Get("Content-Type")
			isCompressed := resp.Header.Get("Content-Encoding") == "gzip"
			md5Hash := resp.Header.Get("Content-MD5")
			var filename string
			if cd := resp.Header.Get("Content-Disposition"); len(cd) > 0 {
				if _, after, found := strings.Cut(cd, "filename="); found {
					filename = strings.Trim(after, "\"")
				}
			}

			// Buffer the full response body so the download context can be
			// cancelled before the upload starts — otherwise the 5-minute
			// dlCtx timeout would also cancel the (potentially slow) upload.
			bodyBytes, readBodyErr := io.ReadAll(reader)
			reader.Close()
			util_http.CloseResponse(resp)
			dlCancel()
			if readBodyErr != nil {
				fail(fmt.Sprintf("read body: %v", readBodyErr))
				return nil
			}

			var replication, collection string
			if vInfo, ok := volumeInfoMap[uint32(oldFid.VolumeId)]; ok {
				replication = fmt.Sprintf("%03d", vInfo.GetReplicaPlacement())
				collection = vInfo.GetCollection()
			}

			assignResult, assignErr := operation.Assign(context.Background(), commandEnv.MasterClient.GetMaster, commandEnv.option.GrpcDialOption,
				&operation.VolumeAssignRequest{
					Count:       1,
					Replication: replication,
					Collection:  collection,
					DataNode:    mv.toNode,
				})
			if assignErr != nil {
				fail(fmt.Sprintf("assign: %v", assignErr))
				return nil
			}

			uploadURL := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

			var jwt security.EncodedJwt
			if assignResult.Auth != "" {
				jwt = assignResult.Auth
			} else {
				v := util.GetViper()
				signingKey := v.GetString("jwt.signing.key")
				if signingKey != "" {
					expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
					jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, assignResult.Fid)
				}
			}

			_, uploadErr, _ := uploader.Upload(context.Background(), bytes.NewReader(bodyBytes), &operation.UploadOption{
				UploadUrl:         uploadURL,
				Filename:          filename,
				IsInputCompressed: isCompressed,
				Cipher:            false,
				MimeType:          contentType,
				Md5:               md5Hash,
				Jwt:               jwt,
			})

			if uploadErr != nil {
				fail(fmt.Sprintf("upload: %v", uploadErr))
				return nil
			}

			newFid, fidParseErr := needle.ParseFileIdFromString(assignResult.Fid)
			if fidParseErr != nil {
				fail(fmt.Sprintf("parse new fid: %v", fidParseErr))
				return nil
			}

			mu.Lock()
			movedCount++
			chunk.Fid = &filer_pb.FileId{
				VolumeId: uint32(newFid.VolumeId),
				FileKey:  uint64(newFid.Key),
				Cookie:   uint32(newFid.Cookie),
			}
			chunk.FileId = ""
			fmt.Fprintf(writer, "%s ... OK -> %s\n", prefix, assignResult.Fid)
			mu.Unlock()

			return nil
		})
	}
	_ = ewg.Wait()
	return movedCount, nil
}

// newShellSaveAsChunk returns a SaveDataAsChunk function for use with
// filer.MaybeManifestize from shell context. The master selects placement —
// manifest chunks are metadata and don't need fan-out distribution.
func newShellSaveAsChunk(commandEnv *CommandEnv) filer.SaveDataAsChunkFunctionType {
	return filer.SaveDataAsChunkFunctionType(func(reader io.Reader, name string, offset int64, tsNs int64, _ uint64) (*filer_pb.FileChunk, error) {
		uploader, err := operation.NewUploader()
		if err != nil {
			return nil, fmt.Errorf("create uploader: %v", err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read manifest data: %v", err)
		}
		assignResult, err := operation.Assign(context.Background(), commandEnv.MasterClient.GetMaster, commandEnv.option.GrpcDialOption,
			&operation.VolumeAssignRequest{Count: 1})
		if err != nil {
			return nil, fmt.Errorf("assign manifest chunk: %v", err)
		}
		var jwt security.EncodedJwt
		if assignResult.Auth != "" {
			jwt = assignResult.Auth
		} else {
			v := util.GetViper()
			signingKey := v.GetString("jwt.signing.key")
			if signingKey != "" {
				expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
				jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, assignResult.Fid)
			}
		}
		uploadResult, uploadErr, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
			UploadUrl: fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid),
			Filename:  name,
			Jwt:       jwt,
		})
		if uploadErr != nil {
			return nil, fmt.Errorf("upload manifest chunk: %v", uploadErr)
		}
		if uploadResult.Error != "" {
			return nil, fmt.Errorf("upload manifest chunk: %s", uploadResult.Error)
		}
		return uploadResult.ToPbFileChunk(assignResult.Fid, offset, tsNs), nil
	})
}

// planOwnerMoves generates moves to balance owner-based chunk distribution.
func planOwnerMoves(ownerCount, ownerTarget map[string]int, chunkToNode map[int]string, chunks []*filer_pb.FileChunk, allRelevantNodes []string) []chunkMove {
	type nodeExcess struct {
		node   string
		excess int
	}
	var overNodes, underNodes []nodeExcess
	for _, node := range allRelevantNodes {
		diff := ownerCount[node] - ownerTarget[node]
		if diff > 0 {
			overNodes = append(overNodes, nodeExcess{node, diff})
		} else if diff < 0 {
			underNodes = append(underNodes, nodeExcess{node, -diff})
		}
	}

	var moves []chunkMove
	for _, over := range overNodes {
		toMove := over.excess
		var nodeChunks []int
		for i, node := range chunkToNode {
			if node == over.node {
				nodeChunks = append(nodeChunks, i)
			}
		}
		moved := 0
		for _, idx := range nodeChunks {
			if moved >= toMove {
				break
			}
			for j := range underNodes {
				if underNodes[j].excess > 0 {
					moves = append(moves, chunkMove{
						chunkIndex: idx,
						chunk:      chunks[idx],
						fromNode:   over.node,
						toNode:     underNodes[j].node,
					})
					underNodes[j].excess--
					moved++
					break
				}
			}
		}
	}
	return moves
}
