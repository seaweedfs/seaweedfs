package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"

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

type commandFsDistributeChunks struct {
}

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

	// ===== 1. Look up the file entry =====
	path, err := commandEnv.parseUrl(*filePath)
	if err != nil {
		return err
	}
	dir, name := util.FullPath(path).DirAndName()

	var entry *filer_pb.Entry
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		return fmt.Errorf("lookup %s: %v", path, err)
	}
	if entry.IsDirectory {
		return fmt.Errorf("%s is a directory", path)
	}

	chunks := entry.GetChunks()
	if len(chunks) == 0 {
		fmt.Fprintf(writer, "File has no chunks (possibly stored inline)\n")
		return nil
	}

	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			return fmt.Errorf("file uses chunk manifests (very large file). Not yet supported")
		}
	}

	// ===== 2. Collect topology =====
	topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return fmt.Errorf("collect topology: %v", err)
	}

	volumeInfoMap := make(map[uint32]*master_pb.VolumeInformationMessage)
	volumeNodesList := make(map[uint32][]string)
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

	volumeToOwner := make(map[uint32]string)
	for vid, nodes := range volumeNodesList {
		sort.Strings(nodes)
		volumeToOwner[vid] = nodes[int(vid)%len(nodes)]
	}

	nodeList := make([]string, 0, len(allNodes))
	for node := range allNodes {
		nodeList = append(nodeList, node)
	}
	sort.Strings(nodeList)

	if len(nodeList) < 2 {
		return fmt.Errorf("need at least 2 volume nodes, found %d", len(nodeList))
	}

	if *targetNodeCount < 0 {
		return fmt.Errorf("-nodes must be >= 0 (0 = all nodes)")
	}
	if *targetNodeCount > len(nodeList) {
		return fmt.Errorf("-nodes=%d exceeds available nodes (%d)", *targetNodeCount, len(nodeList))
	}

	// ===== 3. Build distribution counts =====
	chunkToNode := make(map[int]string)
	ownerCount := make(map[string]int)
	copiesCount := make(map[string]int)

	for i, chunk := range chunks {
		var vid uint32
		if chunk.Fid == nil {
			fileId, parseErr := needle.ParseFileIdFromString(chunk.GetFileIdString())
			if parseErr != nil {
				return fmt.Errorf("failed to parse file id for chunk %d: %v", i, parseErr)
			}
			vid = uint32(fileId.VolumeId)
		} else {
			vid = chunk.Fid.GetVolumeId()
		}
		owner, ok := volumeToOwner[vid]
		if !ok {
			return fmt.Errorf("volume %d not found in topology", vid)
		}
		chunkToNode[i] = owner
		ownerCount[owner]++
		for _, node := range volumeNodesList[vid] {
			copiesCount[node]++
		}
	}

	// Select target nodes
	activeNodeList := nodeList
	if *targetNodeCount > 0 && *targetNodeCount < len(nodeList) {
		var withChunks, withoutChunks []string
		for _, node := range nodeList {
			if ownerCount[node] > 0 {
				withChunks = append(withChunks, node)
			} else {
				withoutChunks = append(withoutChunks, node)
			}
		}
		n := *targetNodeCount
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

	activeSet := make(map[string]bool)
	for _, node := range activeNodeList {
		activeSet[node] = true
	}

	totalChunks := len(chunks)
	totalNodes := len(activeNodeList)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	// ===== 4. Print current distribution =====
	fmt.Fprintf(writer, "\nFile: %s\n", path)
	fmt.Fprintf(writer, "Total chunks: %d, Total nodes: %d, Mode: %s\n", totalChunks, totalNodes, *mode)

	fmt.Fprintf(writer, "\nCurrent distribution:\n")
	maxOwner := 0
	for _, cnt := range ownerCount {
		if cnt > maxOwner {
			maxOwner = cnt
		}
	}
	for _, node := range nodeList {
		oc := ownerCount[node]
		cc := copiesCount[node]
		bar := strings.Repeat("#", int(math.Ceil(float64(oc)*30/math.Max(float64(maxOwner), 1))))
		shortNode := strings.Split(node, ".")[0]
		marker := " "
		if *targetNodeCount > 0 && !activeSet[node] {
			marker = "-"
		}
		if *mode == "replica" {
			fmt.Fprintf(writer, "  %s %-20s %4d chunks  %4d copies  %-30s\n", marker, shortNode, oc, cc, bar)
		} else {
			fmt.Fprintf(writer, "  %s %-20s %4d chunks  %-30s\n", marker, shortNode, oc, bar)
		}
	}
	fmt.Fprintf(writer, "  Ideal: ~%.0f chunks per node", math.Ceil(float64(totalChunks)/float64(totalNodes)))
	if *mode == "replica" {
		fmt.Fprintf(writer, ", ~%.0f copies per node", math.Ceil(float64(totalCopies)/float64(totalNodes)))
	}
	fmt.Fprintln(writer)

	// ===== 5. Calculate redistribution plan (mode-dependent) =====
	var moves []chunkMove

	type nodeExcess struct {
		node   string
		excess int
	}

	allRelevantNodes := make([]string, 0, len(nodeList))
	for _, node := range nodeList {
		if activeSet[node] || ownerCount[node] > 0 {
			allRelevantNodes = append(allRelevantNodes, node)
		}
	}

	ownerTarget := make(map[string]int)

	switch *mode {
	case "primary":
		// Balance owner-based chunk count
		ownerMin := totalChunks / totalNodes
		ownerRemainder := totalChunks % totalNodes
		for i, node := range activeNodeList {
			if i < ownerRemainder {
				ownerTarget[node] = ownerMin + 1
			} else {
				ownerTarget[node] = ownerMin
			}
		}
		for _, node := range nodeList {
			if !activeSet[node] && ownerCount[node] > 0 {
				ownerTarget[node] = 0
			}
		}
		moves = planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, allRelevantNodes)

	case "replica":
		// Step 1: owner balance
		ownerMin := totalChunks / totalNodes
		ownerRemainder := totalChunks % totalNodes
		for i, node := range activeNodeList {
			if i < ownerRemainder {
				ownerTarget[node] = ownerMin + 1
			} else {
				ownerTarget[node] = ownerMin
			}
		}
		for _, node := range nodeList {
			if !activeSet[node] && ownerCount[node] > 0 {
				ownerTarget[node] = 0
			}
		}
		moves = planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, allRelevantNodes)

		// Step 2: copies balance (additional moves)
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

		var copiesOver, copiesUnder []nodeExcess
		for _, node := range activeNodeList {
			diff := copiesCount[node] - copiesTarget[node]
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
				vid := chunk.Fid.GetVolumeId()
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
							copiesCount[srcNode]--
							for k := range copiesOver {
								if copiesOver[k].node == srcNode {
									copiesOver[k].excess--
								}
							}
						}
						copiesUnder[j].excess--
						copiesCount[copiesUnder[j].node]++
						over.excess--
						break
					}
				}
			}
		}

	case "round-robin":
		// Sort chunks by offset, assign to nodes in round-robin order
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
			ownerTarget[targetNode]++ // build target for display
			if currentOwner != targetNode {
				moves = append(moves, chunkMove{
					chunkIndex: sc.index,
					chunk:      chunks[sc.index],
					fromNode:   currentOwner,
					toNode:     targetNode,
				})
			}
		}

		// Print round-robin assignment
		fmt.Fprintf(writer, "\nRound-robin assignment (sequential read optimized):\n")
		fmt.Fprintf(writer, "  First assignments: ")
		for pos := range sortedChunks {
			if pos >= 12 {
				fmt.Fprintf(writer, "... ")
				break
			}
			targetNode := activeNodeList[pos%totalNodes]
			fmt.Fprintf(writer, "%s ", strings.Split(targetNode, ".")[0])
		}
		fmt.Fprintf(writer, "\n  Pattern: ")
		for i := 0; i < totalNodes && i < 6; i++ {
			fmt.Fprintf(writer, "%s ", strings.Split(activeNodeList[i], ".")[0])
		}
		if totalNodes > 6 {
			fmt.Fprintf(writer, "...")
		}
		fmt.Fprintf(writer, "(repeating)\n")
	}

	if len(moves) == 0 {
		fmt.Fprintf(writer, "\nAlready well distributed. No moves needed.\n")
		return nil
	}

	// ===== 6. Print redistribution plan =====
	fmt.Fprintf(writer, "\nRedistribution plan: %d chunks to move\n", len(moves))
	for _, node := range allRelevantNodes {
		current := ownerCount[node]
		target := ownerTarget[node]
		diff := target - current
		sign := ""
		if diff > 0 {
			sign = "+"
		}
		shortNode := strings.Split(node, ".")[0]
		fmt.Fprintf(writer, "  %-25s %4d -> %4d  (%s%d)\n", shortNode, current, target, sign, diff)
	}

	if !*apply {
		fmt.Fprintf(writer, "\nTo apply, add -apply flag\n")
		return nil
	}

	// ===== 7. Execute moves =====
	fmt.Fprintf(writer, "\nExecuting redistribution...\n")

	defer util_http.GetGlobalHttpClient().CloseIdleConnections()

	type movedChunkRecord struct {
		oldFidStr string
		oldVid    needle.VolumeId
	}
	var movedRecords []movedChunkRecord

	uploader, err := operation.NewUploader()
	if err != nil {
		return fmt.Errorf("create uploader: %v", err)
	}

	for i, mv := range moves {
		chunk := mv.chunk
		oldFidStr := chunk.GetFileIdString()

		fmt.Fprintf(writer, "  [%d/%d] Moving %s from %s to %s ...",
			i+1, len(moves),
			oldFidStr,
			strings.Split(mv.fromNode, ".")[0],
			strings.Split(mv.toNode, ".")[0],
		)

		oldFid, parseErr := needle.ParseFileIdFromString(oldFidStr)
		if parseErr != nil {
			fmt.Fprintf(writer, " FAILED (parse fid: %v)\n", parseErr)
			continue
		}

		downloadURLs, lookupErr := commandEnv.MasterClient.LookupVolumeServerUrl(oldFid.VolumeId.String())
		if lookupErr != nil || len(downloadURLs) == 0 {
			fmt.Fprintf(writer, " FAILED (lookup source: %v)\n", lookupErr)
			continue
		}
		downloadURL := fmt.Sprintf("http://%s/%s", downloadURLs[0], oldFidStr)

		resp, reader, readErr := readUrl(downloadURL)
		if readErr != nil {
			fmt.Fprintf(writer, " FAILED (download: %v)\n", readErr)
			continue
		}

		contentType := resp.Header.Get("Content-Type")
		isCompressed := resp.Header.Get("Content-Encoding") == "gzip"
		md5Hash := resp.Header.Get("Content-MD5")
		var filename string
		if cd := resp.Header.Get("Content-Disposition"); len(cd) > 0 {
			if before, after, found := strings.Cut(cd, "filename="); found {
				_ = before
				filename = strings.Trim(after, "\"")
			}
		}

		var replication, collection string
		if vInfo, ok := volumeInfoMap[chunk.Fid.GetVolumeId()]; ok {
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
			reader.Close()
			util_http.CloseResponse(resp)
			fmt.Fprintf(writer, " FAILED (assign: %v)\n", assignErr)
			continue
		}

		uploadURL := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

		v := util.GetViper()
		signingKey := v.GetString("jwt.signing.key")
		var jwt security.EncodedJwt
		if signingKey != "" {
			expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
			jwt = security.GenJwtForVolumeServer(security.SigningKey(signingKey), expiresAfterSec, assignResult.Fid)
		}

		_, uploadErr, _ := uploader.Upload(context.Background(), reader, &operation.UploadOption{
			UploadUrl:         uploadURL,
			Filename:          filename,
			IsInputCompressed: isCompressed,
			Cipher:            false,
			MimeType:          contentType,
			Md5:               md5Hash,
			Jwt:               jwt,
		})
		reader.Close()
		util_http.CloseResponse(resp)

		if uploadErr != nil {
			fmt.Fprintf(writer, " FAILED (upload: %v)\n", uploadErr)
			continue
		}

		newFid, fidParseErr := needle.ParseFileIdFromString(assignResult.Fid)
		if fidParseErr != nil {
			fmt.Fprintf(writer, " FAILED (parse new fid: %v)\n", fidParseErr)
			continue
		}

		movedRecords = append(movedRecords, movedChunkRecord{
			oldFidStr: oldFidStr,
			oldVid:    oldFid.VolumeId,
		})

		chunk.Fid = &filer_pb.FileId{
			VolumeId: uint32(newFid.VolumeId),
			FileKey:  uint64(newFid.Key),
			Cookie:   uint32(newFid.Cookie),
		}
		chunk.FileId = ""

		fmt.Fprintf(writer, " OK -> %s\n", assignResult.Fid)
	}

	if len(movedRecords) == 0 {
		fmt.Fprintf(writer, "\nNo chunks were moved successfully.\n")
		return nil
	}

	// ===== 8. Update filer metadata =====
	fmt.Fprintf(writer, "\nUpdating filer metadata (%d chunks moved)...\n", len(movedRecords))
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

	// ===== 9. Delete old chunks =====
	// Send DELETE to only ONE volume server per chunk.
	// SeaweedFS's ReplicatedDelete propagates the deletion to all replicas automatically.
	fmt.Fprintf(writer, "\nDeleting old chunks from source volumes...\n")
	deleteFailCount := 0
	for _, rec := range movedRecords {
		serverURLs, lookupErr := commandEnv.MasterClient.LookupVolumeServerUrl(rec.oldVid.String())
		if lookupErr != nil || len(serverURLs) == 0 {
			fmt.Fprintf(writer, "  WARNING: cannot lookup volume server for %s: %v\n", rec.oldFidStr, lookupErr)
			deleteFailCount++
			continue
		}
		deleteURL := fmt.Sprintf("http://%s/%s", serverURLs[0], rec.oldFidStr)
		delReq, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
		delResp, delErr := util_http.GetGlobalHttpClient().Do(delReq)
		if delErr != nil {
			fmt.Fprintf(writer, "  WARNING: failed to delete %s: %v\n", rec.oldFidStr, delErr)
			deleteFailCount++
			continue
		}
		delResp.Body.Close()
		if delResp.StatusCode >= 400 {
			fmt.Fprintf(writer, "  WARNING: delete %s returned %s\n", rec.oldFidStr, delResp.Status)
			deleteFailCount++
		}
	}
	if deleteFailCount > 0 {
		fmt.Fprintf(writer, "%d old chunks failed to delete (orphaned on volume servers)\n", deleteFailCount)
	} else {
		fmt.Fprintf(writer, "All old chunks deleted successfully.\n")
	}

	fmt.Fprintf(writer, "\nRedistribution complete. %d chunks moved.\n", len(movedRecords))
	return nil
}

// planOwnerMoves generates moves to balance owner-based chunk distribution
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
