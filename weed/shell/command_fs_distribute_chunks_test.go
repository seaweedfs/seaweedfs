package shell

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeChunk builds a FileChunk with the given volume ID, file key, and byte offset.
func makeChunk(vid uint32, key uint64, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{
		Fid:    &filer_pb.FileId{VolumeId: vid, FileKey: key, Cookie: 1},
		Offset: offset,
		Size:   1024 * 1024 * 16,
	}
}

// threeNodeTopology returns a minimal topology for 3 nodes with 3 volumes (one per node).
//
//	node A owns vol 1, node B owns vol 2, node C owns vol 3
//	no replication: each volume exists on exactly one node.
func threeNodeTopology() (volumeToOwner map[uint32]string, volumeNodesList map[uint32][]string) {
	volumeToOwner = map[uint32]string{
		1: "nodeA",
		2: "nodeB",
		3: "nodeC",
	}
	volumeNodesList = map[uint32][]string{
		1: {"nodeA"},
		2: {"nodeB"},
		3: {"nodeC"},
	}
	return volumeToOwner, volumeNodesList
}

// ── shortName ────────────────────────────────────────────────────────────────

func TestShortName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"nodeA.example.com", "nodeA"},
		{"nodeA", "nodeA"},
		{"10.0.0.1", "10"},
		{"", ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, shortName(tt.input), "input=%q", tt.input)
	}
}

// ── relevantNodes ────────────────────────────────────────────────────────────

func TestRelevantNodes(t *testing.T) {
	nodeList := []string{"nodeA", "nodeB", "nodeC", "nodeD"}
	activeSet := map[string]bool{"nodeA": true, "nodeB": true}
	ownerCount := map[string]int{"nodeA": 3, "nodeC": 1} // nodeC has chunks but isn't active

	got := relevantNodes(nodeList, activeSet, ownerCount)

	// nodeA (active+chunks), nodeB (active), nodeC (inactive but has chunks)
	// nodeD (inactive, no chunks) → excluded
	assert.ElementsMatch(t, []string{"nodeA", "nodeB", "nodeC"}, got)
}

// ── computeOwnerTarget ───────────────────────────────────────────────────────

func TestComputeOwnerTarget_EvenDivision(t *testing.T) {
	nodes := []string{"nodeA", "nodeB", "nodeC"}
	activeSet := map[string]bool{"nodeA": true, "nodeB": true, "nodeC": true}

	target := computeOwnerTarget(nodes, nodes, activeSet, 9, 3)

	assert.Equal(t, 3, target["nodeA"])
	assert.Equal(t, 3, target["nodeB"])
	assert.Equal(t, 3, target["nodeC"])
}

func TestComputeOwnerTarget_UnevenDivision(t *testing.T) {
	nodes := []string{"nodeA", "nodeB", "nodeC"}
	activeSet := map[string]bool{"nodeA": true, "nodeB": true, "nodeC": true}

	// 10 chunks / 3 nodes → 3,3,4 (remainder goes to first nodes)
	target := computeOwnerTarget(nodes, nodes, activeSet, 10, 3)

	total := target["nodeA"] + target["nodeB"] + target["nodeC"]
	assert.Equal(t, 10, total)
	for _, node := range nodes {
		assert.True(t, target[node] == 3 || target[node] == 4,
			"node %s has unexpected target %d", node, target[node])
	}
}

func TestComputeOwnerTarget_InactiveNodesDrainToZero(t *testing.T) {
	activeNodes := []string{"nodeA", "nodeB"}
	allNodes := []string{"nodeA", "nodeB", "nodeC"}
	activeSet := map[string]bool{"nodeA": true, "nodeB": true}

	target := computeOwnerTarget(activeNodes, allNodes, activeSet, 4, 2)

	assert.Equal(t, 0, target["nodeC"], "inactive node should have target 0")
	assert.Equal(t, 2, target["nodeA"])
	assert.Equal(t, 2, target["nodeB"])
}

// ── buildDistributionCounts ──────────────────────────────────────────────────

func TestBuildDistributionCounts(t *testing.T) {
	volumeToOwner, volumeNodesList := threeNodeTopology()

	// 4 chunks on vol1 (nodeA), 2 on vol2 (nodeB), 0 on nodeC
	chunks := []*filer_pb.FileChunk{
		makeChunk(1, 1, 0),
		makeChunk(1, 2, 1),
		makeChunk(1, 3, 2),
		makeChunk(1, 4, 3),
		makeChunk(2, 5, 4),
		makeChunk(2, 6, 5),
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)

	require.NoError(t, err)
	assert.Equal(t, 4, ownerCount["nodeA"])
	assert.Equal(t, 2, ownerCount["nodeB"])
	assert.Equal(t, 0, ownerCount["nodeC"])

	// each chunk maps to its owner
	assert.Equal(t, "nodeA", chunkToNode[0])
	assert.Equal(t, "nodeB", chunkToNode[4])

	// no replication → copies == owner counts
	assert.Equal(t, 4, copiesCount["nodeA"])
	assert.Equal(t, 2, copiesCount["nodeB"])
}

func TestBuildDistributionCounts_NilFidFallback(t *testing.T) {
	volumeToOwner := map[uint32]string{3: "nodeA"}
	volumeNodesList := map[uint32][]string{3: {"nodeA"}}

	// chunk with nil Fid but a valid legacy FileId string "3,0123456789abcdef01"
	chunks := []*filer_pb.FileChunk{
		{Fid: nil, FileId: "3,0123456789abcdef01"},
	}

	_, ownerCount, _, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)

	require.NoError(t, err)
	assert.Equal(t, 1, ownerCount["nodeA"])
}

// ── selectActiveNodes ────────────────────────────────────────────────────────

func TestSelectActiveNodes_AllNodes(t *testing.T) {
	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	ownerCount := map[string]int{"nodeA": 5, "nodeB": 3, "nodeC": 2}

	active, activeSet := selectActiveNodes(nodeList, ownerCount, 0)

	assert.Equal(t, nodeList, active)
	assert.Len(t, activeSet, 3)
}

func TestSelectActiveNodes_LimitToTwo(t *testing.T) {
	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	ownerCount := map[string]int{"nodeA": 5, "nodeB": 3, "nodeC": 0}

	active, activeSet := selectActiveNodes(nodeList, ownerCount, 2)

	assert.Len(t, active, 2)
	// nodeA and nodeB have chunks and are the top-2 heaviest
	assert.True(t, activeSet["nodeA"])
	assert.True(t, activeSet["nodeB"])
	assert.False(t, activeSet["nodeC"])
}

// ── planOwnerMoves ───────────────────────────────────────────────────────────

func TestPlanOwnerMoves_BasicBalance(t *testing.T) {
	// nodeA has 5 chunks, nodeB/C have 0; target is 2,2,1 (or similar even split of 5)
	ownerCount := map[string]int{"nodeA": 5, "nodeB": 0, "nodeC": 0}
	ownerTarget := map[string]int{"nodeA": 1, "nodeB": 2, "nodeC": 2}
	nodes := []string{"nodeA", "nodeB", "nodeC"}

	chunkToNode := map[int]string{0: "nodeA", 1: "nodeA", 2: "nodeA", 3: "nodeA", 4: "nodeA"}
	chunks := []*filer_pb.FileChunk{
		makeChunk(1, 1, 0),
		makeChunk(1, 2, 1),
		makeChunk(1, 3, 2),
		makeChunk(1, 4, 3),
		makeChunk(1, 5, 4),
	}

	moves := planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, nodes)

	// 4 chunks must leave nodeA
	assert.Len(t, moves, 4)
	for _, mv := range moves {
		assert.Equal(t, "nodeA", mv.fromNode)
		assert.NotEqual(t, "nodeA", mv.toNode)
	}
}

func TestPlanOwnerMoves_AlreadyBalanced(t *testing.T) {
	ownerCount := map[string]int{"nodeA": 2, "nodeB": 2, "nodeC": 2}
	ownerTarget := map[string]int{"nodeA": 2, "nodeB": 2, "nodeC": 2}
	nodes := []string{"nodeA", "nodeB", "nodeC"}
	chunkToNode := map[int]string{0: "nodeA", 1: "nodeA", 2: "nodeB", 3: "nodeB", 4: "nodeC", 5: "nodeC"}
	chunks := make([]*filer_pb.FileChunk, 6)
	for i := range chunks {
		chunks[i] = makeChunk(1, uint64(i+1), int64(i))
	}

	moves := planOwnerMoves(ownerCount, ownerTarget, chunkToNode, chunks, nodes)

	assert.Empty(t, moves)
}

// ── planDistribution: primary ─────────────────────────────────────────────────

func TestPlanDistribution_Primary_BalancesChunks(t *testing.T) {
	// 9 chunks, all on nodeA; 3 nodes; primary should balance to 3 each
	volumeToOwner, volumeNodesList := threeNodeTopology()
	chunks := make([]*filer_pb.FileChunk, 9)
	for i := range chunks {
		chunks[i] = makeChunk(1, uint64(i+1), int64(i)) // all on vol1 → nodeA
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	require.NoError(t, err)

	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, 0)
	totalChunks := len(chunks)
	totalNodes := len(activeNodeList)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	moves, ownerTarget := planDistribution("primary", &bytes.Buffer{},
		activeNodeList, nodeList, activeSet,
		ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList,
		totalChunks, totalNodes, totalCopies)

	assert.Equal(t, 3, ownerTarget["nodeA"])
	assert.Equal(t, 3, ownerTarget["nodeB"])
	assert.Equal(t, 3, ownerTarget["nodeC"])
	// 6 chunks must be moved off nodeA
	assert.Len(t, moves, 6)
	for _, mv := range moves {
		assert.Equal(t, "nodeA", mv.fromNode)
	}
}

func TestPlanDistribution_Primary_NoMovesWhenBalanced(t *testing.T) {
	volumeToOwner, volumeNodesList := threeNodeTopology()
	// 3 chunks evenly spread: one per volume/node
	chunks := []*filer_pb.FileChunk{
		makeChunk(1, 1, 0),
		makeChunk(2, 2, 1),
		makeChunk(3, 3, 2),
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	require.NoError(t, err)

	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, 0)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	moves, _ := planDistribution("primary", &bytes.Buffer{},
		activeNodeList, nodeList, activeSet,
		ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList,
		len(chunks), len(activeNodeList), totalCopies)

	assert.Empty(t, moves)
}

// ── planDistribution: round-robin ─────────────────────────────────────────────

func TestPlanDistribution_RoundRobin_AssignsByOffset(t *testing.T) {
	// 6 chunks all on nodeA; round-robin over 3 nodes should spread them A,B,C,A,B,C
	volumeToOwner, volumeNodesList := threeNodeTopology()
	chunks := []*filer_pb.FileChunk{
		makeChunk(1, 1, 0),
		makeChunk(1, 2, 1),
		makeChunk(1, 3, 2),
		makeChunk(1, 4, 3),
		makeChunk(1, 5, 4),
		makeChunk(1, 6, 5),
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	require.NoError(t, err)

	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, 0)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	var buf bytes.Buffer
	moves, ownerTarget := planDistribution("round-robin", &buf,
		activeNodeList, nodeList, activeSet,
		ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList,
		len(chunks), len(activeNodeList), totalCopies)

	// 2 chunks stay on nodeA (positions 0,3), 3 moves to nodeB and 3 to nodeC
	assert.Equal(t, 2, ownerTarget["nodeA"])
	assert.Equal(t, 2, ownerTarget["nodeB"])
	assert.Equal(t, 2, ownerTarget["nodeC"])
	// 4 chunks must move (nodeB and nodeC positions)
	assert.Len(t, moves, 4)

	// output should contain the round-robin preview
	assert.Contains(t, buf.String(), "Round-robin assignment")
}

func TestPlanDistribution_RoundRobin_OffsetOrdering(t *testing.T) {
	// chunks deliberately added in reverse offset order to verify sort
	volumeToOwner := map[uint32]string{1: "nodeA", 2: "nodeB", 3: "nodeC"}
	volumeNodesList := map[uint32][]string{1: {"nodeA"}, 2: {"nodeB"}, 3: {"nodeC"}}

	// 3 chunks already balanced but in non-sequential order
	chunks := []*filer_pb.FileChunk{
		makeChunk(3, 3, 200), // highest offset → should be assigned last
		makeChunk(1, 1, 0),   // lowest offset  → assigned to nodeList[0]
		makeChunk(2, 2, 100),
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	require.NoError(t, err)

	nodeList := []string{"nodeA", "nodeB", "nodeC"}
	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, 0)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	_, ownerTarget := planDistribution("round-robin", &bytes.Buffer{},
		activeNodeList, nodeList, activeSet,
		ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList,
		len(chunks), len(activeNodeList), totalCopies)

	// every node gets exactly 1 chunk
	assert.Equal(t, 1, ownerTarget["nodeA"])
	assert.Equal(t, 1, ownerTarget["nodeB"])
	assert.Equal(t, 1, ownerTarget["nodeC"])
}

// ── planDistribution: replica ─────────────────────────────────────────────────

func TestPlanDistribution_Replica_BalancesOwnerAndCopies(t *testing.T) {
	// 2-node replica topology: each volume has copies on both nodeA and nodeB
	volumeToOwner := map[uint32]string{
		1: "nodeA", // vid%2=1 → nodeA
		2: "nodeB", // vid%2=0 → nodeB (sorted: [nodeA,nodeB], idx 2%2=0 → nodeA... let's be explicit)
	}
	// explicit: vol1 primary=nodeA, vol2 primary=nodeB; both replicated on each other
	volumeNodesList := map[uint32][]string{
		1: {"nodeA", "nodeB"},
		2: {"nodeA", "nodeB"},
	}

	// 6 chunks: 5 on vol1 (nodeA), 1 on vol2 (nodeB)
	chunks := []*filer_pb.FileChunk{
		makeChunk(1, 1, 0),
		makeChunk(1, 2, 1),
		makeChunk(1, 3, 2),
		makeChunk(1, 4, 3),
		makeChunk(1, 5, 4),
		makeChunk(2, 6, 5),
	}

	chunkToNode, ownerCount, copiesCount, err := buildDistributionCounts(chunks, volumeToOwner, volumeNodesList)
	require.NoError(t, err)

	nodeList := []string{"nodeA", "nodeB"}
	activeNodeList, activeSet := selectActiveNodes(nodeList, ownerCount, 0)
	totalCopies := 0
	for _, cnt := range copiesCount {
		totalCopies += cnt
	}

	moves, ownerTarget := planDistribution("replica", &bytes.Buffer{},
		activeNodeList, nodeList, activeSet,
		ownerCount, copiesCount, chunkToNode, chunks, volumeNodesList,
		len(chunks), len(activeNodeList), totalCopies)

	// after balancing, nodeA should have 3 and nodeB should have 3
	assert.Equal(t, 3, ownerTarget["nodeA"])
	assert.Equal(t, 3, ownerTarget["nodeB"])
	assert.NotEmpty(t, moves)
	for _, mv := range moves {
		assert.Equal(t, "nodeA", mv.fromNode)
		assert.Equal(t, "nodeB", mv.toNode)
	}
}

// ── printRedistributionPlan ──────────────────────────────────────────────────

func TestPrintRedistributionPlan_Output(t *testing.T) {
	var buf bytes.Buffer
	nodes := []string{"nodeA.local", "nodeB.local"}
	ownerCount := map[string]int{"nodeA.local": 5, "nodeB.local": 1}
	ownerTarget := map[string]int{"nodeA.local": 3, "nodeB.local": 3}

	printRedistributionPlan(&buf, nodes, ownerCount, ownerTarget, 2)

	out := buf.String()
	assert.Contains(t, out, "2 chunks to move")
	assert.Contains(t, out, "nodeA")
	assert.Contains(t, out, "nodeB")
	assert.Contains(t, out, "5 ->")
	assert.Contains(t, out, "1 ->")
}
