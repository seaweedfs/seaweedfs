package sequence

import (
	"fmt"
	"hash/fnv"

	"github.com/bwmarrin/snowflake"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// a simple snowflake Sequencer
type SnowflakeSequencer struct {
	node *snowflake.Node
}

func NewSnowflakeSequencer(nodeid string, snowflakeId int) (*SnowflakeSequencer, error) {
	nodeid_hash := hash(nodeid) & 0x3ff
	if snowflakeId != 0 {
		// Mask to 10 bits to match the snowflake library's node-id range and
		// avoid a wide int → uint32 conversion when snowflakeId is sourced
		// from user input such as an environment variable.
		nodeid_hash = uint32(snowflakeId & 0x3ff)
	}
	glog.V(0).Infof("use snowflake seq id generator, nodeid:%s hex_of_nodeid: %x", nodeid, nodeid_hash)
	node, err := snowflake.NewNode(int64(nodeid_hash))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	sequencer := &SnowflakeSequencer{node: node}
	return sequencer, nil
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (m *SnowflakeSequencer) NextFileId(count uint64) uint64 {
	return uint64(m.node.Generate().Int64())
}

// ignore setmax as we are snowflake
func (m *SnowflakeSequencer) SetMax(seenValue uint64) {
}
