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
		nodeid_hash = uint32(snowflakeId)
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
