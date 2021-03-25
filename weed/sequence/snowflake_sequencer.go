package sequence

import (
	"fmt"
	"hash/fnv"

	"github.com/bwmarrin/snowflake"
)

// a simple snowflake Sequencer
type SnowflakeSequencer struct {
	node *snowflake.Node
}

func NewSnowflakeSequencer(nodeid string) (*SnowflakeSequencer, error) {
	node, err := snowflake.NewNode(int64(hash(nodeid) & 0x3ff))
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

// return a new id as no Peek is stored
func (m *SnowflakeSequencer) Peek() uint64 {
	return uint64(m.node.Generate().Int64())
}
