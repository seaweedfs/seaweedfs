package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	HARD_LINK_MARKER = '\x01'
)

type HardLinkId []byte // 16 bytes + 1 marker byte

func NewHardLinkId() HardLinkId {
	bytes := append(util.RandomBytes(16), HARD_LINK_MARKER)
	return bytes
}
