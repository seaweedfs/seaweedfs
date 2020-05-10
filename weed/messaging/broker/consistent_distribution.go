package broker

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func PickMember(members []string, key []byte) string {
	cfg := consistent.Config{
		PartitionCount:    9791,
		ReplicationFactor: 2,
		Load:              1.25,
		Hasher:            hasher{},
	}

	cmembers := []consistent.Member{}
	for _, m := range members {
		cmembers = append(cmembers, Member(m))
	}

	c := consistent.New(cmembers, cfg)

	m := c.LocateKey(key)

	return m.String()
}
