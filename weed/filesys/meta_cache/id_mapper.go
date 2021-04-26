package meta_cache

import (
	"fmt"
	"strconv"
	"strings"
)

type UidGidMapper struct {
	uidMapper *IdMapper
	gidMapper *IdMapper
}

type IdMapper struct {
	localToFiler map[uint32]uint32
	filerToLocal map[uint32]uint32
}

// UidGidMapper translates local uid/gid to filer uid/gid
// The local storage always persists the same as the filer.
// The local->filer translation happens when updating the filer first and later saving to meta_cache.
// And filer->local happens when reading from the meta_cache.
func NewUidGidMapper(uidPairsStr, gidPairStr string) (*UidGidMapper, error) {
	uidMapper, err := newIdMapper(uidPairsStr)
	if err != nil {
		return nil, err
	}
	gidMapper, err := newIdMapper(gidPairStr)
	if err != nil {
		return nil, err
	}

	return &UidGidMapper{
		uidMapper: uidMapper,
		gidMapper: gidMapper,
	}, nil
}

func (m *UidGidMapper) LocalToFiler(uid, gid uint32) (uint32, uint32) {
	return m.uidMapper.LocalToFiler(uid), m.gidMapper.LocalToFiler(gid)
}
func (m *UidGidMapper) FilerToLocal(uid, gid uint32) (uint32, uint32) {
	return m.uidMapper.FilerToLocal(uid), m.gidMapper.FilerToLocal(gid)
}

func (m *IdMapper) LocalToFiler(id uint32) uint32 {
	value, found := m.localToFiler[id]
	if found {
		return value
	}
	return id
}
func (m *IdMapper) FilerToLocal(id uint32) uint32 {
	value, found := m.filerToLocal[id]
	if found {
		return value
	}
	return id
}

func newIdMapper(pairsStr string) (*IdMapper, error) {

	localToFiler, filerToLocal, err := parseUint32Pairs(pairsStr)
	if err != nil {
		return nil, err
	}

	return &IdMapper{
		localToFiler: localToFiler,
		filerToLocal: filerToLocal,
	}, nil

}

func parseUint32Pairs(pairsStr string) (localToFiler, filerToLocal map[uint32]uint32, err error) {

	if pairsStr == "" {
		return
	}

	localToFiler = make(map[uint32]uint32)
	filerToLocal = make(map[uint32]uint32)
	for _, pairStr := range strings.Split(pairsStr, ",") {
		pair := strings.Split(pairStr, ":")
		localUidStr, filerUidStr := pair[0], pair[1]
		localUid, localUidErr := strconv.Atoi(localUidStr)
		if localUidErr != nil {
			err = fmt.Errorf("failed to parse local %s: %v", localUidStr, localUidErr)
			return
		}
		filerUid, filerUidErr := strconv.Atoi(filerUidStr)
		if filerUidErr != nil {
			err = fmt.Errorf("failed to parse remote %s: %v", filerUidStr, filerUidErr)
			return
		}
		localToFiler[uint32(localUid)] = uint32(filerUid)
		filerToLocal[uint32(filerUid)] = uint32(localUid)
	}

	return
}
