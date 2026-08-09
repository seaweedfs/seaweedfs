package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// maxTrackedLocations is how many replicas of one volume can carry per-node
// state. Beyond that the flags are simply not recorded, which loses a read-only
// or oversized marking rather than misreporting one.
const maxTrackedLocations = 32

type VolumeLocationList struct {
	list []*DataNode
	// readOnly and oversized mirror list by index, so the per-node state that
	// used to need an index of its own rides along with the location it
	// describes. Both are only ever asked whether any node reports them.
	readOnly  uint32
	oversized uint32
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (dnll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", dnll.list)
}

func (dnll *VolumeLocationList) Copy() *VolumeLocationList {
	list := make([]*DataNode, len(dnll.list))
	copy(list, dnll.list)
	return &VolumeLocationList{
		list:      list,
		readOnly:  dnll.readOnly,
		oversized: dnll.oversized,
	}
}

func (dnll *VolumeLocationList) indexOf(loc *DataNode) int {
	for i, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			return i
		}
	}
	return -1
}

func setFlag(flags *uint32, index int, on bool) {
	if index < 0 || index >= maxTrackedLocations {
		return
	}
	if on {
		*flags |= 1 << uint(index)
	} else {
		*flags &^= 1 << uint(index)
	}
}

func hasFlag(flags uint32, index int) bool {
	return index >= 0 && index < maxTrackedLocations && flags&(1<<uint(index)) != 0
}

// removeFlag drops the bit at index and closes the gap, keeping the flags
// aligned with the locations after one is removed.
func removeFlag(flags *uint32, index int) {
	if index < 0 || index >= maxTrackedLocations {
		return
	}
	low := *flags & (1<<uint(index) - 1)
	high := *flags >> uint(index+1) << uint(index)
	*flags = low | high
}

// SetReadOnly records whether loc reports the volume read-only.
func (dnll *VolumeLocationList) SetReadOnly(loc *DataNode, readOnly bool) {
	setFlag(&dnll.readOnly, dnll.indexOf(loc), readOnly)
}

// AnyReadOnly reports whether any location has the volume read-only.
func (dnll *VolumeLocationList) AnyReadOnly() bool {
	return dnll != nil && dnll.readOnly != 0
}

// SetOversized records whether loc reports the volume past the size limit.
func (dnll *VolumeLocationList) SetOversized(loc *DataNode, oversized bool) {
	setFlag(&dnll.oversized, dnll.indexOf(loc), oversized)
}

// AnyOversized reports whether any location has the volume past the size limit.
func (dnll *VolumeLocationList) AnyOversized() bool {
	return dnll != nil && dnll.oversized != 0
}

func (dnll *VolumeLocationList) Head() *DataNode {
	//mark first node as master volume
	if dnll.Length() == 0 {
		return nil
	}
	return dnll.list[0]
}

func (dnll *VolumeLocationList) Rest() []*DataNode {
	//mark first node as master volume
	return dnll.list[1:]
}

func (dnll *VolumeLocationList) Length() int {
	if dnll == nil {
		return 0
	}
	return len(dnll.list)
}

// Set adds loc, or replaces the entry at the same address, returning the node it
// displaced. Two volume servers can share an address -- GetOrCreateDataNode keys
// on the reported id and refuses to merge a new id onto an address an older node
// still claims -- so the displaced node is not necessarily loc, and callers
// tracking which node a volume is reachable through must move their bookkeeping
// off it rather than assume loc already owned the entry.
func (dnll *VolumeLocationList) Set(loc *DataNode) (displaced *DataNode) {
	for i := 0; i < len(dnll.list); i++ {
		if loc.Ip == dnll.list[i].Ip && loc.Port == dnll.list[i].Port {
			displaced = dnll.list[i]
			dnll.list[i] = loc
			return displaced
		}
	}
	dnll.list = append(dnll.list, loc)
	return nil
}

// Remove drops the entry at loc's address and returns the node removed, or nil
// if the volume was not reachable there. As with Set, the removed node is
// matched by address and need not be loc.
func (dnll *VolumeLocationList) Remove(loc *DataNode) (removed *DataNode) {
	for i, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			removeFlag(&dnll.readOnly, i)
			removeFlag(&dnll.oversized, i)
			return dnl
		}
	}
	return nil
}

func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	// The flags index the locations, so dropping some means rebuilding both
	// rather than leaving bits describing whoever moved into their place.
	var l []*DataNode
	var readOnly, oversized uint32
	for i, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			continue
		}
		setFlag(&readOnly, len(l), hasFlag(dnll.readOnly, i))
		setFlag(&oversized, len(l), hasFlag(dnll.oversized, i))
		l = append(l, dnl)
	}
	dnll.list, dnll.readOnly, dnll.oversized = l, readOnly, oversized
}

// Stats returns logic size and count. Both subtractions are clamped: the
// deleted counters are maintained apart from the totals they come off and can
// transiently exceed them.
func (dnll *VolumeLocationList) Stats(vid needle.VolumeId, freshThreshHold int64) (size uint64, fileCount int) {
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			vinfo, err := dnl.GetVolumesById(vid)
			if err == nil {
				if vinfo.Size > vinfo.DeletedByteCount {
					size = vinfo.Size - vinfo.DeletedByteCount
				}
				if vinfo.FileCount > vinfo.DeleteCount {
					fileCount = vinfo.FileCount - vinfo.DeleteCount
				}
				return size, fileCount
			}
		}
	}
	return 0, 0
}
