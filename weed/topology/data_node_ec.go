package topology

import (
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
)

func (dn *DataNode) GetEcShards() (ret []erasure_coding.EcVolumeInfo) {
	dn.RLock()
	for _, ecVolumeInfo := range dn.ecShards {
		ret = append(ret, ecVolumeInfo)
	}
	dn.RUnlock()
	return ret
}
