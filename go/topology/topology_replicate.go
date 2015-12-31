package topology

import (
	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/storage"
)

func (t *Topology) CheckReplicate() int {
	glog.V(0).Infoln("Start replicate checker on demand")
	for _, col := range t.collectionMap.Items {
		c := col.(*Collection)
		glog.V(0).Infoln("checking replicate on collection:", c.Name)
		for _, vl := range c.storageType2VolumeLayout.Items {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				copyCount := volumeLayout.rp.GetCopyCount()
				for vid, locationList := range volumeLayout.vid2location {
					if locationList.Length() < copyCount {
						//set volume readonly
						glog.V(0).Infoln("replicate volume :", vid)
						SetVolumeReadonly(locationList, vid.String(), true)

					}
				}
			}
		}
	}
	return 0
}

func (t *Topology) doReplicate(vl *VolumeLayout, vid storage.VolumeId) {
	locationList := vl.vid2location[vid]
	if !SetVolumeReadonly(locationList, vid.String(), true) {
		return
	}
	defer SetVolumeReadonly(locationList, vid.String(), false)

}
