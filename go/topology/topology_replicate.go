package topology

import "github.com/chrislusf/seaweedfs/go/glog"

func (t *Topology) Replicate(garbageThreshold string) int {
	glog.V(0).Infoln("Start replicate on demand")
	for _, col := range t.collectionMap.Items {
		c := col.(*Collection)
		glog.V(0).Infoln("replicate on collection:", c.Name)
		for _, vl := range c.storageType2VolumeLayout.Items {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				copyCount := volumeLayout.rp.GetCopyCount()
				for vid, locationList := range volumeLayout.vid2location {
					if locationList.Length() < copyCount {
						//set volume readonly
						glog.V(0).Infoln("replicate volume :", vid)

					}
				}
			}
		}
	}
	return 0
}
