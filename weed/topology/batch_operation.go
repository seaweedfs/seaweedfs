package topology

import (
	"net/url"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func BatchOperation(locationList *VolumeLocationList, path string, values url.Values) (isSuccess bool) {
	ch := make(chan bool, locationList.Length())
	for _, dn := range locationList.AllDataNode() {
		go func(url string, path string, values url.Values) {
			_, e := util.RemoteApiCall(url, path, values)
			if e != nil {
				glog.V(0).Infoln("RemoteApiCall:", util.MkUrl(url, path, values), "error =", e)
			}
			ch <- e == nil

		}(dn.Url(), path, values)
	}
	isSuccess = true
	for range locationList.AllDataNode() {
		select {
		case canVacuum := <-ch:
			isSuccess = isSuccess && canVacuum
		case <-time.After(30 * time.Minute):
			isSuccess = false
			break
		}
	}
	return isSuccess
}

func SetVolumeReadonly(locationList *VolumeLocationList, volume string, isReadonly bool) (isSuccess bool) {
	forms := url.Values{}
	forms.Set("key", "readonly")
	forms.Set("value", strconv.FormatBool(isReadonly))
	forms.Set("volume", volume)
	return BatchOperation(locationList, "/admin/setting", forms)
}
