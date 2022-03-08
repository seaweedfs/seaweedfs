package mount

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"time"
)

func (wfs *WFS) loopCheckQuota() {

	if wfs.option.Quota <= 0 {
		return
	}

	for {

		err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
				DiskType:    string(wfs.option.DiskType),
			}

			resp, err := client.Statistics(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("reading quota usage %v: %v", request, err)
				return err
			}
			glog.V(4).Infof("read quota usage: %+v", resp)

			isOverQuota := int64(resp.UsedSize) > wfs.option.Quota
			if isOverQuota && !wfs.IsOverQuota {
				glog.Warningf("Quota Exceeded! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
			} else if !isOverQuota && wfs.IsOverQuota {
				glog.Warningf("Within quota limit! quota:%d used:%d", wfs.option.Quota, resp.UsedSize)
			}
			wfs.IsOverQuota = isOverQuota

			return nil
		})

		if err != nil {
			glog.Warningf("read quota usage: %v", err)
		}

		time.Sleep(61 * time.Second)
	}

}
