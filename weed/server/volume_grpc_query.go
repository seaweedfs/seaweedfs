package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/tidwall/gjson"
)

func (vs *VolumeServer) Query(req *volume_server_pb.QueryRequest, stream volume_server_pb.VolumeServer_QueryServer) error {

	for _, fid := range req.FromFileIds {

		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			glog.V(0).Infof("volume query failed to parse fid %s: %v", fid, err)
			return err
		}

		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)
		n.ParsePath(id_cookie)

		cookie := n.Cookie
		if _, err := vs.store.ReadVolumeNeedle(volumeId, n); err != nil {
			glog.V(0).Infof("volume query failed to read fid %s: %v", fid, err)
			return err
		}

		if n.Cookie != cookie {
			glog.V(0).Infof("volume query failed to read fid cookie %s: %v", fid, err)
			return err
		}

		if req.InputSerialization.CsvInput!=nil{



		}

		if req.InputSerialization.JsonInput!=nil{

			err = stream.Send(&volume_server_pb.QueriedStripe{
				Records:nil,
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}
			gjson.ForEachLine(string(n.Data), func(line gjson.Result) bool{
				println(line.String())
				return true
			})
		}



	}

	return nil
}
