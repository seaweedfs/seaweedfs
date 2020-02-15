package weed_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) HandleTcpConnection(conn net.Conn) error {

	// println("handle tcp conn", conn.RemoteAddr())
	tcpMessage := &volume_server_pb.TcpRequestHeader{}
	if err := util.ReadMessage(conn, tcpMessage); err != nil {
		return fmt.Errorf("read message: %v", err)
	}

	if tcpMessage.Get != nil {
		vs.handleFileGet(conn, tcpMessage.Get)
	}

	err := util.WriteMessageEOF(conn)
	// println("processed", tcpMessage.Get.FileId)
	return err
}

func (vs *VolumeServer) handleFileGet(conn net.Conn, req *volume_server_pb.FileGetRequest) error {

	headResponse := &volume_server_pb.FileGetResponse{}
	n := new(needle.Needle)

	commaIndex := strings.LastIndex(req.FileId, ",")
	vid := req.FileId[:commaIndex]
	fid := req.FileId[commaIndex+1:]

	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		headResponse.ErrorCode = http.StatusBadRequest
		return util.WriteMessage(conn, headResponse)
	}
	err = n.ParsePath(fid)
	if err != nil {
		headResponse.ErrorCode = http.StatusBadRequest
		return util.WriteMessage(conn, headResponse)
	}

	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if !hasVolume && !hasEcVolume {
		headResponse.ErrorCode = http.StatusMovedPermanently
		return util.WriteMessage(conn, headResponse)
	}

	cookie := n.Cookie
	var count int
	if hasVolume {
		count, err = vs.store.ReadVolumeNeedle(volumeId, n)
	} else if hasEcVolume {
		count, err = vs.store.ReadEcShardNeedle(context.Background(), volumeId, n)
	}

	if err != nil || count < 0 {
		headResponse.ErrorCode = http.StatusNotFound
		return util.WriteMessage(conn, headResponse)
	}
	if n.Cookie != cookie {
		headResponse.ErrorCode = http.StatusNotFound
		return util.WriteMessage(conn, headResponse)
	}

	if n.LastModified != 0 {
		headResponse.LastModified = n.LastModified
	}

	headResponse.Etag = n.Etag()

	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		headResponse.Headers = pairMap
	}

	/*
		// skip this, no redirection
		if vs.tryHandleChunkedFile(n, filename, w, r) {
			return
		}
	*/

	if n.NameSize > 0 {
		headResponse.Filename = string(n.Name)
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}
	headResponse.ContentType = mtype

	headResponse.IsGzipped = n.IsGzipped()

	if n.IsGzipped() && req.AcceptGzip {
		if n.Data, err = util.UnGzipData(n.Data); err != nil {
			glog.V(0).Infof("ungzip %s error: %v", req.FileId, err)
		}
	}

	headResponse.ContentLength = uint32(len(n.Data))
	bytesToRead := len(n.Data)
	bytesRead := 0

	t := headResponse

	for bytesRead < bytesToRead {

		stopIndex := bytesRead + BufferSizeLimit
		if stopIndex > bytesToRead {
			stopIndex = bytesToRead
		}

		if t == nil {
			t = &volume_server_pb.FileGetResponse{}
		}
		t.Data = n.Data[bytesRead:stopIndex]

		err = util.WriteMessage(conn, t)
		t = nil
		if err != nil {
			return err
		}

		bytesRead = stopIndex
	}

	return nil

}
