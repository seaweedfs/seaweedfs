package source

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type ReplicationSource interface {
	ReadPart(part string) io.ReadCloser
}

type FilerSource struct {
	grpcAddress    string
	grpcDialOption grpc.DialOption
	Dir            string
	address        string
	proxyByFiler   bool
	dataCenter     string
	signature      int32
}

func (fs *FilerSource) Initialize(configuration util.Configuration, prefix string) error {
	fs.dataCenter = configuration.GetString(prefix + "dataCenter")
	fs.signature = util.RandomInt32()
	return fs.DoInitialize(
		"",
		configuration.GetString(prefix+"grpcAddress"),
		configuration.GetString(prefix+"directory"),
		false,
	)
}

func (fs *FilerSource) DoInitialize(address, grpcAddress string, dir string, readChunkFromFiler bool) (err error) {
	fs.address = address
	if fs.address == "" {
		fs.address = pb.GrpcAddressToServerAddress(grpcAddress)
	}
	fs.grpcAddress = grpcAddress
	fs.Dir = dir
	fs.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")
	fs.proxyByFiler = readChunkFromFiler
	return nil
}

func (fs *FilerSource) LookupFileId(ctx context.Context, part string) (fileUrls []string, err error) {

	vid2Locations := make(map[string]*filer_pb.Locations)

	vid := volumeId(part)

	err = fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
			VolumeIds: []string{vid},
		})
		if err != nil {
			return err
		}

		vid2Locations = resp.LocationsMap

		return nil
	})

	if err != nil {
		glog.V(1).Infof("LookupFileId volume id %s: %v", vid, err)
		return nil, fmt.Errorf("LookupFileId volume id %s: %v", vid, err)
	}

	locations := vid2Locations[vid]

	if locations == nil || len(locations.Locations) == 0 {
		glog.V(1).Infof("LookupFileId locate volume id %s: %v", vid, err)
		return nil, fmt.Errorf("LookupFileId locate volume id %s: %v", vid, err)
	}

	if !fs.proxyByFiler {
		for _, loc := range locations.Locations {
			fileUrl := fmt.Sprintf("http://%s/%s?readDeleted=true", loc.Url, part)
			// Prefer same data center
			if fs.dataCenter != "" && fs.dataCenter == loc.DataCenter {
				fileUrls = append([]string{fileUrl}, fileUrls...)
			} else {
				fileUrls = append(fileUrls, fileUrl)
			}
		}
	} else {
		fileUrls = append(fileUrls, fmt.Sprintf("http://%s/?proxyChunkId=%s", fs.address, part))
	}

	return
}

func (fs *FilerSource) ReadPart(fileId string) (filename string, header http.Header, resp *http.Response, err error) {

	if fs.proxyByFiler {
		return util_http.DownloadFile("http://"+fs.address+"/?proxyChunkId="+fileId, "")
	}

	fileUrls, err := fs.LookupFileId(context.Background(), fileId)
	if err != nil {
		return "", nil, nil, err
	}

	for _, fileUrl := range fileUrls {
		filename, header, resp, err = util_http.DownloadFile(fileUrl, "")
		if err != nil {
			glog.V(1).Infof("fail to read from %s: %v", fileUrl, err)
		} else {
			break
		}
	}

	return filename, header, resp, err
}

var _ = filer_pb.FilerClient(&FilerSource{})

func (fs *FilerSource) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, false, fs.grpcDialOption)

}

func (fs *FilerSource) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fs *FilerSource) GetDataCenter() string {
	return fs.dataCenter
}

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
