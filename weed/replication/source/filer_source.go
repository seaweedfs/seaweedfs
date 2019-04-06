package source

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type ReplicationSource interface {
	ReadPart(part string) io.ReadCloser
}

type FilerSource struct {
	grpcAddress    string
	grpcDialOption grpc.DialOption
	Dir            string
}

func (fs *FilerSource) Initialize(configuration util.Configuration) error {
	return fs.initialize(
		configuration.GetString("grpcAddress"),
		configuration.GetString("directory"),
	)
}

func (fs *FilerSource) initialize(grpcAddress string, dir string) (err error) {
	fs.grpcAddress = grpcAddress
	fs.Dir = dir
	fs.grpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")
	return nil
}

func (fs *FilerSource) LookupFileId(ctx context.Context, part string) (fileUrl string, err error) {

	vid2Locations := make(map[string]*filer_pb.Locations)

	vid := volumeId(part)

	err = fs.withFilerClient(ctx, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		glog.V(4).Infof("read lookup volume id locations: %v", vid)
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
		return "", fmt.Errorf("LookupFileId volume id %s: %v", vid, err)
	}

	locations := vid2Locations[vid]

	if locations == nil || len(locations.Locations) == 0 {
		glog.V(1).Infof("LookupFileId locate volume id %s: %v", vid, err)
		return "", fmt.Errorf("LookupFileId locate volume id %s: %v", vid, err)
	}

	fileUrl = fmt.Sprintf("http://%s/%s", locations.Locations[0].Url, part)

	return
}

func (fs *FilerSource) ReadPart(ctx context.Context, part string) (filename string, header http.Header, readCloser io.ReadCloser, err error) {

	fileUrl, err := fs.LookupFileId(ctx, part)
	if err != nil {
		return "", nil, nil, err
	}

	filename, header, readCloser, err = util.DownloadFile(fileUrl)

	return filename, header, readCloser, err
}

func (fs *FilerSource) withFilerClient(ctx context.Context, grpcDialOption grpc.DialOption, fn func(filer_pb.SeaweedFilerClient) error) error {

	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, fs.grpcDialOption)

}

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
