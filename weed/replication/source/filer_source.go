package source

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

// ErrVolumeNotFound reports that the source cluster has no location for a
// chunk's volume: vacuumed away, deleted, or every replica offline. Callers
// need it apart from a lookup that simply failed, which a later attempt can
// still get past.
var ErrVolumeNotFound = errors.New("volume not found")

type FilerSource struct {
	grpcAddress    string
	grpcDialOption grpc.DialOption
	Dir            string
	address        string
	proxyByFiler   bool
	dataCenter     string
	signature      int32
	httpClient     *util_http_client.HTTPClient
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

func (fs *FilerSource) SetGrpcDialOption(option grpc.DialOption) {
	fs.grpcDialOption = option
}

func (fs *FilerSource) SetHttpClient(client *util_http_client.HTTPClient) {
	fs.httpClient = client
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
		glog.V(1).InfofCtx(ctx, "LookupFileId volume id %s: %v", vid, err)
		return nil, fmt.Errorf("LookupFileId volume id %s: %v", vid, err)
	}

	locations := vid2Locations[vid]

	if locations == nil || len(locations.Locations) == 0 {
		glog.V(1).InfofCtx(ctx, "LookupFileId locate volume id %s: %v", vid, ErrVolumeNotFound)
		return nil, fmt.Errorf("LookupFileId locate volume id %s: %w", vid, ErrVolumeNotFound)
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

func (fs *FilerSource) ReadPart(fileId string, offset int64) (filename string, header http.Header, resp *http.Response, err error) {
	downloadFn := util_http.DownloadFile
	if fs.httpClient != nil {
		downloadFn = func(fileUrl string, jwt string, offset ...int64) (string, http.Header, *http.Response, error) {
			return util_http.DownloadFileWithClient(fs.httpClient, fileUrl, jwt, offset...)
		}
	}

	if fs.proxyByFiler {
		fileUrl := "http://" + fs.address + "/?proxyChunkId=" + fileId
		filename, header, resp, err = downloadFn(fileUrl, "", offset)
		if err == nil {
			err = readPartStatusError(fileUrl, resp)
		}
		if err != nil {
			glog.V(0).Infof("read part %s via filer proxy %s offset %d: %v", fileId, fs.address, offset, err)
			return "", nil, nil, err
		}
		glog.V(4).Infof("read part %s via filer proxy %s offset %d content-length:%s", fileId, fs.address, offset, header.Get("Content-Length"))
		return
	}

	fileUrls, err := fs.LookupFileId(context.Background(), fileId)
	if err != nil {
		return "", nil, nil, err
	}

	for _, fileUrl := range fileUrls {
		filename, header, resp, err = downloadFn(fileUrl, "", offset)
		if err == nil {
			err = readPartStatusError(fileUrl, resp)
		}
		if err != nil {
			resp = nil
			glog.V(0).Infof("fail to read part %s from %s offset %d: %v", fileId, fileUrl, offset, err)
			continue
		}
		glog.V(4).Infof("read part %s from %s offset %d content-length:%s", fileId, fileUrl, offset, header.Get("Content-Length"))
		break
	}

	return filename, header, resp, err
}

// readPartStatusError turns a failure status into an error and closes the
// response. Otherwise the caller copies the error page as chunk content and
// reports it as a short read; a needle vacuum has removed answers 404, which
// is the source having lost the data rather than corruption.
func readPartStatusError(fileUrl string, resp *http.Response) error {
	if resp == nil || resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	defer util_http.CloseResponse(resp)
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%s: %s: %w", fileUrl, resp.Status, util_http.ErrNotFound)
	}
	return fmt.Errorf("%s: %s", fileUrl, resp.Status)
}

var _ = filer_pb.FilerClient(&FilerSource{})

func (fs *FilerSource) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(context.Background(), streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
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
