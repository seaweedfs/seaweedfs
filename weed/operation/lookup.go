package operation

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type Location struct {
	Url               string `json:"url,omitempty"`
	PublicUrl         string `json:"publicUrl,omitempty"`
	DataCenter        string `json:"dataCenter,omitempty"`
	GrpcPort          int    `json:"grpcPort,omitempty"`
	DataInRemote      bool   `json:"dataInRemote,omitempty"`
	ReadOnly          bool   `json:"readOnly,omitempty"`
	ReadOnlyCanDelete bool   `json:"readOnlyCanDelete,omitempty"`
}

func (l *Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

type LookupResult struct {
	VolumeOrFileId string     `json:"volumeOrFileId,omitempty"`
	Locations      []Location `json:"locations,omitempty"`
	Jwt            string     `json:"jwt,omitempty"`
	Error          string     `json:"error,omitempty"`
	NotFound       bool       `json:"-"`
}

func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeOrFileId:%s, Locations:%v, Error:%s", lr.VolumeOrFileId, lr.Locations, lr.Error)
}

var (
	vc VidCache // caching of volume locations, re-check if after 10 minutes
)

// LookupFileId resolves a "<vid>,<cookie>" file id to one HTTP read URL,
// preferring a volume server whose replica holds the data locally over one
// backed by remote-tier storage. If no local replica is known the function
// falls back to a random remote replica. The returned jwt is the read
// authorization the master stamped on the volume; pass it through to the
// volume server on the read request.
func LookupFileId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (fullUrl string, jwt string, err error) {
	fullUrls, jwt, err := lookupFileIdUrls(masterFn, grpcDialOption, fileId)
	if err != nil {
		return "", jwt, err
	}
	return fullUrls[0], jwt, nil
}

// lookupFileIdUrls returns the read URLs of every replica of fileId, local
// replicas first, each group in random order.
func lookupFileIdUrls(masterFn GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (fullUrls []string, jwt string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, jwt, errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := LookupVolumeId(masterFn, grpcDialOption, parts[0])
	if lookupError != nil {
		return nil, jwt, lookupError
	}
	if len(lookup.Locations) == 0 {
		return nil, jwt, errors.New("File Not Found")
	}

	var localUrls, remoteUrls []string
	for _, loc := range lookup.Locations {
		fullUrl := "http://" + loc.Url + "/" + fileId
		if loc.DataInRemote {
			remoteUrls = append(remoteUrls, fullUrl)
		} else {
			localUrls = append(localUrls, fullUrl)
		}
	}
	rand.Shuffle(len(localUrls), func(i, j int) { localUrls[i], localUrls[j] = localUrls[j], localUrls[i] })
	rand.Shuffle(len(remoteUrls), func(i, j int) { remoteUrls[i], remoteUrls[j] = remoteUrls[j], remoteUrls[i] })

	return append(localUrls, remoteUrls...), lookup.Jwt, nil
}

func LookupVolumeId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vid string) (*LookupResult, error) {
	return LookupVolumeIdWithContext(context.Background(), masterFn, grpcDialOption, vid)
}

func LookupVolumeIdWithContext(ctx context.Context, masterFn GetMasterFn, grpcDialOption grpc.DialOption, vid string) (*LookupResult, error) {
	results, err := LookupVolumeIdsWithContext(ctx, masterFn, grpcDialOption, []string{vid})
	return results[vid], err
}

// InvalidateVolumeIdLocationCache drops cached locations for vid so the next lookup re-queries the master.
func InvalidateVolumeIdLocationCache(vid string) {
	vc.Delete(vid)
}

// LookupVolumeIds find volume locations by cache and actual lookup
func LookupVolumeIds(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vids []string, useCache ...bool) (map[string]*LookupResult, error) {
	return LookupVolumeIdsWithContext(context.Background(), masterFn, grpcDialOption, vids, useCache...)
}

// LookupVolumeIdsWithContext is LookupVolumeIds bound to ctx, so a caller's
// cancellation (e.g. a dropped inbound request) also stops the master query.
func LookupVolumeIdsWithContext(ctx context.Context, masterFn GetMasterFn, grpcDialOption grpc.DialOption, vids []string, useCache ...bool) (map[string]*LookupResult, error) {
	ret := make(map[string]*LookupResult)
	var unknown_vids []string
	cacheLocations := len(useCache) == 0 || useCache[0]

	//check vid cache first
	for _, vid := range vids {
		if cacheLocations {
			locations, cacheErr := vc.Get(vid)
			if cacheErr == nil {
				ret[vid] = &LookupResult{VolumeOrFileId: vid, Locations: locations}
				continue
			}
		}
		unknown_vids = append(unknown_vids, vid)
	}
	//return success if all volume ids are known
	if len(unknown_vids) == 0 {
		return ret, nil
	}

	//only query unknown_vids

	err := WithMasterServerClient(ctx, false, masterFn(ctx), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		req := &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: unknown_vids,
		}
		resp, grpcErr := masterClient.LookupVolume(ctx, req)
		if grpcErr != nil {
			return grpcErr
		}

		//set newly checked vids to cache
		for _, vidLocations := range resp.VolumeIdLocations {
			var locations []Location
			for _, loc := range vidLocations.Locations {
				locations = append(locations, Location{
					Url:               loc.Url,
					PublicUrl:         loc.PublicUrl,
					DataCenter:        loc.DataCenter,
					GrpcPort:          int(loc.GrpcPort),
					DataInRemote:      loc.DataInRemote,
					ReadOnly:          loc.ReadOnly,
					ReadOnlyCanDelete: loc.ReadOnlyCanDelete,
				})
			}
			if cacheLocations && vidLocations.Error == "" {
				vc.Set(vidLocations.VolumeOrFileId, locations, 10*time.Minute)
			}
			ret[vidLocations.VolumeOrFileId] = &LookupResult{
				VolumeOrFileId: vidLocations.VolumeOrFileId,
				Locations:      locations,
				Jwt:            vidLocations.Auth,
				Error:          vidLocations.Error,
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
