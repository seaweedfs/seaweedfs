package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/grpc"
)

// ReplicatedWrite writes a needle to the local volume and fans it out to all
// remote replica locations. When type=replicate is set, the request is itself
// a forwarded replication and no further remote lookups are performed.
// Returns isUnchanged=true when the local write determined the needle content
// was already present.
func ReplicatedWrite(ctx context.Context, masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, s *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request, contentMd5 string) (isUnchanged bool, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	// check whether this is a replicated write request
	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		// this is the initial request
		remoteLocations, err = GetWritableRemoteReplications(s, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// read fsync value
	fsync := false
	if r.FormValue("fsync") == "true" {
		fsync = true
	}

	replicaCount := len(remoteLocations)

	if replicaCount > 0 {
		// Record replication duration histogram for the overall write operation
		defer func(t time.Time) {
			stats.VolumeServerReplicationHistogram.WithLabelValues(stats.ReplicationOpWrite).Observe(time.Since(t).Seconds())
		}(time.Now())
	}

	if s.GetVolume(volumeId) != nil {
		start := time.Now()

		inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(stats.WriteToLocalDisk)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

		isUnchanged, err = s.WriteVolumeNeedle(volumeId, n, true, fsync)
		stats.VolumeServerRequestHistogram.WithLabelValues(stats.WriteToLocalDisk).Observe(time.Since(start).Seconds())
		if err != nil {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorWriteToLocalDisk).Inc()
			err = fmt.Errorf("failed to write to local disk: %w", err)
			glog.V(0).Infoln(err)
			return
		}
	}

	// Observe replication targets histogram for all operations (including zero)
	stats.VolumeServerReplicationTargets.Observe(float64(replicaCount))

	if replicaCount > 0 { //send to other replica locations
		start := time.Now()

		inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(stats.WriteToReplicas)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

		err = DistributedOperation(ctx, remoteLocations, func(ctx context.Context, location operation.Location) error {
			u := url.URL{
				Scheme: "http",
				Host:   location.Url,
				Path:   r.URL.Path,
			}
			q := url.Values{
				"type": {"replicate"},
				"ttl":  {n.Ttl.String()},
			}
			if n.LastModified > 0 {
				q.Set("ts", strconv.FormatUint(n.LastModified, 10))
			}
			if n.IsChunkedManifest() {
				q.Set("cm", "true")
			}
			u.RawQuery = q.Encode()

			pairMap := make(map[string]string)
			if n.HasPairs() {
				tmpMap := make(map[string]string)
				err := json.Unmarshal(n.Pairs, &tmpMap)
				if err != nil {
					stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorUnmarshalPairs).Inc()
					glog.V(0).Infoln("Unmarshal pairs error:", err)
				}
				for k, v := range tmpMap {
					pairMap[needle.PairNamePrefix+k] = v
				}
			}
			bytesBuffer := buffer_pool.SyncPoolGetBuffer()
			defer buffer_pool.SyncPoolPutBuffer(bytesBuffer)

			// volume server do not know about encryption
			// TODO optimize here to compress data only once
			uploadOption := &operation.UploadOption{
				UploadUrl:         u.String(),
				Filename:          string(n.Name),
				Cipher:            false,
				IsInputCompressed: n.IsCompressed(),
				IsReplication:     true,
				MimeType:          string(n.Mime),
				PairMap:           pairMap,
				Jwt:               jwt,
				Md5:               contentMd5,
				BytesBuffer:       bytesBuffer,
				MaxAttempts:       1, // fail fast on a dead replica; the client write retries
			}

			uploader, err := operation.NewUploader()
			if err != nil {
				glog.Errorf("replication-UploadData, err:%v, url:%s", err, u.String())
				return err
			}
			_, err = uploader.UploadData(ctx, n.Data, uploadOption)
			if err != nil {
				glog.Errorf("replication-UploadData, err:%v, url:%s", err, u.String())
			}
			return err
		})
		stats.VolumeServerRequestHistogram.WithLabelValues(stats.WriteToReplicas).Observe(time.Since(start).Seconds())
		if err != nil {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorWriteToReplicas).Inc()
			stats.VolumeServerReplicationCounter.WithLabelValues(stats.ReplicationOpWrite, stats.ReplicationFailure).Inc()
			reason := classifyReplicationError(err)
			stats.VolumeServerReplicationFailures.WithLabelValues(stats.ReplicationOpWrite, reason).Inc()
			err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			glog.V(0).Infoln(err)
			return false, err
		}
		stats.VolumeServerReplicationCounter.WithLabelValues(stats.ReplicationOpWrite, stats.ReplicationSuccess).Inc()
	}
	return
}

// ReplicatedDelete deletes a needle from the local volume and sends delete
// requests to all remote replica locations. Replica deletes use
// context.Background() so that a client disconnect does not orphan replica
// deletes.
func ReplicatedDelete(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, store *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request) (size types.Size, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		remoteLocations, err = GetWritableRemoteReplications(store, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	replicaCount := len(remoteLocations)

	if replicaCount > 0 {
		// Record replication duration and operation counter for delete
		defer func(t time.Time) {
			stats.VolumeServerReplicationHistogram.WithLabelValues(stats.ReplicationOpDelete).Observe(time.Since(t).Seconds())
			if err != nil {
				stats.VolumeServerReplicationCounter.WithLabelValues(stats.ReplicationOpDelete, stats.ReplicationFailure).Inc()
			} else {
				stats.VolumeServerReplicationCounter.WithLabelValues(stats.ReplicationOpDelete, stats.ReplicationSuccess).Inc()
			}
		}(time.Now())
	}

	size, err = store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}

	// Observe replication targets histogram for all operations (including zero)
	stats.VolumeServerReplicationTargets.Observe(float64(replicaCount))

	if replicaCount > 0 { //send to other replica locations
		// background, not r.Context(): a client disconnect must not orphan replica deletes
		if err = DistributedOperation(context.Background(), remoteLocations, func(ctx context.Context, location operation.Location) error {
			return util_http.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
		}); err != nil {
			reason := classifyReplicationError(err)
			stats.VolumeServerReplicationFailures.WithLabelValues(stats.ReplicationOpDelete, reason).Inc()
			size = 0
		}
	}
	return
}

type DistributedOperationResult map[string]error

func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

type RemoteResult struct {
	Host  string
	Error error
}

func DistributedOperation(ctx context.Context, locations []operation.Location, op func(ctx context.Context, location operation.Location) error) error {
	length := len(locations)
	if length == 0 {
		return nil
	}

	// cancel outstanding replica ops once the outcome is decided
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// buffered so a straggler (e.g. a replica stalled on a TCP dial timeout) can
	// still deliver its result and exit after we have already returned.
	resultCh := make(chan RemoteResult, length)
	for _, location := range locations {
		go func(location operation.Location) {
			resultCh <- RemoteResult{location.Url, op(ctx, location)}
		}(location)
	}

	ret := DistributedOperationResult(make(map[string]error))
	for i := 0; i < length; i++ {
		result := <-resultCh
		ret[result.Host] = result.Error
		if result.Error != nil {
			// fail fast on the first error instead of waiting for slow replicas
			return ret.Error()
		}
	}
	return ret.Error()
}

func GetWritableRemoteReplications(s *storage.Store, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, masterFn operation.GetMasterFn) (remoteLocations []operation.Location, err error) {

	v := s.GetVolume(volumeId)
	if v != nil && v.ReplicaPlacement.GetCopyCount() == 1 {
		return
	}

	// not on local store, or has replications
	lookupResult, lookupErr := operation.LookupVolumeId(masterFn, grpcDialOption, volumeId.String())
	if lookupErr == nil {
		selfUrl := util.JoinHostPort(s.Ip, s.Port)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				remoteLocations = append(remoteLocations, location)
			}
		}
	} else {
		err = fmt.Errorf("replicating lookup failed for %d: %v", volumeId, lookupErr)
		return
	}

	if v != nil {
		// has one local and has remote replications
		copyCount := v.ReplicaPlacement.GetCopyCount()
		if len(lookupResult.Locations) < copyCount {
			err = fmt.Errorf("replicating operations [%d] is less than volume %d replication copy count [%d]",
				len(lookupResult.Locations), volumeId, copyCount)
		}
	}

	return
}

// classifyReplicationError maps a Go error to a bounded-cardinality failure
// reason label for the replication_failures_total metric. Returns an empty
// string for nil errors.
func classifyReplicationError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return stats.FailureTimeout
	}
	if errors.Is(err, context.Canceled) {
		return stats.FailureContextCancelled
	}
	errStr := err.Error()
	if strings.Contains(errStr, "connection refused") {
		return stats.FailureConnectionRefused
	}
	return stats.FailureServerError
}
