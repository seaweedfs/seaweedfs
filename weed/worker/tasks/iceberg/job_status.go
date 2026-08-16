package iceberg

import (
	"context"
	"encoding/json"
	"path"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// jobStatusUpdateAttempts bounds the read-modify-write retry loop for the job
// status attribute.
const jobStatusUpdateAttempts = 2

// buildJobStatus folds the operations this job ran into the AWS job types that
// govern them. Operations that did not run are left out so a partial run does
// not erase what an earlier one recorded.
func buildJobStatus(opErrors map[string]error, ranAt time.Time) s3tables.MaintenanceJobStatus {
	jobStatus := s3tables.MaintenanceJobStatus{}

	for op, opErr := range opErrors {
		jobType := maintenanceJobType(op)
		if jobType == "" {
			continue
		}

		value, ok := jobStatus[jobType]
		if !ok {
			timestamp := ranAt
			value = &s3tables.MaintenanceJobStatusValue{
				Status:           s3tables.MaintenanceJobStatusSuccessful,
				LastRunTimestamp: &timestamp,
			}
			jobStatus[jobType] = value
		}

		// Several operations can map to one job type; one failure fails the type.
		if opErr != nil && value.Status != s3tables.MaintenanceJobStatusFailed {
			value.Status = s3tables.MaintenanceJobStatusFailed
			value.FailureMessage = opErr.Error()
		}
	}

	return jobStatus
}

// recordJobStatus merges this job's outcome into the table's stored status.
// Status is advisory, so a lost race is logged rather than failing a job whose
// work already committed.
func recordJobStatus(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	jobStatus s3tables.MaintenanceJobStatus,
) {
	if len(jobStatus) == 0 {
		return
	}

	tableDir := path.Join(s3tables.TablesPath, bucketName, tablePath)
	dir, name := path.Dir(tableDir), path.Base(tableDir)

	for attempt := 0; attempt < jobStatusUpdateAttempts; attempt++ {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			glog.V(1).Infof("iceberg maintenance: cannot record job status for %s/%s: %v", bucketName, tablePath, err)
			return
		}
		if resp == nil || resp.Entry == nil {
			return
		}

		entry := resp.Entry
		current := entry.Extended[s3tables.ExtendedKeyMaintenanceStatus]

		merged := s3tables.MaintenanceJobStatus{}
		if len(current) > 0 {
			if err := json.Unmarshal(current, &merged); err != nil {
				glog.V(1).Infof("iceberg maintenance: replacing unreadable job status on %s/%s: %v", bucketName, tablePath, err)
				merged = s3tables.MaintenanceJobStatus{}
			}
		}
		for jobType, value := range jobStatus {
			merged[jobType] = value
		}

		data, err := json.Marshal(merged)
		if err != nil {
			glog.V(1).Infof("iceberg maintenance: cannot marshal job status for %s/%s: %v", bucketName, tablePath, err)
			return
		}

		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3tables.ExtendedKeyMaintenanceStatus] = data

		_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory:        dir,
			Entry:            entry,
			ExpectedExtended: map[string][]byte{s3tables.ExtendedKeyMaintenanceStatus: current},
		})
		if err == nil {
			return
		}
		if status.Code(err) != codes.FailedPrecondition {
			glog.V(1).Infof("iceberg maintenance: cannot record job status for %s/%s: %v", bucketName, tablePath, err)
			return
		}
	}

	glog.V(1).Infof("iceberg maintenance: gave up recording job status for %s/%s after %d attempts", bucketName, tablePath, jobStatusUpdateAttempts)
}
