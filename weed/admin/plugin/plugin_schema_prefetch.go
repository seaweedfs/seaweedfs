package plugin

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const descriptorPrefetchTimeout = 20 * time.Second

func (r *Plugin) prefetchDescriptorsFromHello(hello *plugin_pb.WorkerHello) {
	if hello == nil || hello.WorkerId == "" || len(hello.Capabilities) == 0 {
		return
	}

	jobTypeSet := make(map[string]struct{})
	for _, capability := range hello.Capabilities {
		if capability == nil || capability.JobType == "" {
			continue
		}
		if !capability.CanDetect && !capability.CanExecute {
			continue
		}
		jobTypeSet[capability.JobType] = struct{}{}
	}

	if len(jobTypeSet) == 0 {
		return
	}

	jobTypes := make([]string, 0, len(jobTypeSet))
	for jobType := range jobTypeSet {
		jobTypes = append(jobTypes, jobType)
	}
	sort.Strings(jobTypes)

	for _, jobType := range jobTypes {
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(r.ctx, descriptorPrefetchTimeout)
		_, err := r.requestConfigSchema(ctx, jobType, true, hello.WorkerId)
		cancel()
		if err != nil {
			glog.V(1).Infof("Plugin descriptor prefetch skipped for %s: %v", jobType, err)
			continue
		}

		glog.V(1).Infof("Plugin descriptor prefetched for job_type=%s", jobType)
	}
}
