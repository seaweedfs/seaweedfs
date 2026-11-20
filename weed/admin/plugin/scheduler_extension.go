package plugin

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
)

// schedulerExtension is the set of points where scheduling policy can be
// layered on the lane scheduler. The default leaves every decision alone.
type schedulerExtension interface {
	// runLane may take over one scheduling pass of a lane; handled reports
	// whether it did. The lane loop does not sleep while a detection time of
	// the lane is past due, so a handled pass that reports no jobs must
	// advance those times or wait before it returns.
	runLane(ls *schedulerLaneState, jobTypes []string) (hadJobs, handled bool)
	// admitJob reports whether a job built from a proposal is queued.
	admitJob(job *plugin_pb.JobSpec) bool
	// startJob decides whether a queued job starts on the executor a slot
	// was just reserved on. If it does, done is called when that slot is
	// given back: after a timeout or cancel, that is once the cancel is
	// sent, not once the worker stops. done may be nil.
	startJob(job *plugin_pb.JobSpec, executor *WorkerSession) (done func(), hold startHold)
	// jobHoldsLock reports whether a job of a lane that needs the cluster
	// admin lock holds it from start to finish on this executor.
	jobHoldsLock(job *plugin_pb.JobSpec, executor *WorkerSession) bool
	// registerServices adds what the extension serves to workers.
	registerServices(server grpc.ServiceRegistrar)
}

// startHold is what to do with a job that reached the head of its queue.
type startHold int

const (
	// startNow runs the job.
	startNow startHold = iota
	// startRetry keeps the job at the head of its queue and asks again.
	startRetry
	// startOthersFirst moves the job behind the rest of its queue.
	startOthersFirst
)

type defaultSchedulerExtension struct{}

func (defaultSchedulerExtension) runLane(*schedulerLaneState, []string) (bool, bool) {
	return false, false
}

func (defaultSchedulerExtension) admitJob(*plugin_pb.JobSpec) bool { return true }

func (defaultSchedulerExtension) startJob(*plugin_pb.JobSpec, *WorkerSession) (func(), startHold) {
	return nil, startNow
}

func (defaultSchedulerExtension) jobHoldsLock(*plugin_pb.JobSpec, *WorkerSession) bool {
	return true
}

func (defaultSchedulerExtension) registerServices(grpc.ServiceRegistrar) {}

// newSchedulerExtension builds the extension a Plugin schedules with.
var newSchedulerExtension = func(*Plugin) schedulerExtension { return defaultSchedulerExtension{} }

// RegisterGrpcServices registers what the plugin serves to workers.
func (r *Plugin) RegisterGrpcServices(server grpc.ServiceRegistrar) {
	plugin_pb.RegisterPluginControlServiceServer(server, r)
	r.schedulerExt().registerServices(server)
}

func (r *Plugin) schedulerExt() schedulerExtension {
	if r.schedulerExtension == nil {
		return defaultSchedulerExtension{}
	}
	return r.schedulerExtension
}

// startScheduledJob asks the extension whether the job may run in the slot
// reserved for it. If not, the slot is given back and the job returns to its
// queue. If so, the release it returns also reports the job done.
func (r *Plugin) startScheduledJob(queues *workerJobQueues, item *queuedJob, executor *WorkerSession, release func()) (func(), bool) {
	done, hold := r.schedulerExt().startJob(item.job, executor)
	switch hold {
	case startNow:
		if done == nil {
			return release, true
		}
		return func() {
			release()
			done()
		}, true
	case startOthersFirst:
		release()
		queues.push(item.home, item.job)
	default:
		release()
		queues.pushFront(item)
	}
	return nil, false
}
