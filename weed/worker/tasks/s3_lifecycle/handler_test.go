package s3_lifecycle

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Tests cover the worker-handler surface that runs without a live filer
// or S3 server: pure helpers (clusterS3Endpoints, readString),
// Capability/Descriptor sanity, and the Detect input-validation /
// skip-activity / proposal paths driven by a recorder sender.

// recordingSender captures everything the handler sends so each Detect
// case can assert on activities, proposals, and completion.
type recordingSender struct {
	proposals  []*plugin_pb.DetectionProposals
	completes  []*plugin_pb.DetectionComplete
	activities []*plugin_pb.ActivityEvent
	// errOn forces the named send to fail; lets tests cover the
	// SendComplete error-propagation path without a full transport stub.
	errOn map[string]error
}

func (r *recordingSender) SendProposals(p *plugin_pb.DetectionProposals) error {
	if err := r.errOn["proposals"]; err != nil {
		return err
	}
	r.proposals = append(r.proposals, p)
	return nil
}
func (r *recordingSender) SendComplete(c *plugin_pb.DetectionComplete) error {
	if err := r.errOn["complete"]; err != nil {
		return err
	}
	r.completes = append(r.completes, c)
	return nil
}
func (r *recordingSender) SendActivity(a *plugin_pb.ActivityEvent) error {
	if err := r.errOn["activity"]; err != nil {
		return err
	}
	r.activities = append(r.activities, a)
	return nil
}

// ---------- clusterS3Endpoints ----------

func TestClusterS3Endpoints_NilContext(t *testing.T) {
	// A nil ClusterContext occurs in early-bootstrap detect calls; the
	// handler must return an empty slice rather than panic.
	assert.Nil(t, clusterS3Endpoints(nil))
}

func TestClusterS3Endpoints_EmptyList(t *testing.T) {
	assert.Empty(t, clusterS3Endpoints(&plugin_pb.ClusterContext{}))
}

func TestClusterS3Endpoints_FiltersEmptyEntries(t *testing.T) {
	// An empty-string address represents a stale registry entry the
	// master is about to evict; never dial that. Order of the surviving
	// entries must be preserved so the handler dials a deterministic
	// host across detect runs.
	cc := &plugin_pb.ClusterContext{S3GrpcAddresses: []string{"s3a:8333", "", "s3b:8333", ""}}
	assert.Equal(t, []string{"s3a:8333", "s3b:8333"}, clusterS3Endpoints(cc))
}

func TestClusterS3Endpoints_AllValid(t *testing.T) {
	cc := &plugin_pb.ClusterContext{S3GrpcAddresses: []string{"s3a:8333", "s3b:8333"}}
	assert.Equal(t, []string{"s3a:8333", "s3b:8333"}, clusterS3Endpoints(cc))
}

// ---------- readString ----------

func TestReadString_MissingKeyReturnsFallback(t *testing.T) {
	got := readString(map[string]*plugin_pb.ConfigValue{}, "missing", "fallback")
	assert.Equal(t, "fallback", got)
}

func TestReadString_NilValueReturnsFallback(t *testing.T) {
	got := readString(map[string]*plugin_pb.ConfigValue{"k": nil}, "k", "fallback")
	assert.Equal(t, "fallback", got)
}

func TestReadString_WrongKindReturnsFallback(t *testing.T) {
	// Configs are typed; an Int64 in a string slot is a writer-side bug
	// the handler must tolerate rather than panic on a type assertion.
	values := map[string]*plugin_pb.ConfigValue{
		"k": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 42}},
	}
	assert.Equal(t, "fallback", readString(values, "k", "fallback"))
}

func TestReadString_StringValueReturned(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"k": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "expected"}},
	}
	assert.Equal(t, "expected", readString(values, "k", "fallback"))
}

// ---------- Capability ----------

func TestCapability_AdvertisesJobType(t *testing.T) {
	h := NewHandler(nil)
	cap := h.Capability()
	require.NotNil(t, cap)
	assert.Equal(t, jobType, cap.JobType)
	assert.True(t, cap.CanDetect)
	assert.True(t, cap.CanExecute)
	// Admin caps concurrency at one job per worker; a rebuild of the
	// scheduler must wait for the prior one to exit.
	assert.Equal(t, int32(1), cap.MaxDetectionConcurrency)
	assert.Equal(t, int32(1), cap.MaxExecutionConcurrency)
}

// ---------- Detect ----------

func TestDetect_NilRequestErrors(t *testing.T) {
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), nil, r)
	require.Error(t, err)
	assert.Empty(t, r.proposals)
	assert.Empty(t, r.completes)
}

func TestDetect_NilSenderErrors(t *testing.T) {
	h := NewHandler(nil)
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{}, nil)
	require.Error(t, err)
}

func TestDetect_WrongJobTypeErrors(t *testing.T) {
	// A request routed to this handler with a foreign JobType is the
	// admin's bug; surface as an error so it's visible rather than
	// silently emitting a bogus proposal.
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{JobType: "different_job"}, r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different_job")
}

func TestDetect_NoS3EndpointsCompletesWithSkipActivity(t *testing.T) {
	// A cluster with no S3 servers registered yet must not spawn the
	// scheduler; emit a "skipped" activity for operator visibility and
	// complete with success so the admin doesn't classify as a failure.
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:        jobType,
		ClusterContext: &plugin_pb.ClusterContext{},
	}, r)
	require.NoError(t, err)
	assert.Empty(t, r.proposals, "no S3 endpoints must not yield a proposal")
	require.Len(t, r.activities, 1)
	assert.Equal(t, "skipped", r.activities[0].Stage)
	assert.Contains(t, r.activities[0].Message, "no s3 servers")
	require.Len(t, r.completes, 1)
	assert.True(t, r.completes[0].Success, "skip is success, not failure")
	assert.Equal(t, jobType, r.completes[0].JobType)
}

func TestDetect_NoFilerAddressesCompletesWithSkipActivity(t *testing.T) {
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: jobType,
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
			// no FilerAddresses
		},
	}, r)
	require.NoError(t, err)
	assert.Empty(t, r.proposals)
	require.Len(t, r.activities, 1)
	assert.Equal(t, "skipped", r.activities[0].Stage)
	assert.Contains(t, r.activities[0].Message, "no filer addresses")
	require.Len(t, r.completes, 1)
	assert.True(t, r.completes[0].Success)
}

func TestDetect_HappyPathProposesOneJobWithFirstFilerAddress(t *testing.T) {
	// Detect must propose exactly one job that targets the first filer
	// address in the cluster context; the master refreshes the list so
	// a stale entry self-heals on the next run.
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: jobType,
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
			// pb.ServerAddress dual-port form; the gRPC port is pinned off the
			// +10000 convention so a raw-forwarding regression resurfaces here.
			FilerAddresses: []string{"filer-a:8888.18890", "filer-b:8888.18890"},
		},
	}, r)
	require.NoError(t, err)
	assert.Empty(t, r.activities, "happy path emits no skip activity")

	require.Len(t, r.proposals, 1)
	require.Len(t, r.proposals[0].Proposals, 1)
	prop := r.proposals[0].Proposals[0]
	assert.Equal(t, jobType, prop.JobType)
	assert.NotEmpty(t, prop.ProposalId, "proposal id must be unique-per-run")
	require.Contains(t, prop.Parameters, "filer_grpc_address")
	val := prop.Parameters["filer_grpc_address"].GetStringValue()
	assert.Equal(t, "filer-a:18890", val, "first filer resolved to its gRPC port, not host:28890")

	require.Len(t, r.completes, 1)
	assert.True(t, r.completes[0].Success)
	assert.Equal(t, int32(1), r.completes[0].TotalProposals)
}

func TestDetect_EmptyJobTypeAccepted(t *testing.T) {
	// Detect is sometimes invoked with an unset JobType (broadcast
	// detect); the handler must accept and behave as if it matched.
	h := NewHandler(nil)
	r := &recordingSender{}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
			FilerAddresses:  []string{"f:8888.18890"},
		},
	}, r)
	require.NoError(t, err)
	require.Len(t, r.proposals, 1)
}

func TestDetect_PropagatesProposalsSendError(t *testing.T) {
	// SendProposals failing must propagate; otherwise the worker would
	// silently report success despite never delivering the proposal.
	h := NewHandler(nil)
	want := errors.New("transport down")
	r := &recordingSender{errOn: map[string]error{"proposals": want}}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: jobType,
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
			FilerAddresses:  []string{"f:8888.18890"},
		},
	}, r)
	assert.ErrorIs(t, err, want)
	assert.Empty(t, r.proposals)
	assert.Empty(t, r.completes, "complete must not fire when proposals fail")
}

func TestDetect_PropagatesCompleteSendError(t *testing.T) {
	// SendComplete failing must propagate; otherwise the worker would
	// report success to the admin despite the completion signal never
	// landing. Proposals went out before the failure, so they remain in
	// the recorder.
	h := NewHandler(nil)
	want := errors.New("transport down")
	r := &recordingSender{errOn: map[string]error{"complete": want}}
	err := h.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: jobType,
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
			FilerAddresses:  []string{"f:8888.18890"},
		},
	}, r)
	assert.ErrorIs(t, err, want)
	assert.Len(t, r.proposals, 1, "proposals send before complete and remain recorded")
	assert.Empty(t, r.completes)
}

// ---------- Descriptor ----------

func TestDescriptor_BasicShape(t *testing.T) {
	// Sanity-check the Descriptor's public-facing identifiers so a
	// rename in handler.go doesn't silently break the admin UI without
	// an admin-side change too.
	h := NewHandler(nil)
	d := h.Descriptor()
	require.NotNil(t, d)
	assert.Equal(t, jobType, d.JobType)
	assert.NotEmpty(t, d.DisplayName)
	assert.NotEmpty(t, d.Description)
	assert.Greater(t, d.DescriptorVersion, uint32(0), "descriptor version must be positive (admins use it for compat)")
}

func TestDescriptor_AdminConfigFormHasNoWorkersField(t *testing.T) {
	// "workers" used to be an admin form field controlling in-process
	// pipeline goroutines. It's per-worker tuning, not a cluster-wide
	// scope concern, so it was removed from the form. ParseConfig hard-
	// codes cfg.Workers from shardPipelineGoroutines instead.
	h := NewHandler(nil)
	d := h.Descriptor()
	require.NotNil(t, d.AdminConfigForm)
	for _, sec := range d.AdminConfigForm.Sections {
		for _, f := range sec.Fields {
			assert.NotEqual(t, "workers", f.Name, "admin form must NOT expose the in-memory pipeline-goroutine knob")
		}
	}
	_, hasDefault := d.AdminConfigForm.DefaultValues["workers"]
	assert.False(t, hasDefault, "DefaultValues must NOT include 'workers' (form field removed)")
}

func TestDescriptor_WorkerConfigFormIsAbsent(t *testing.T) {
	// "Per-Run Time Limit (minutes)" was the only worker-side knob and
	// duplicated the admin scheduler's Execution Timeout (both are
	// wall-clock caps on the same Execute call). Removed in favor of
	// AdminRuntimeDefaults.ExecutionTimeoutSeconds — single source of
	// truth. A WorkerConfigForm with no fields would render as an
	// empty section in the admin UI; drop the form entirely.
	h := NewHandler(nil)
	d := h.Descriptor()
	assert.Nil(t, d.WorkerConfigForm,
		"WorkerConfigForm should be nil now that max_runtime_minutes is gone; if you re-add a worker-side knob, restore the form and pin it here")
}

func TestDescriptor_AdminRuntimeDefaultsHaveNoTimeoutInPractice(t *testing.T) {
	// Lifecycle is a scheduled batch whose natural duration is "as
	// long as today's events take." The scheduler's global 90s default
	// would kill every real run, and a numeric cap operators have to
	// estimate is a footgun (too low truncates a legitimate
	// large-bucket pass; too high makes the value meaningless). Declare
	// math.MaxInt32 seconds for both knobs to say "no timeout in
	// practice" in a code-review-readable way. A future change that
	// tightens the cap should fail this test so the choice is
	// re-examined consciously.
	h := NewHandler(nil)
	d := h.Descriptor()
	require.NotNil(t, d.AdminRuntimeDefaults)
	assert.Equal(t, int32(math.MaxInt32), d.AdminRuntimeDefaults.ExecutionTimeoutSeconds,
		"ExecutionTimeoutSeconds should be effectively unlimited; the scheduler's 90s default would otherwise clobber the worker mid-run")
	assert.Equal(t, int32(math.MaxInt32), d.AdminRuntimeDefaults.JobTypeMaxRuntimeSeconds,
		"JobTypeMaxRuntimeSeconds is the per-pass budget — keep aligned with ExecutionTimeoutSeconds")
}

func TestDescriptor_AdminRuntimeDefaultsDailyCadence(t *testing.T) {
	// Lifecycle is a daily batch; the admin must default to a 24-hour
	// detection interval so cron pressure doesn't escalate. Bound the
	// detection timeout so a stuck detect can't pin a worker slot
	// indefinitely. Max 1 job per detection = scheduler runs alone.
	h := NewHandler(nil)
	d := h.Descriptor()
	require.NotNil(t, d.AdminRuntimeDefaults)
	assert.Equal(t, int32(24*60), d.AdminRuntimeDefaults.DetectionIntervalMinutes)
	assert.Equal(t, int32(60), d.AdminRuntimeDefaults.DetectionTimeoutSeconds, "60s timeout caps a stuck detect at one minute")
	assert.Equal(t, int32(1), d.AdminRuntimeDefaults.MaxJobsPerDetection)
}

func TestDescriptor_AdminRuntimeDefaultsEnabledByDefault(t *testing.T) {
	// S3 lifecycle is a standard bucket feature — operators set
	// PutBucketLifecycleConfiguration expecting it to actually fire.
	// Default-off would silently retain data past declared expiration
	// until an operator notices and flips it on. Document the choice
	// here so a future change to disabled-by-default fails the test
	// and surfaces a conscious revisit.
	h := NewHandler(nil)
	d := h.Descriptor()
	require.NotNil(t, d.AdminRuntimeDefaults)
	assert.True(t, d.AdminRuntimeDefaults.Enabled,
		"s3_lifecycle defaults must enable the scheduler so configured rules fire without operator opt-in")
}

// ---------- Execute ----------

// recordingExecSender captures Execute-side messages. The Execute path
// dials gRPC after passing validation, so these tests only exercise
// the validation surface that errors out before any dial — proving
// the handler refuses malformed jobs early instead of waiting on a
// 30s dial timeout.
type recordingExecSender struct {
	progress     []*plugin_pb.JobProgressUpdate
	completed    []*plugin_pb.JobCompleted
	completedErr error
}

func (r *recordingExecSender) SendProgress(p *plugin_pb.JobProgressUpdate) error {
	r.progress = append(r.progress, p)
	return nil
}
func (r *recordingExecSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	if r.completedErr != nil {
		return r.completedErr
	}
	r.completed = append(r.completed, c)
	return nil
}

func TestExecute_NilRequestErrors(t *testing.T) {
	h := NewHandler(nil)
	err := h.Execute(context.Background(), nil, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestExecute_NilJobErrors(t *testing.T) {
	// A non-nil request with nil Job is a writer-side bug; refuse it
	// rather than panic dereferencing request.Job.JobType.
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{}, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestExecute_NilSenderErrors(t *testing.T) {
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{JobType: jobType},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestExecute_WrongJobTypeErrors(t *testing.T) {
	// A foreign job type routed to this handler is the admin's bug;
	// surface as an error rather than running a bogus scheduler.
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{JobType: "different_job"},
	}, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different_job")
}

func TestExecute_NoS3EndpointsErrors(t *testing.T) {
	// Detect emits a "skipped" activity for this case; Execute is
	// stricter — the admin shouldn't have routed an Execute request
	// without S3 endpoints, so error out instead of silently no-oping.
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job:            &plugin_pb.JobSpec{JobType: jobType},
		ClusterContext: &plugin_pb.ClusterContext{}, // no S3GrpcAddresses
	}, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no s3 servers")
}

func TestExecute_MissingFilerAddressErrors(t *testing.T) {
	// filer_grpc_address is set by Detect when it builds the proposal;
	// missing it means the proposal was tampered with or the admin
	// dropped the parameter. Refuse rather than dial nothing.
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobType:    jobType,
			Parameters: map[string]*plugin_pb.ConfigValue{}, // no filer_grpc_address
		},
		ClusterContext: &plugin_pb.ClusterContext{
			S3GrpcAddresses: []string{"s3a:8333"},
		},
	}, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "filer_grpc_address")
}

func TestExecute_EmptyJobTypeAccepted(t *testing.T) {
	// Same convention as Detect: an empty JobType is broadcast routing
	// and must be accepted. The handler then errors at the next
	// validation step (no S3 endpoints) rather than at the type check.
	h := NewHandler(nil)
	err := h.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job:            &plugin_pb.JobSpec{}, // empty JobType
		ClusterContext: &plugin_pb.ClusterContext{},
	}, &recordingExecSender{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no s3 servers", "validation flowed past the type check")
}

// ---------- sendSuccessCompletion ----------

func TestSendSuccessCompletion_EmitsCanonicalCompletion(t *testing.T) {
	sender := &recordingExecSender{}
	request := &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{JobId: "job-123"}, // empty JobType (broadcast routing)
	}

	require.NoError(t, sendSuccessCompletion(request, sender))

	require.Len(t, sender.completed, 1, "exactly one completion must be sent")
	c := sender.completed[0]
	assert.Equal(t, jobType, c.JobType, "completion must use the canonical jobType, not request.Job.JobType")
	assert.NotEmpty(t, c.JobType, "an empty JobType is dropped by the admin")
	assert.Equal(t, "job-123", c.JobId, "completion must echo the original JobId")
	assert.True(t, c.Success, "a clean replay is a success completion")
	require.NotNil(t, c.Result)
	assert.NotEmpty(t, c.Result.Summary, "admin UI surfaces a non-empty Result.Summary")
}

func TestSendSuccessCompletion_PropagatesSendError(t *testing.T) {
	sentinel := errors.New("stream closed")
	sender := &recordingExecSender{completedErr: sentinel}
	request := &plugin_pb.ExecuteJobRequest{Job: &plugin_pb.JobSpec{JobId: "job-456"}}

	err := sendSuccessCompletion(request, sender)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

// ---------- lookupBucketsPath ----------

// stubFilerConfigClient implements filer_pb.SeaweedFilerClient for the
// GetFilerConfiguration call lookupBucketsPath relies on; methods that
// aren't named here would panic if called, which the tests rely on to
// keep the surface narrow.
type stubFilerConfigClient struct {
	filer_pb.SeaweedFilerClient
	resp *filer_pb.GetFilerConfigurationResponse
	err  error
}

func (c *stubFilerConfigClient) GetFilerConfiguration(_ context.Context, _ *filer_pb.GetFilerConfigurationRequest, _ ...grpc.CallOption) (*filer_pb.GetFilerConfigurationResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}

func TestLookupBucketsPath_PropagatesGRPCError(t *testing.T) {
	// A failed config lookup must surface; without it, Execute would
	// proceed to dial S3 with an empty buckets path and quietly never
	// dispatch anything.
	want := errors.New("filer down")
	got, err := lookupBucketsPath(context.Background(), &stubFilerConfigClient{err: want})
	assert.ErrorIs(t, err, want)
	assert.Empty(t, got)
}

func TestLookupBucketsPath_UsesConfiguredDirBuckets(t *testing.T) {
	// When the filer reports a non-empty DirBuckets, the worker honors
	// it. Operators with a non-default layout (e.g. "/data/buckets")
	// can't be routed to "/buckets".
	got, err := lookupBucketsPath(context.Background(), &stubFilerConfigClient{
		resp: &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/data/buckets"},
	})
	require.NoError(t, err)
	assert.Equal(t, "/data/buckets", got)
}

func TestLookupBucketsPath_EmptyDirBucketsFallsBackToDefault(t *testing.T) {
	// Filer doesn't always populate DirBuckets (older configs); the
	// helper falls back to the documented default "/buckets" rather
	// than returning an empty path that would force-route to root.
	got, err := lookupBucketsPath(context.Background(), &stubFilerConfigClient{
		resp: &filer_pb.GetFilerConfigurationResponse{DirBuckets: ""},
	})
	require.NoError(t, err)
	assert.Equal(t, "/buckets", got)
}
