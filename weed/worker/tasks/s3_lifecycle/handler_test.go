package s3_lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			// no FilerGrpcAddresses
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
			S3GrpcAddresses:    []string{"s3a:8333"},
			FilerGrpcAddresses: []string{"filer-a:18888", "filer-b:18888"},
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
	assert.Equal(t, "filer-a:18888", val, "first reachable filer is dialed")

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
			S3GrpcAddresses:    []string{"s3a:8333"},
			FilerGrpcAddresses: []string{"f:18888"},
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
			S3GrpcAddresses:    []string{"s3a:8333"},
			FilerGrpcAddresses: []string{"f:18888"},
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
			S3GrpcAddresses:    []string{"s3a:8333"},
			FilerGrpcAddresses: []string{"f:18888"},
		},
	}, r)
	assert.ErrorIs(t, err, want)
	assert.Len(t, r.proposals, 1, "proposals send before complete and remain recorded")
	assert.Empty(t, r.completes)
}
