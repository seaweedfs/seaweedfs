package weed_server

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newStoppableFilerServer() *FilerServer {
	fs := &FilerServer{}
	fs.subscriptionsStopped, fs.stopSubscriptions = context.WithCancel(context.Background())
	return fs
}

func TestStopSubscriptionsEndsOpenSubscriptions(t *testing.T) {
	fs := newStoppableFilerServer()
	ctx, cancel := fs.subscriptionContext(context.Background())
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("subscription ended before StopSubscriptions")
	default:
	}
	fs.StopSubscriptions()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription still open after StopSubscriptions")
	}
}

func TestSubscriptionStartedAfterStopEndsAtOnce(t *testing.T) {
	fs := newStoppableFilerServer()
	fs.StopSubscriptions()
	ctx, cancel := fs.subscriptionContext(context.Background())
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a subscription started during shutdown stayed open")
	}
}

func TestSubscriptionStillEndsWithItsStream(t *testing.T) {
	fs := newStoppableFilerServer()
	stream, closeStream := context.WithCancel(context.WithValue(context.Background(), struct{}{}, "peer"))
	ctx, cancel := fs.subscriptionContext(stream)
	defer cancel()
	if ctx.Value(struct{}{}) != "peer" {
		t.Error("subscription context lost the stream's values")
	}
	closeStream()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription outlived its stream")
	}
}

func TestSubscriptionContextWithoutStopSignal(t *testing.T) {
	fs := &FilerServer{}
	fs.StopSubscriptions()
	ctx, cancel := fs.subscriptionContext(context.Background())
	select {
	case <-ctx.Done():
		t.Fatal("subscription ended although nothing stopped it")
	default:
	}
	cancel()
	<-ctx.Done()
}

// A follower reads a clean end as "done" and stops for good (util.RetryUntil
// returns on nil), so a subscription that StopSubscriptions ended must not
// end cleanly.
func TestStoppedSubscriptionEndsUnavailable(t *testing.T) {
	fs := newStoppableFilerServer()
	fs.StopSubscriptions()
	err := fs.endOfSubscription(context.Background(), nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped subscription ended with %v, want Unavailable", err)
	}
}

func TestSubscriptionEndsCleanlyWithoutStop(t *testing.T) {
	fs := newStoppableFilerServer()
	if err := fs.endOfSubscription(context.Background(), nil); err != nil {
		t.Fatalf("subscription without shutdown ended with %v, want nil", err)
	}
	if err := (&FilerServer{}).endOfSubscription(context.Background(), nil); err != nil {
		t.Fatalf("server without stop signal ended with %v, want nil", err)
	}
}

func TestSubscriptionErrorPassesThroughStop(t *testing.T) {
	fs := newStoppableFilerServer()
	fs.StopSubscriptions()
	want := errors.New("reading from persisted logs")
	if err := fs.endOfSubscription(context.Background(), want); !errors.Is(err, want) {
		t.Fatalf("got %v, want the handler's own error", err)
	}
}

func TestClosedStreamEndsCleanlyDuringStop(t *testing.T) {
	fs := newStoppableFilerServer()
	fs.StopSubscriptions()
	stream, closeStream := context.WithCancel(context.Background())
	closeStream()
	if err := fs.endOfSubscription(stream, nil); err != nil {
		t.Fatalf("stream the client closed ended with %v, want nil", err)
	}
}
