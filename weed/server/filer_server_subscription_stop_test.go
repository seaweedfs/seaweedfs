package weed_server

import (
	"context"
	"testing"
	"time"
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
