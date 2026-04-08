// Package testutil provides shared test utilities for SeaweedFS integration tests.
package testutil

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
)

// GrpcPortOffset is the offset weed mini uses to derive gRPC ports from HTTP ports.
const GrpcPortOffset = 10000

// AllocatePorts allocates count unique free ports atomically.
// All listeners are held open until every port is obtained, preventing
// the OS from recycling a port between successive allocations.
func AllocatePorts(count int) ([]int, error) {
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, ll := range listeners {
				_ = ll.Close()
			}
			return nil, err
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports, nil
}

// MustAllocatePorts is a testing wrapper for AllocatePorts.
func MustAllocatePorts(t *testing.T, count int) []int {
	t.Helper()
	ports, err := AllocatePorts(count)
	if err != nil {
		t.Fatalf("Failed to allocate %d free ports: %v", count, err)
	}
	return ports
}

// AllocateMiniPorts allocates n free ports where each port and its gRPC
// counterpart (port + GrpcPortOffset) are available and don't collide
// with any other allocated port or its gRPC counterpart. All listeners
// are held open until the entire batch is allocated, preventing the OS
// from recycling ports between allocations. Use this when ports will be
// passed to weed mini without explicit gRPC port flags, so mini will
// derive gRPC ports as HTTP + 10000.
func AllocateMiniPorts(count int) ([]int, error) {
	const (
		minPort = 10000
		maxPort = 55000
	)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	reserved := make(map[int]bool)
	ports := make([]int, 0, count)
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	for idx := 0; idx < count; idx++ {
		found := false
		for i := 0; i < 1000; i++ {
			port := minPort + r.Intn(maxPort-minPort)
			grpcPort := port + GrpcPortOffset

			if reserved[port] || reserved[grpcPort] {
				continue
			}

			l1, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				continue
			}

			l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
			if err != nil {
				l1.Close()
				continue
			}

			listeners = append(listeners, l1, l2)
			reserved[port] = true
			reserved[grpcPort] = true
			ports = append(ports, port)
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("failed to allocate mini port %d of %d", idx+1, count)
		}
	}

	return ports, nil
}

// MustFreeMiniPorts allocates n ports suitable for weed mini, ensuring
// each port's gRPC offset (port + 10000) doesn't collide with any other
// allocated port. names is used only for error messages.
func MustFreeMiniPorts(t *testing.T, names []string) []int {
	t.Helper()
	ports, err := AllocateMiniPorts(len(names))
	if err != nil {
		t.Fatalf("failed to allocate mini ports for %v: %v", names, err)
	}
	return ports
}

// MustFreeMiniPort allocates a single weed mini port.
func MustFreeMiniPort(t *testing.T, name string) int {
	t.Helper()
	return MustFreeMiniPorts(t, []string{name})[0]
}
