package testutil

import "testing"

// AllocateMiniPorts must never hand out a port that weed mini will reserve
// for one of its default services (or that default's gRPC offset). A real
// failure: Filer was given 33646 (Admin default 23646 + GrpcPortOffset),
// which mini then refused as "reserved for gRPC calculation".
func TestAllocateMiniPortsAvoidsMiniDefaults(t *testing.T) {
	reserved := reservedMiniPorts()
	for iter := 0; iter < 200; iter++ {
		ports, err := AllocateMiniPorts(4)
		if err != nil {
			t.Fatalf("iter %d: AllocateMiniPorts: %v", iter, err)
		}
		for _, p := range ports {
			if reserved[p] {
				t.Fatalf("iter %d: allocated port %d is a mini default (or gRPC offset)", iter, p)
			}
			if reserved[p+GrpcPortOffset] {
				t.Fatalf("iter %d: allocated port %d has gRPC offset %d colliding with a mini default",
					iter, p, p+GrpcPortOffset)
			}
		}
	}
}

// AllocateMiniPorts must keep every port it hands out, and that port's gRPC
// counterpart, below 32768 - the Linux default ip_local_port_range floor.
// A real failure: filer was allocated 44204, then lost the bind to a transient
// outbound connection that had grabbed 44204 as an ephemeral source port
// during mini startup ("bind: address already in use").
func TestAllocateMiniPortsBelowEphemeralFloor(t *testing.T) {
	const ephemeralFloor = 32768
	for iter := 0; iter < 200; iter++ {
		ports, err := AllocateMiniPorts(4)
		if err != nil {
			t.Fatalf("iter %d: AllocateMiniPorts: %v", iter, err)
		}
		for _, p := range ports {
			if p+GrpcPortOffset >= ephemeralFloor {
				t.Fatalf("iter %d: port %d (gRPC %d) reaches the ephemeral range floor %d",
					iter, p, p+GrpcPortOffset, ephemeralFloor)
			}
		}
	}
}

func TestAllocatePortSetNoGrpcCollision(t *testing.T) {
	// Run a few iterations to catch the OS-recycles-just-closed-port race
	// that previously hit regular ports when the mini gRPC offset was freed
	// between AllocateMiniPorts and AllocatePorts calls.
	for iter := 0; iter < 20; iter++ {
		mini, regular, err := AllocatePortSet(1, 3)
		if err != nil {
			t.Fatalf("iter %d: AllocatePortSet: %v", iter, err)
		}
		if len(mini) != 1 || len(regular) != 3 {
			t.Fatalf("iter %d: unexpected counts mini=%d regular=%d", iter, len(mini), len(regular))
		}
		reserved := map[int]bool{
			mini[0]:                  true,
			mini[0] + GrpcPortOffset: true,
		}
		for _, p := range regular {
			if reserved[p] {
				t.Fatalf("iter %d: regular port %d collides with mini pair %d/%d",
					iter, p, mini[0], mini[0]+GrpcPortOffset)
			}
			reserved[p] = true
		}
	}
}
