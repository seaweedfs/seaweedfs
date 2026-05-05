package testutil

import "testing"

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
