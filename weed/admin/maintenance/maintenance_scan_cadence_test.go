package maintenance

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// scanCadenceClient is a minimal AdminClient whose first call blocks until the test
// releases it. Blocking the first scan is what makes the cadence test deterministic:
// the scan loop evaluates the error state right after triggering a scan, and that scan
// runs in its own goroutine, so without the block the loop could observe the error
// counter either before or after the scan finished.
type scanCadenceClient struct {
	mu      sync.Mutex
	calls   int
	release chan struct{}
}

func (c *scanCadenceClient) WithMasterClient(fn func(client master_pb.SeaweedClient) error) error {
	c.mu.Lock()
	c.calls++
	first := c.calls == 1
	c.mu.Unlock()

	if first {
		<-c.release
	}

	// Returning nil without invoking fn yields an empty, successful scan.
	return nil
}

func (c *scanCadenceClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestScanLoopRestoresIntervalAfterBackoff is the regression test for the scan flood. The loop shortens its ticker to the
// error backoff delay after a failed scan. It used to compare the target interval against
// the *configured* scan interval rather than against the interval the ticker was actually
// running at, so once the errors stopped the comparison came out false and the ticker was
// never restored: one transient failure pinned the scanner to one scan per second for as
// long as the process lived.
//
// The test seeds the error state, lets the loop drop into the 1s backoff, then lets a scan
// succeed and asserts the loop goes back to the configured 3s cadence instead of keeping
// the 1s one.
func TestScanLoopRestoresIntervalAfterBackoff(t *testing.T) {
	const baseInterval = 3 * time.Second

	client := &scanCadenceClient{release: make(chan struct{})}

	config := DefaultMaintenanceConfig()
	config.ScanIntervalSeconds = int32(baseInterval / time.Second)
	manager := NewMaintenanceManager(client, config, nil)

	// Seed a failed scan so the very first cadence decision drops to the backoff delay.
	// backoffDelay is what getScanInterval returns while errorCount > 0.
	manager.mutex.Lock()
	manager.errorCount = 1
	manager.backoffDelay = time.Second
	manager.running = true
	manager.mutex.Unlock()

	go manager.scanLoop()
	defer manager.Stop()

	// First tick at ~3s: the scan blocks in the client, so the loop still sees errorCount == 1
	// and switches the ticker to the 1s backoff.
	deadline := time.Now().Add(baseInterval + 2*time.Second)
	for client.callCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("scan loop never triggered its first scan")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Let the blocked scan complete successfully, which clears the error state.
	close(client.release)

	// Wait for the error state to clear so the next cadence decision is unambiguous.
	deadline = time.Now().Add(2 * time.Second)
	for {
		errorCount, _, _ := manager.GetErrorState()
		if errorCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("error tracking never reset, errorCount=%d", errorCount)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The ticker is at 1s now. Wait for the next tick, where the loop must notice the
	// recovery and restore the 3s cadence.
	time.Sleep(1500 * time.Millisecond)
	before := client.callCount()

	// Observe a window that a 1s cadence would fill with scans and a 3s cadence would not.
	const window = 5 * time.Second
	time.Sleep(window)
	callsInWindow := client.callCount() - before

	// One scan makes two master-client calls. 3s cadence: at most 2 scans (4 calls) in 5s,
	// even if a stall shifts the window. 1s cadence: about 5 scans (10 calls).
	if callsInWindow > 4 {
		t.Errorf("scan loop made %d master-client calls in %v after recovering from a failed scan; "+
			"the ticker was left at the %v backoff instead of returning to the configured %v",
			callsInWindow, window, time.Second, baseInterval)
	}
	if callsInWindow == 0 {
		t.Errorf("scan loop ran no scans in %v, expected the %v cadence to fire at least once", window, baseInterval)
	}
}

// TestScanLoopEntersBackoffOnError checks the other half of the cadence logic: a failing
// scan still has to shorten the ticker, so the fix above did not simply pin the loop to
// the configured interval.
func TestScanLoopEntersBackoffOnError(t *testing.T) {
	config := DefaultMaintenanceConfig()
	config.ScanIntervalSeconds = 600
	manager := NewMaintenanceManager(nil, config, nil)

	baseInterval := time.Duration(config.ScanIntervalSeconds) * time.Second

	if got := manager.getScanInterval(baseInterval); got != baseInterval {
		t.Errorf("healthy scan interval = %v, want the configured %v", got, baseInterval)
	}

	manager.mutex.Lock()
	manager.errorCount = 1
	manager.backoffDelay = time.Second
	manager.mutex.Unlock()

	if got := manager.getScanInterval(baseInterval); got != time.Second {
		t.Errorf("scan interval while failing = %v, want the %v backoff", got, time.Second)
	}

	manager.mutex.Lock()
	manager.resetErrorTracking()
	manager.mutex.Unlock()

	if got := manager.getScanInterval(baseInterval); got != baseInterval {
		t.Errorf("scan interval after recovery = %v, want the configured %v", got, baseInterval)
	}
}

// TestStopIsIdempotent guards the stop channel against a double close, which used to panic
// when StopMaintenanceManager ran twice (for example on a shutdown path that also runs on
// a signal handler).
func TestStopIsIdempotent(t *testing.T) {
	manager := NewMaintenanceManager(nil, DefaultMaintenanceConfig(), nil)

	manager.mutex.Lock()
	manager.running = true
	manager.mutex.Unlock()

	manager.Stop()
	manager.Stop()

	if manager.IsRunning() {
		t.Error("manager still reports running after Stop")
	}
}
