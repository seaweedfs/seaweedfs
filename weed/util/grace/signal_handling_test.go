//go:build !plan9

package grace

import (
	"reflect"
	"testing"
)

func TestOnInterruptFirstRunsBeforeEarlierHooks(t *testing.T) {
	interruptHookLock.Lock()
	savedHooks, savedFirst := interruptHooks, firstInterruptHooks
	interruptHooks, firstInterruptHooks = nil, nil
	interruptHookLock.Unlock()
	t.Cleanup(func() {
		interruptHookLock.Lock()
		interruptHooks, firstInterruptHooks = savedHooks, savedFirst
		interruptHookLock.Unlock()
	})

	var order []string
	OnInterrupt(func() { order = append(order, "volume") })
	OnInterruptFirst(func() { order = append(order, "s3 wait") })
	OnInterrupt(func() { order = append(order, "s3") })
	OnInterruptFirst(func() { order = append(order, "second wait") })

	runInterruptHooks()

	want := []string{"s3 wait", "second wait", "s3", "volume"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("hooks ran as %v, want %v", order, want)
	}
}
