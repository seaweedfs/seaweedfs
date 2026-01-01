package needle

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestNewAsyncRequest verifies AsyncRequest creation
func TestNewAsyncRequest(t *testing.T) {
	testCases := []struct {
		name           string
		isWriteRequest bool
	}{
		{
			name:           "Write request",
			isWriteRequest: true,
		},
		{
			name:           "Read request",
			isWriteRequest: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			n := &Needle{Id: types.NeedleId(123)}
			req := NewAsyncRequest(n, tc.isWriteRequest)

			if req == nil {
				t.Fatal("NewAsyncRequest returned nil")
			}
			if req.N != n {
				t.Error("Needle not set correctly")
			}
			if req.IsWriteRequest != tc.isWriteRequest {
				t.Errorf("IsWriteRequest mismatch: expected %v, got %v", tc.isWriteRequest, req.IsWriteRequest)
			}
			if req.ActualSize != 0 {
				t.Error("ActualSize should be 0")
			}
			if req.doneChan == nil {
				t.Error("doneChan not initialized")
			}
		})
	}
}

// TestAsyncRequest_Complete verifies Complete and WaitComplete
func TestAsyncRequest_Complete(t *testing.T) {
	n := &Needle{Id: types.NeedleId(456)}
	req := NewAsyncRequest(n, true)

	var wg sync.WaitGroup
	wg.Add(1)

	// Goroutine waiting for completion
	var receivedOffset, receivedSize uint64
	var receivedUnchanged bool
	var receivedErr error

	go func() {
		defer wg.Done()
		receivedOffset, receivedSize, receivedUnchanged, receivedErr = req.WaitComplete()
	}()

	// Give goroutine time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Complete the request
	expectedOffset := uint64(1000)
	expectedSize := uint64(2000)
	expectedUnchanged := true
	expectedErr := errors.New("test error")

	req.Complete(expectedOffset, expectedSize, expectedUnchanged, expectedErr)

	// Wait for goroutine to finish
	wg.Wait()

	// Verify received values
	if receivedOffset != expectedOffset {
		t.Errorf("Offset mismatch: expected %d, got %d", expectedOffset, receivedOffset)
	}
	if receivedSize != expectedSize {
		t.Errorf("Size mismatch: expected %d, got %d", expectedSize, receivedSize)
	}
	if receivedUnchanged != expectedUnchanged {
		t.Errorf("Unchanged mismatch: expected %v, got %v", expectedUnchanged, receivedUnchanged)
	}
	if receivedErr != expectedErr {
		t.Errorf("Error mismatch: expected %v, got %v", expectedErr, receivedErr)
	}
}

// TestAsyncRequest_UpdateResult verifies UpdateResult
func TestAsyncRequest_UpdateResult(t *testing.T) {
	n := &Needle{Id: types.NeedleId(789)}
	req := NewAsyncRequest(n, false)

	offset := uint64(3000)
	size := uint64(4000)
	unchanged := false
	err := errors.New("update error")

	req.UpdateResult(offset, size, unchanged, err)

	// Complete to unblock WaitComplete
	req.Submit()

	// Verify values were updated
	gotOffset, gotSize, gotUnchanged, gotErr := req.WaitComplete()
	if gotOffset != offset {
		t.Errorf("Offset: expected %d, got %d", offset, gotOffset)
	}
	if gotSize != size {
		t.Errorf("Size: expected %d, got %d", size, gotSize)
	}
	if gotUnchanged != unchanged {
		t.Errorf("Unchanged: expected %v, got %v", unchanged, gotUnchanged)
	}
	if gotErr != err {
		t.Errorf("Error: expected %v, got %v", err, gotErr)
	}
}

// TestAsyncRequest_Submit verifies Submit functionality
func TestAsyncRequest_Submit(t *testing.T) {
	n := &Needle{Id: types.NeedleId(111)}
	req := NewAsyncRequest(n, true)

	// Update values
	req.UpdateResult(5000, 6000, true, nil)

	// Submit
	req.Submit()

	// WaitComplete should return immediately
	start := time.Now()
	offset, size, unchanged, err := req.WaitComplete()
	elapsed := time.Since(start)

	if elapsed > 10*time.Millisecond {
		t.Errorf("WaitComplete blocked too long: %v", elapsed)
	}

	if offset != 5000 {
		t.Errorf("Offset: expected 5000, got %d", offset)
	}
	if size != 6000 {
		t.Errorf("Size: expected 6000, got %d", size)
	}
	if !unchanged {
		t.Error("Unchanged: expected true")
	}
	if err != nil {
		t.Errorf("Error: expected nil, got %v", err)
	}
}

// TestAsyncRequest_IsSucceed verifies success check
func TestAsyncRequest_IsSucceed(t *testing.T) {
	testCases := []struct {
		name    string
		err     error
		succeed bool
	}{
		{
			name:    "Success - no error",
			err:     nil,
			succeed: true,
		},
		{
			name:    "Failure - with error",
			err:     errors.New("test error"),
			succeed: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			n := &Needle{Id: types.NeedleId(222)}
			req := NewAsyncRequest(n, true)
			req.UpdateResult(0, 0, false, tc.err)

			if req.IsSucceed() != tc.succeed {
				t.Errorf("IsSucceed: expected %v, got %v", tc.succeed, req.IsSucceed())
			}
		})
	}
}

// TestAsyncRequest_MultipleWaiters verifies multiple goroutines can wait
func TestAsyncRequest_MultipleWaiters(t *testing.T) {
	n := &Needle{Id: types.NeedleId(333)}
	req := NewAsyncRequest(n, true)

	numWaiters := 5
	var wg sync.WaitGroup
	wg.Add(numWaiters)

	expectedOffset := uint64(7000)
	expectedSize := uint64(8000)

	// Start multiple waiters
	for i := 0; i < numWaiters; i++ {
		go func(id int) {
			defer wg.Done()
			offset, size, _, _ := req.WaitComplete()
			if offset != expectedOffset {
				t.Errorf("Waiter %d: offset mismatch", id)
			}
			if size != expectedSize {
				t.Errorf("Waiter %d: size mismatch", id)
			}
		}(i)
	}

	// Give waiters time to start
	time.Sleep(20 * time.Millisecond)

	// Complete once
	req.Complete(expectedOffset, expectedSize, false, nil)

	// All waiters should finish
	wg.Wait()
}

// TestAsyncRequest_Timeout verifies waiting with timeout
func TestAsyncRequest_Timeout(t *testing.T) {
	n := &Needle{Id: types.NeedleId(444)}
	req := NewAsyncRequest(n, false)

	done := make(chan bool)

	go func() {
		select {
		case <-time.After(50 * time.Millisecond):
			done <- false // timeout
		case <-time.After(10 * time.Millisecond):
			req.Complete(9000, 10000, false, nil)
			done <- true // completed
		}
	}()

	start := time.Now()
	req.WaitComplete()
	elapsed := time.Since(start)

	result := <-done
	if !result {
		t.Error("Request timed out")
	}
	if elapsed > 30*time.Millisecond {
		t.Errorf("Took too long: %v", elapsed)
	}
}

// TestAsyncRequest_ActualSize verifies ActualSize field
func TestAsyncRequest_ActualSize(t *testing.T) {
	n := &Needle{
		Id:       types.NeedleId(555),
		DataSize: 12345,
	}
	req := NewAsyncRequest(n, true)

	if req.ActualSize != 0 {
		t.Errorf("Initial ActualSize: expected 0, got %d", req.ActualSize)
	}

	// Simulate setting ActualSize
	req.ActualSize = 67890
	if req.ActualSize != 67890 {
		t.Errorf("ActualSize: expected 67890, got %d", req.ActualSize)
	}
}

// BenchmarkNewAsyncRequest benchmarks request creation
func BenchmarkNewAsyncRequest(b *testing.B) {
	n := &Needle{Id: types.NeedleId(1)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewAsyncRequest(n, true)
	}
}

// BenchmarkAsyncRequest_Complete benchmarks completion
func BenchmarkAsyncRequest_Complete(b *testing.B) {
	n := &Needle{Id: types.NeedleId(1)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := NewAsyncRequest(n, true)
		go req.WaitComplete()
		req.Complete(1000, 2000, false, nil)
	}
}

