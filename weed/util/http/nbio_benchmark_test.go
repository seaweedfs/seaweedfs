package http

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio/nbhttp"
)

// BenchmarkHTTPServerMemory compares memory usage between standard net/http
// and nbio HTTP server implementations with many concurrent connections.
// This is exploratory testing for issue #3884 - evaluating nbio for memory optimization.

const (
	testPayload        = "Hello, World!"
	warmupConnections  = 100
	connectionHoldTime = 100 * time.Millisecond
)

// getMemStats returns the current memory allocation in bytes
func getMemStats() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// findFreePort finds an available port on localhost
func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// TestNbioVsStdHTTPMemory is an exploratory test that compares memory usage
// between standard net/http and nbio for handling many HTTP connections.
func TestNbioVsStdHTTPMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory comparison test in short mode")
	}

	t.Run("StandardHTTP", func(t *testing.T) {
		memBefore := getMemStats()
		testStandardHTTPConnections(t, warmupConnections)
		memAfter := getMemStats()
		if memAfter >= memBefore {
			t.Logf("Standard HTTP: %d connections, memory delta: +%d KB",
				warmupConnections, (memAfter-memBefore)/1024)
		} else {
			t.Logf("Standard HTTP: %d connections, memory delta: -%d KB",
				warmupConnections, (memBefore-memAfter)/1024)
		}
	})

	t.Run("NbioHTTP", func(t *testing.T) {
		memBefore := getMemStats()
		testNbioHTTPConnections(t, warmupConnections)
		memAfter := getMemStats()
		if memAfter >= memBefore {
			t.Logf("Nbio HTTP: %d connections, memory delta: +%d KB",
				warmupConnections, (memAfter-memBefore)/1024)
		} else {
			t.Logf("Nbio HTTP: %d connections, memory delta: -%d KB",
				warmupConnections, (memBefore-memAfter)/1024)
		}
	})
}

// BenchmarkStandardHTTPConnections benchmarks standard net/http server memory usage
func BenchmarkStandardHTTPConnections(b *testing.B) {
	benchmarkHTTPConnections(b, false)
}

// BenchmarkNbioHTTPConnections benchmarks nbio HTTP server memory usage
func BenchmarkNbioHTTPConnections(b *testing.B) {
	benchmarkHTTPConnections(b, true)
}

func benchmarkHTTPConnections(b *testing.B, useNbio bool) {
	connectionCounts := []int{100, 500, 1000}

	for _, connCount := range connectionCounts {
		name := fmt.Sprintf("Connections_%d", connCount)
		b.Run(name, func(b *testing.B) {
			var totalMemDelta uint64
			for i := 0; i < b.N; i++ {
				runtime.GC()
				memBefore := getMemStats()

				if useNbio {
					testNbioHTTPConnections(b, connCount)
				} else {
					testStandardHTTPConnections(b, connCount)
				}

				runtime.GC()
				memAfter := getMemStats()

				if memAfter > memBefore {
					totalMemDelta += memAfter - memBefore
				}
			}

			avgMemDelta := totalMemDelta / uint64(b.N)
			b.ReportMetric(float64(avgMemDelta)/1024, "KB/op")
			b.ReportMetric(float64(avgMemDelta)/float64(connectionCounts[0]), "bytes/conn")
		})
	}
}

// BenchmarkHTTPServerMemoryComparison provides a side-by-side comparison
func BenchmarkHTTPServerMemoryComparison(b *testing.B) {
	connCounts := []int{100, 500, 1000}

	for _, count := range connCounts {
		b.Run(fmt.Sprintf("StdHTTP_%d_conns", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				testStandardHTTPConnections(b, count)
			}
		})

		b.Run(fmt.Sprintf("NbioHTTP_%d_conns", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				testNbioHTTPConnections(b, count)
			}
		})
	}
}

func testStandardHTTPConnections(tb testing.TB, numConnections int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("Failed to create listener: %v", err)
	}

	server := &http.Server{Handler: mux}
	serverDone := make(chan struct{})
	go func() {
		server.Serve(listener)
		close(serverDone)
	}()

	addr := listener.Addr().String()

	makeConnectionsToServer(tb, addr, numConnections)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	<-serverDone
}

func testNbioHTTPConnections(tb testing.TB, numConnections int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	port, err := findFreePort()
	if err != nil {
		tb.Fatalf("Failed to find free port: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	engine := nbhttp.NewEngine(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{addr},
		Handler: mux,
	})

	if err := engine.Start(); err != nil {
		tb.Fatalf("Failed to start nbio engine: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	makeConnectionsToServer(tb, addr, numConnections)

	engine.Stop()
}

func makeConnectionsToServer(tb testing.TB, addr string, numConnections int) {
	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	semaphore := make(chan struct{}, 50)

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			client := &http.Client{
				Timeout: 5 * time.Second,
				Transport: &http.Transport{
					DisableKeepAlives: false,
					MaxIdleConns:      100,
					IdleConnTimeout:   90 * time.Second,
				},
			}

			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}
			defer resp.Body.Close()

			_, err = io.ReadAll(resp.Body)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}

			atomic.AddInt64(&successCount, 1)

			time.Sleep(connectionHoldTime)
		}()
	}

	wg.Wait()

	if testing.Verbose() {
		if t, ok := tb.(*testing.T); ok {
			t.Logf("Connections: success=%d, errors=%d", successCount, errorCount)
		}
	}
}

// BenchmarkNbioHTTPThroughput benchmarks request throughput with nbio
func BenchmarkNbioHTTPThroughput(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	port, err := findFreePort()
	if err != nil {
		b.Fatalf("Failed to find free port: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	engine := nbhttp.NewEngine(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{addr},
		Handler: mux,
	})

	if err := engine.Start(); err != nil {
		b.Fatalf("Failed to start nbio engine: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: false,
			MaxIdleConns:      100,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})

	b.StopTimer()
	engine.Stop()
}

// BenchmarkStandardHTTPThroughput benchmarks request throughput with standard net/http
func BenchmarkStandardHTTPThroughput(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to create listener: %v", err)
	}

	server := &http.Server{Handler: mux}
	go server.Serve(listener)

	addr := listener.Addr().String()
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: false,
			MaxIdleConns:      100,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})

	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

// BenchmarkIdleConnectionMemory measures memory overhead of idle connections
func BenchmarkIdleConnectionMemory(b *testing.B) {
	b.Run("StdHTTP_IdleConns", func(b *testing.B) {
		benchmarkIdleConnectionsStd(b)
	})

	b.Run("Nbio_IdleConns", func(b *testing.B) {
		benchmarkIdleConnectionsNbio(b)
	})
}

func benchmarkIdleConnectionsStd(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to create listener: %v", err)
	}

	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	addr := listener.Addr().String()

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conns := make([]net.Conn, 0, 100)
		for j := 0; j < 100; j++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				continue
			}
			conns = append(conns, conn)
		}

		time.Sleep(10 * time.Millisecond)

		for _, conn := range conns {
			conn.Close()
		}
	}
}

func benchmarkIdleConnectionsNbio(b *testing.B) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testPayload))
	})

	port, err := findFreePort()
	if err != nil {
		b.Fatalf("Failed to find free port: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	engine := nbhttp.NewEngine(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{addr},
		Handler: mux,
	})

	if err := engine.Start(); err != nil {
		b.Fatalf("Failed to start nbio engine: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	defer engine.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conns := make([]net.Conn, 0, 100)
		for j := 0; j < 100; j++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				continue
			}
			conns = append(conns, conn)
		}

		time.Sleep(10 * time.Millisecond)

		for _, conn := range conns {
			conn.Close()
		}
	}
}
