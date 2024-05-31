package operation

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func BenchmarkWithConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 10, 100, 1000}

	ap, _ := NewAssignProxy(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress("localhost:9333")
	}, grpc.WithInsecure(), 16)

	for _, concurrency := range concurrencyLevels {
		b.Run(
			fmt.Sprintf("Concurrency-%d", concurrency),
			func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					done := make(chan struct{})
					startTime := time.Now()

					for j := 0; j < concurrency; j++ {
						go func() {

							ap.Assign(&VolumeAssignRequest{
								Count: 1,
							})

							done <- struct{}{}
						}()
					}

					for j := 0; j < concurrency; j++ {
						<-done
					}

					duration := time.Since(startTime)
					b.Logf("Concurrency: %d, Duration: %v", concurrency, duration)
				}
			},
		)
	}
}

func BenchmarkStreamAssign(b *testing.B) {
	ap, _ := NewAssignProxy(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress("localhost:9333")
	}, grpc.WithInsecure(), 16)
	for i := 0; i < b.N; i++ {
		ap.Assign(&VolumeAssignRequest{
			Count: 1,
		})
	}
}

func BenchmarkUnaryAssign(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Assign(func(_ context.Context) pb.ServerAddress {
			return pb.ServerAddress("localhost:9333")
		}, grpc.WithInsecure(), &VolumeAssignRequest{
			Count: 1,
		})
	}
}
