// Command s3_benchmark drives concurrent PUT and GET load against an S3
// gateway and reports throughput and latency. It is a self-contained load
// generator for the performance CI, using the aws-sdk-go-v2 client the rest
// of the S3 tests already depend on.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	endpoint := flag.String("endpoint", "http://localhost:8000", "S3 gateway endpoint")
	accessKey := flag.String("access-key", "some_access_key1", "S3 access key")
	secretKey := flag.String("secret-key", "some_secret_key1", "S3 secret key")
	region := flag.String("region", "us-east-1", "S3 region")
	bucket := flag.String("bucket", "perf-benchmark", "bucket to write into")
	objects := flag.Int("objects", 10000, "number of objects")
	size := flag.Int("size", 1024, "object size in bytes")
	concurrency := flag.Int("concurrency", 16, "concurrent workers")
	mode := flag.String("mode", "both", "write, read, or both")
	flag.Parse()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(*region),
		config.WithRetryMaxAttempts(1),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, "")),
	)
	if err != nil {
		fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})

	if err := ensureBucket(client, *bucket); err != nil {
		fatalf("ensure bucket: %v", err)
	}

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i)
	}

	doWrite := *mode == "both" || *mode == "write"
	doRead := *mode == "both" || *mode == "read"

	if doWrite {
		report("S3 WRITE", *concurrency, *size, run(*concurrency, *objects, func(key string) error {
			_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:        bucket,
				Key:           aws.String(key),
				Body:          bytes.NewReader(payload),
				ContentLength: aws.Int64(int64(*size)),
			})
			return err
		}))
	}

	if doRead {
		report("S3 READ", *concurrency, *size, run(*concurrency, *objects, func(key string) error {
			out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucket,
				Key:    aws.String(key),
			})
			if err != nil {
				return err
			}
			defer out.Body.Close()
			_, err = io.Copy(io.Discard, out.Body)
			return err
		}))
	}
}

type result struct {
	completed int64
	failed    int64
	elapsed   time.Duration
	latencies []time.Duration
	firstErr  error
}

// run fans `total` keyed operations across `workers` goroutines, timing each.
func run(workers, total int, op func(key string) error) result {
	var (
		completed, failed, nextKey atomic.Int64
		mu                         sync.Mutex
		firstErr                   error
		latencies                  = make([]time.Duration, 0, total)
		wg                         sync.WaitGroup
	)
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, total/workers+1)
			for {
				i := nextKey.Add(1) - 1
				if i >= int64(total) {
					break
				}
				key := fmt.Sprintf("obj-%d", i)
				opStart := time.Now()
				err := op(key)
				local = append(local, time.Since(opStart))
				if err != nil {
					failed.Add(1)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				completed.Add(1)
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result{
		completed: completed.Load(),
		failed:    failed.Load(),
		elapsed:   time.Since(start),
		latencies: latencies,
		firstErr:  firstErr,
	}
}

func report(label string, concurrency, size int, r result) {
	secs := r.elapsed.Seconds()
	transferred := r.completed * int64(size)
	rps := 0.0
	kbps := 0.0
	if secs > 0 {
		rps = float64(r.completed) / secs
		kbps = float64(transferred) / 1024 / secs
	}
	fmt.Printf("\n%s results:\n", label)
	fmt.Printf("Concurrency Level:      %d\n", concurrency)
	fmt.Printf("Time taken for tests:   %.3f seconds\n", secs)
	fmt.Printf("Completed requests:     %d\n", r.completed)
	fmt.Printf("Failed requests:        %d\n", r.failed)
	fmt.Printf("Total transferred:      %d bytes\n", transferred)
	fmt.Printf("Requests per second:    %.2f [#/sec]\n", rps)
	fmt.Printf("Transfer rate:          %.2f [Kbytes/sec]\n", kbps)
	p := percentiles(r.latencies)
	fmt.Printf("Latency (ms):           avg=%.2f p50=%.2f p90=%.2f p99=%.2f max=%.2f\n",
		ms(p.avg), ms(p.p50), ms(p.p90), ms(p.p99), ms(p.max))
	if r.failed > 0 && r.firstErr != nil {
		fmt.Printf("First error:            %v\n", r.firstErr)
		os.Exit(1)
	}
}

type latencyStats struct {
	avg, p50, p90, p99, max time.Duration
}

func percentiles(d []time.Duration) latencyStats {
	if len(d) == 0 {
		return latencyStats{}
	}
	slices.Sort(d)
	var sum time.Duration
	for _, v := range d {
		sum += v
	}
	at := func(q float64) time.Duration {
		idx := int(q * float64(len(d)))
		if idx >= len(d) {
			idx = len(d) - 1
		}
		return d[idx]
	}
	return latencyStats{
		avg: sum / time.Duration(len(d)),
		p50: at(0.50),
		p90: at(0.90),
		p99: at(0.99),
		max: d[len(d)-1],
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func ensureBucket(client *s3.Client, bucket string) error {
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return err
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
