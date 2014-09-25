package main

import (
	"bufio"
	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/operation"
	"github.com/chrislusf/weed-fs/go/util"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
)

type BenchmarkOptions struct {
	server           *string
	concurrency      *int
	numberOfFiles    *int
	fileSize         *int
	idListFile       *string
	write            *bool
	deletePercentage *int
	read             *bool
	sequentialRead   *bool
	collection       *string
	cpuprofile       *string
	vid2server       map[string]string //cache for vid locations
}

var (
	b BenchmarkOptions
)

func init() {
	cmdBenchmark.Run = runbenchmark // break init cycle
	cmdBenchmark.IsDebug = cmdBenchmark.Flag.Bool("debug", false, "verbose debug information")
	b.server = cmdBenchmark.Flag.String("server", "localhost:9333", "weedfs master location")
	b.concurrency = cmdBenchmark.Flag.Int("c", 16, "number of concurrent write or read processes")
	b.fileSize = cmdBenchmark.Flag.Int("size", 1024, "simulated file size in bytes, with random(0~63) bytes padding")
	b.numberOfFiles = cmdBenchmark.Flag.Int("n", 1024*1024, "number of files to write for each thread")
	b.idListFile = cmdBenchmark.Flag.String("list", os.TempDir()+"/benchmark_list.txt", "list of uploaded file ids")
	b.write = cmdBenchmark.Flag.Bool("write", true, "enable write")
	b.deletePercentage = cmdBenchmark.Flag.Int("deletePercent", 0, "the percent of writes that are deletes")
	b.read = cmdBenchmark.Flag.Bool("read", true, "enable read")
	b.sequentialRead = cmdBenchmark.Flag.Bool("readSequentially", false, "randomly read by ids from \"-list\" specified file")
	b.collection = cmdBenchmark.Flag.String("collection", "benchmark", "write data to this collection")
	b.cpuprofile = cmdBenchmark.Flag.String("cpuprofile", "", "write cpu profile to file")
	b.vid2server = make(map[string]string)
}

var cmdBenchmark = &Command{
	UsageLine: "benchmark -server=localhost:9333 -c=10 -n=100000",
	Short:     "benchmark on writing millions of files and read out",
	Long: `benchmark on an empty weed file system.
  
  Two tests during benchmark:
  1) write lots of small files to the system
  2) read the files out
  
  The file content is mostly zero, but no compression is done.
    
  You can choose to only benchmark read or write.
  During write, the list of uploaded file ids is stored in "-list" specified file.
  You can also use your own list of file ids to run read test.
  
  Write speed and read speed will be collected.
  The numbers are used to get a sense of the system.
  Usually your network or the hard drive is the real bottleneck.
  
  Another thing to watch is whether the volumes are evenly distributed
  to each volume server. Because the 7 more benchmark volumes are randomly distributed
  to servers with free slots, it's highly possible some servers have uneven amount of
  benchmark volumes. To remedy this, you can use this to grow the benchmark volumes 
  before starting the benchmark command:
    http://localhost:9333/vol/grow?collection=benchmark&count=5

  After benchmarking, you can clean up the written data by deleting the benchmark collection
    http://localhost:9333/col/delete?collection=benchmark

  `,
}

var (
	wait            sync.WaitGroup
	writeStats      *stats
	readStats       *stats
	serverLimitChan map[string]chan bool
)

func init() {
	serverLimitChan = make(map[string]chan bool)
}

func runbenchmark(cmd *Command, args []string) bool {
	fmt.Printf("This is Seaweed File System version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *b.cpuprofile != "" {
		f, err := os.Create(*b.cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *b.write {
		bench_write()
	}

	if *b.read {
		bench_read()
	}

	return true
}

func bench_write() {
	fileIdLineChan := make(chan string)
	finishChan := make(chan bool)
	writeStats = newStats()
	idChan := make(chan int)
	wait.Add(*b.concurrency)
	go writeFileIds(*b.idListFile, fileIdLineChan, finishChan)
	for i := 0; i < *b.concurrency; i++ {
		go writeFiles(idChan, fileIdLineChan, writeStats)
	}
	writeStats.start = time.Now()
	writeStats.total = *b.numberOfFiles
	go writeStats.checkProgress("Writing Benchmark", finishChan)
	for i := 0; i < *b.numberOfFiles; i++ {
		idChan <- i
	}
	close(idChan)
	wait.Wait()
	writeStats.end = time.Now()
	wait.Add(1)
	finishChan <- true
	finishChan <- true
	close(finishChan)
	wait.Wait()
	writeStats.printStats()
}

func bench_read() {
	fileIdLineChan := make(chan string)
	finishChan := make(chan bool)
	readStats = newStats()
	wait.Add(*b.concurrency)
	go readFileIds(*b.idListFile, fileIdLineChan)
	readStats.start = time.Now()
	readStats.total = *b.numberOfFiles
	go readStats.checkProgress("Randomly Reading Benchmark", finishChan)
	for i := 0; i < *b.concurrency; i++ {
		go readFiles(fileIdLineChan, readStats)
	}
	wait.Wait()
	finishChan <- true
	close(finishChan)
	readStats.end = time.Now()
	readStats.printStats()
}

type delayedFile struct {
	enterTime time.Time
	fp        *operation.FilePart
}

func writeFiles(idChan chan int, fileIdLineChan chan string, s *stats) {
	delayedDeleteChan := make(chan *delayedFile, 100)
	var waitForDeletions sync.WaitGroup
	for i := 0; i < 7; i++ {
		go func() {
			waitForDeletions.Add(1)
			for df := range delayedDeleteChan {
				if df == nil {
					break
				}
				if df.enterTime.After(time.Now()) {
					time.Sleep(df.enterTime.Sub(time.Now()))
				}
				fp := df.fp
				serverLimitChan[fp.Server] <- true
				if e := util.Delete("http://" + fp.Server + "/" + fp.Fid); e == nil {
					s.completed++
				} else {
					s.failed++
				}
				<-serverLimitChan[fp.Server]
			}
			waitForDeletions.Done()
		}()
	}

	for {
		if id, ok := <-idChan; ok {
			start := time.Now()
			fileSize := int64(*b.fileSize + rand.Intn(64))
			fp := &operation.FilePart{Reader: &FakeReader{id: uint64(id), size: fileSize}, FileSize: fileSize}
			if assignResult, err := operation.Assign(*b.server, 1, "", *b.collection, ""); err == nil {
				fp.Server, fp.Fid, fp.Collection = assignResult.PublicUrl, assignResult.Fid, *b.collection
				if _, ok := serverLimitChan[fp.Server]; !ok {
					serverLimitChan[fp.Server] = make(chan bool, 7)
				}
				serverLimitChan[fp.Server] <- true
				if _, err := fp.Upload(0, *b.server); err == nil {
					if rand.Intn(100) < *b.deletePercentage {
						s.total++
						delayedDeleteChan <- &delayedFile{time.Now().Add(time.Second), fp}
					} else {
						fileIdLineChan <- fp.Fid
					}
					s.completed++
					s.transferred += fileSize
				} else {
					s.failed++
				}
				writeStats.addSample(time.Now().Sub(start))
				<-serverLimitChan[fp.Server]
				if *cmdBenchmark.IsDebug {
					fmt.Printf("writing %d file %s\n", id, fp.Fid)
				}
			} else {
				s.failed++
				println("writing file error:", err.Error())
			}
		} else {
			break
		}
	}
	close(delayedDeleteChan)
	waitForDeletions.Wait()
	wait.Done()
}

func readFiles(fileIdLineChan chan string, s *stats) {
	serverLimitChan := make(map[string]chan bool)
	masterLimitChan := make(chan bool, 1)
	for {
		if fid, ok := <-fileIdLineChan; ok {
			if len(fid) == 0 {
				continue
			}
			if fid[0] == '#' {
				continue
			}
			if *cmdBenchmark.IsDebug {
				fmt.Printf("reading file %s\n", fid)
			}
			parts := strings.SplitN(fid, ",", 2)
			vid := parts[0]
			start := time.Now()
			if server, ok := b.vid2server[vid]; !ok {
				masterLimitChan <- true
				if _, now_ok := b.vid2server[vid]; !now_ok {
					if ret, err := operation.Lookup(*b.server, vid); err == nil {
						if len(ret.Locations) > 0 {
							server = ret.Locations[0].PublicUrl
							b.vid2server[vid] = server
						}
					}
				}
				<-masterLimitChan
			}
			if server, ok := b.vid2server[vid]; ok {
				if _, ok := serverLimitChan[server]; !ok {
					serverLimitChan[server] = make(chan bool, 7)
				}
				serverLimitChan[server] <- true
				url := "http://" + server + "/" + fid
				if bytesRead, err := util.Get(url); err == nil {
					s.completed++
					s.transferred += int64(len(bytesRead))
					readStats.addSample(time.Now().Sub(start))
				} else {
					s.failed++
					println("!!!! Failed to read from ", url, " !!!!!")
				}
				<-serverLimitChan[server]
			} else {
				s.failed++
				println("!!!! volume id ", vid, " location not found!!!!!")
			}
		} else {
			break
		}
	}
	wait.Done()
}

func writeFileIds(fileName string, fileIdLineChan chan string, finishChan chan bool) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.Fatalf("File to create file %s: %s\n", fileName, err)
	}
	defer file.Close()

	for {
		select {
		case <-finishChan:
			wait.Done()
			return
		case line := <-fileIdLineChan:
			file.Write([]byte(line))
			file.Write([]byte("\n"))
		}
	}
}

func readFileIds(fileName string, fileIdLineChan chan string) {
	file, err := os.Open(fileName) // For read access.
	if err != nil {
		glog.Fatalf("File to read file %s: %s\n", fileName, err)
	}
	defer file.Close()

	r := bufio.NewReader(file)
	if *b.sequentialRead {
		for {
			if line, err := Readln(r); err == nil {
				fileIdLineChan <- string(line)
			} else {
				break
			}
		}
	} else {
		lines := make([]string, 0, readStats.total)
		for {
			if line, err := Readln(r); err == nil {
				lines = append(lines, string(line))
			} else {
				break
			}
		}
		if len(lines) > 0 {
			for i := 0; i < readStats.total; i++ {
				fileIdLineChan <- lines[rand.Intn(len(lines))]
			}
		}
	}

	close(fileIdLineChan)
}

const (
	benchResolution = 10000 //0.1 microsecond
	benchBucket     = 1000000000 / benchResolution
)

// An efficient statics collecting and rendering
type stats struct {
	data        []int
	overflow    []int
	completed   int
	failed      int
	total       int
	transferred int64
	start       time.Time
	end         time.Time
}

var percentages = []int{50, 66, 75, 80, 90, 95, 98, 99, 100}

func newStats() *stats {
	return &stats{data: make([]int, benchResolution), overflow: make([]int, 0)}
}

func (s *stats) addSample(d time.Duration) {
	index := int(d / benchBucket)
	if index < 0 {
		fmt.Printf("This request takes %3.1f seconds, skipping!\n", float64(index)/10000)
	} else if index < len(s.data) {
		s.data[int(d/benchBucket)]++
	} else {
		s.overflow = append(s.overflow, index)
	}
}

func (s *stats) checkProgress(testName string, finishChan chan bool) {
	fmt.Printf("\n------------ %s ----------\n", testName)
	ticker := time.Tick(time.Second)
	lastCompleted, lastTransferred, lastTime := 0, int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, transferred, taken := s.completed-lastCompleted, s.transferred-lastTransferred, t.Sub(lastTime)
			fmt.Printf("Completed %d of %d requests, %3.1f%% %3.1f/s %3.1fMB/s\n",
				s.completed, s.total, float64(s.completed)*100/float64(s.total),
				float64(completed)*float64(int64(time.Second))/float64(int64(taken)),
				float64(transferred)*float64(int64(time.Second))/float64(int64(taken))/float64(1024*1024),
			)
			lastCompleted, lastTransferred, lastTime = s.completed, s.transferred, t
		}
	}
}

func (s *stats) printStats() {
	timeTaken := float64(int64(s.end.Sub(s.start))) / 1000000000
	fmt.Printf("\nConcurrency Level:      %d\n", *b.concurrency)
	fmt.Printf("Time taken for tests:   %.3f seconds\n", timeTaken)
	fmt.Printf("Complete requests:      %d\n", s.completed)
	fmt.Printf("Failed requests:        %d\n", s.failed)
	fmt.Printf("Total transferred:      %d bytes\n", s.transferred)
	fmt.Printf("Requests per second:    %.2f [#/sec]\n", float64(s.completed)/timeTaken)
	fmt.Printf("Transfer rate:          %.2f [Kbytes/sec]\n", float64(s.transferred)/1024/timeTaken)
	n, sum := 0, 0
	min, max := 10000000, 0
	for i := 0; i < len(s.data); i++ {
		n += s.data[i]
		sum += s.data[i] * i
		if s.data[i] > 0 {
			if min > i {
				min = i
			}
			if max < i {
				max = i
			}
		}
	}
	n += len(s.overflow)
	for i := 0; i < len(s.overflow); i++ {
		sum += s.overflow[i]
		if min > s.overflow[i] {
			min = s.overflow[i]
		}
		if max < s.overflow[i] {
			max = s.overflow[i]
		}
	}
	avg := float64(sum) / float64(n)
	varianceSum := 0.0
	for i := 0; i < len(s.data); i++ {
		if s.data[i] > 0 {
			d := float64(i) - avg
			varianceSum += d * d * float64(s.data[i])
		}
	}
	for i := 0; i < len(s.overflow); i++ {
		d := float64(s.overflow[i]) - avg
		varianceSum += d * d
	}
	std := math.Sqrt(varianceSum / float64(n))
	fmt.Printf("\nConnection Times (ms)\n")
	fmt.Printf("              min      avg        max      std\n")
	fmt.Printf("Total:        %2.1f      %3.1f       %3.1f      %3.1f\n", float32(min)/10, float32(avg)/10, float32(max)/10, std/10)
	//printing percentiles
	fmt.Printf("\nPercentage of the requests served within a certain time (ms)\n")
	percentiles := make([]int, len(percentages))
	for i := 0; i < len(percentages); i++ {
		percentiles[i] = n * percentages[i] / 100
	}
	percentiles[len(percentiles)-1] = n
	percentileIndex := 0
	currentSum := 0
	for i := 0; i < len(s.data); i++ {
		currentSum += s.data[i]
		if s.data[i] > 0 && percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
			fmt.Printf("  %3d%%    %5.1f ms\n", percentages[percentileIndex], float32(i)/10.0)
			percentileIndex++
			for percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
				percentileIndex++
			}
		}
	}
	sort.Ints(s.overflow)
	for i := 0; i < len(s.overflow); i++ {
		currentSum++
		if percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
			fmt.Printf("  %3d%%    %5.1f ms\n", percentages[percentileIndex], float32(s.overflow[i])/10.0)
			percentileIndex++
			for percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
				percentileIndex++
			}
		}
	}
}

// a fake reader to generate content to upload
type FakeReader struct {
	id   uint64 // an id number
	size int64  // max bytes
}

func (l *FakeReader) Read(p []byte) (n int, err error) {
	if l.size <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.size {
		n = int(l.size)
	} else {
		n = len(p)
	}
	for i := 0; i < n-8; i += 8 {
		for s := uint(0); s < 8; s++ {
			p[i] = byte(l.id >> (s * 8))
		}
	}
	l.size -= int64(n)
	return
}

func Readln(r *bufio.Reader) ([]byte, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return ln, err
}
