package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// replayMaster stands in for the master while a filer replays its metadata
// log. KeepConnected announces either nothing, which is what a filer's volume
// map holds right after it starts (registration and the volume list come only
// once the filer has caught up with its peers), or the volumes on announced.
// LookupVolume answers with the volume server, says a volume in gone does not
// exist, or fails without an answer while failing is set.
type replayMaster struct {
	master_pb.UnimplementedSeaweedServer
	volumeUrl     string
	announced     string // volume server url announced with announcedVids
	announcedVids []uint32
	gone          map[string]bool
	failing       atomic.Bool
	lookups       atomic.Int32
}

func (m *replayMaster) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	resp := &master_pb.KeepConnectedResponse{}
	if len(m.announcedVids) > 0 {
		resp.VolumeLocation = &master_pb.VolumeLocation{Url: m.announced, PublicUrl: m.announced, NewVids: m.announcedVids}
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (m *replayMaster) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	m.lookups.Add(1)
	if m.failing.Load() {
		// Not Unavailable, which the client retries for 30 s.
		return nil, status.Error(codes.Internal, "master cannot look volumes up")
	}
	resp := &master_pb.LookupVolumeResponse{}
	for _, id := range req.VolumeOrFileIds {
		vid, _, _ := strings.Cut(id, ",")
		loc := &master_pb.LookupVolumeResponse_VolumeIdLocation{VolumeOrFileId: id}
		if m.gone[vid] {
			loc.Error = fmt.Sprintf("volume id %s not found", vid)
		} else {
			loc.Locations = []*master_pb.Location{{Url: m.volumeUrl, PublicUrl: m.volumeUrl}}
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, loc)
	}
	return resp, nil
}

// logVolumeServer serves log chunks by file id and counts the reads.
func logVolumeServer(t *testing.T, chunks map[string][]byte) (url string, reads *atomic.Int32) {
	t.Helper()
	reads = &atomic.Int32{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		data, found := chunks[strings.TrimPrefix(r.URL.Path, "/")]
		mu.Unlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		reads.Add(1)
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://"), reads
}

// connectReplayMaster serves m over gRPC and returns a filer-type master
// client connected to it. The client's volume map holds only what m announces.
func connectReplayMaster(t *testing.T, m *replayMaster) *wdclient.MasterClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, m)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	mc := wdclient.NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", cluster.FilerType, pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))}),
	)
	connecting, stopConnecting := context.WithCancel(context.Background())
	t.Cleanup(stopConnecting)
	go mc.KeepConnectedToMaster(connecting)
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWaiting()
	mc.WaitUntilConnected(waiting)
	if waiting.Err() != nil {
		t.Fatal("the master client never connected to the fake master")
	}
	if len(m.announcedVids) > 0 {
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if _, found := mc.GetLocations(m.announcedVids[0]); found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the announced volumes never reached the volume map")
			}
		}
	}
	return mc
}

// replayQueue builds a per-filer log file iterator over files, in order, with
// no further days to collect.
func replayQueue(mc *wdclient.MasterClient, files ...*Entry) (*LogFileQueueIterator, *OrderedLogVisitor) {
	q := util.NewQueue[*LogFileEntry]()
	for _, file := range files {
		q.Enqueue(&LogFileEntry{TsNs: 0, FileEntry: file})
	}
	iter := newLogFileQueueIterator(mc, newPersistedLogCache(persistedLogCacheMaxBytes), q, 0, 0)
	v := &OrderedLogVisitor{logFileEntryCollector: &LogFileEntryCollector{dayEntryQueue: util.NewQueue[*Entry]()}}
	return iter, v
}

func replayAll(iter *LogFileQueueIterator, v *OrderedLogVisitor) (got []int64, err error) {
	for {
		entry, nextErr := iter.getNext(v)
		if nextErr == io.EOF {
			return got, nil
		}
		if nextErr != nil {
			return got, nextErr
		}
		got = append(got, entry.TsNs)
	}
}

func logChunk(fileId string, data []byte) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileId, Offset: 0, Size: uint64(len(data)), ModifiedTsNs: 100}
}

// Right after a filer starts, its volume map is empty until it registers with
// the master, which it does only once it has caught up with its peers. A
// subscriber that resumes in that window replays log files whose volumes are
// all alive; reading a chunk must ask the master rather than take the empty
// map's "volume not found" as the volume being deleted.
func TestLogChunkOfAVolumeMissingFromTheMapIsReadThroughTheMaster(t *testing.T) {
	data := encodeLogRecords(t, logEntriesAt(10, 20))
	volumeUrl, reads := logVolumeServer(t, map[string][]byte{"3,01": data})
	m := &replayMaster{volumeUrl: volumeUrl}
	mc := connectReplayMaster(t, m)

	entries, cacheable, err := loadLogFileEntries(mc, logChunk("3,01", data))
	if err != nil {
		t.Fatalf("load a log chunk of a volume the map does not hold yet: %v", err)
	}
	if !cacheable || len(entries) != 2 || entries[0].TsNs != 10 || entries[1].TsNs != 20 {
		t.Fatalf("loaded %d entries (cacheable %v), want ts 10 and 20", len(entries), cacheable)
	}
	if m.lookups.Load() == 0 || reads.Load() != 1 {
		t.Fatalf("master lookups %d, volume reads %d: want the master asked and one read", m.lookups.Load(), reads.Load())
	}
}

// A read that fails at the cached location sets the volume's locations aside
// and looks the volume up again. Asked only of the map, the volume is then
// missing, and the replay skipped the rest of the file although the master
// knew where the volume lives now.
func TestLogChunkReadFailingAtACachedLocationIsRetriedWhereTheMasterSays(t *testing.T) {
	data := encodeLogRecords(t, logEntriesAt(10, 20))
	volumeUrl, reads := logVolumeServer(t, map[string][]byte{"3,01": data})
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadUrl := dead.Addr().String()
	dead.Close() // nothing answers there any more

	m := &replayMaster{volumeUrl: volumeUrl, announced: deadUrl, announcedVids: []uint32{3}}
	mc := connectReplayMaster(t, m)
	iter, v := replayQueue(mc, logFileEntry(logChunk("3,01", data)))

	got, err := replayAll(iter, v)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint([]int64{10, 20}) {
		t.Fatalf("replayed %v, want [10 20]: the file was skipped", got)
	}
	if m.lookups.Load() == 0 || reads.Load() != 1 {
		t.Fatalf("master lookups %d, volume reads %d: want the master asked and one read", m.lookups.Load(), reads.Load())
	}
}

// A volume the master says does not exist is gone for good; waiting on it
// would park every subscriber that replays past it, so the file is skipped
// and the next one is read.
func TestReplaySkipsALogFileWhoseVolumeTheMasterSaysIsGone(t *testing.T) {
	gone := encodeLogRecords(t, logEntriesAt(10, 20))
	next := encodeLogRecords(t, logEntriesAt(30, 40))
	volumeUrl, _ := logVolumeServer(t, map[string][]byte{"3,01": gone, "4,01": next})
	m := &replayMaster{volumeUrl: volumeUrl, gone: map[string]bool{"3": true}}
	mc := connectReplayMaster(t, m)
	iter, v := replayQueue(mc, logFileEntry(logChunk("3,01", gone)), logFileEntry(logChunk("4,01", next)))

	got, err := replayAll(iter, v)
	if err != nil {
		t.Fatalf("replay past a deleted volume: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint([]int64{30, 40}) {
		t.Fatalf("replayed %v, want only the next file's [30 40]", got)
	}
}

// A lookup the master did not answer also reads "volume N not found", but the
// volume may be fine. Skipping would drop the file's events for good; the
// replay must stop instead, so the subscriber resumes from here.
func TestReplayStopsWhenTheMasterGivesNoAnswer(t *testing.T) {
	first := encodeLogRecords(t, logEntriesAt(10, 20))
	next := encodeLogRecords(t, logEntriesAt(30, 40))
	volumeUrl, reads := logVolumeServer(t, map[string][]byte{"3,01": first, "4,01": next})
	m := &replayMaster{volumeUrl: volumeUrl}
	m.failing.Store(true)
	mc := connectReplayMaster(t, m)
	iter, v := replayQueue(mc, logFileEntry(logChunk("3,01", first)), logFileEntry(logChunk("4,01", next)))

	got, err := replayAll(iter, v)
	if err == nil {
		t.Fatalf("replay without answers from the master ended cleanly with %v: the files were skipped", got)
	}
	if !errors.Is(err, wdclient.ErrLookupUnanswered) {
		t.Fatalf("replay error %q is not marked as an unanswered lookup", err)
	}
	if len(got) != 0 || reads.Load() != 0 {
		t.Fatalf("replayed %v with %d volume reads before stopping, want nothing", got, reads.Load())
	}

	// Once the master answers again, the same files replay in full.
	m.failing.Store(false)
	iter, v = replayQueue(mc, logFileEntry(logChunk("3,01", first)), logFileEntry(logChunk("4,01", next)))
	got, err = replayAll(iter, v)
	if err != nil || fmt.Sprint(got) != fmt.Sprint([]int64{10, 20, 30, 40}) {
		t.Fatalf("replay after the master answers again: %v, %v", got, err)
	}
}
