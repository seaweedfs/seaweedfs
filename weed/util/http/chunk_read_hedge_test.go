package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// shortHedgeDelay makes chunk reads hedge after d of quiet, before and after
// the response headers alike, and starts the test with a full hedge budget.
func shortHedgeDelay(t *testing.T, d time.Duration) {
	t.Helper()
	hedgeDelays(t, d, d)
}

func hedgeDelays(t *testing.T, answer, stall time.Duration) {
	t.Helper()
	savedAnswer, savedStall, savedTokens := chunkReadAnswerDelay, chunkReadStallDelay, hedgeTokens.Load()
	chunkReadAnswerDelay, chunkReadStallDelay = answer, stall
	hedgeTokens.Store(hedgeTokenBurst)
	t.Cleanup(func() {
		chunkReadAnswerDelay, chunkReadStallDelay = savedAnswer, savedStall
		hedgeTokens.Store(savedTokens)
	})
}

// chunkServer serves one chunk. Each request is answered by behave, which gets
// the request's 1-based number at this server.
type chunkServer struct {
	*httptest.Server
	requests  atomic.Int32
	abandoned atomic.Int32 // requests whose client went away before the end
}

func newChunkServer(t *testing.T, behave func(n int32, w http.ResponseWriter, r *http.Request)) *chunkServer {
	t.Helper()
	s := &chunkServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		behave(s.requests.Add(1), w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *chunkServer) url() string { return s.URL + "/3,abc" }

// wait blocks until the client gives up on r, and counts that.
func (s *chunkServer) wait(r *http.Request) {
	<-r.Context().Done()
	s.abandoned.Add(1)
}

func sendHeaders(w http.ResponseWriter, body []byte, gzipped bool) {
	if gzipped {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
}

func flush(w http.ResponseWriter) { w.(http.Flusher).Flush() }

// stallsAfter sends the headers and the first part of body, then sends
// nothing more until the client gives up: a response whose next TCP segment
// was lost.
func stallsAfter(s **chunkServer, body []byte, part int, gzipped bool) func(n int32, w http.ResponseWriter, r *http.Request) {
	return func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, body, gzipped)
		w.Write(body[:part])
		flush(w)
		(*s).wait(r)
	}
}

func answers(body []byte, gzipped bool) func(n int32, w http.ResponseWriter, r *http.Request) {
	return func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, body, gzipped)
		w.Write(body)
	}
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// readPath is one of the ways RetriedFetchChunkData reads a chunk.
type readPath struct {
	name        string
	body        []byte // what the volume server sends
	cipherKey   []byte
	isGzipped   bool
	isFullChunk bool
}

func readPaths(t *testing.T, payload []byte) []readPath {
	cipherKey := util.GenCipherKey()
	encrypted, err := util.Encrypt(payload, cipherKey)
	if err != nil {
		t.Fatal(err)
	}
	return []readPath{
		{name: "direct", body: payload, isFullChunk: true},
		{name: "range", body: payload},
		{name: "gzip", body: gzipped(t, payload), isGzipped: true, isFullChunk: true},
		{name: "encrypted", body: encrypted, cipherKey: cipherKey, isFullChunk: true},
	}
}

func fetch(ctx context.Context, p readPath, size int, urls []string, refresh RefreshUrlsFunc) ([]byte, time.Duration, error) {
	buffer := make([]byte, size)
	start := time.Now()
	n, err := RetriedFetchChunkData(ctx, buffer, urls, p.cipherKey, p.isGzipped, p.isFullChunk, 0, "3,abc", refresh)
	return buffer[:n], time.Since(start), err
}

func testPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

// TestChunkReadHedgesABodyThatStalls is the read the hedge is for: the first
// replica sends part of the chunk and then nothing, as a response does while
// it waits for a TCP retransmission, and the second replica is fine. The read
// must come from the second replica a hedge delay later instead of whenever
// the first one recovers, and give up on the first one.
func TestChunkReadHedgesABodyThatStalls(t *testing.T) {
	payload := testPayload(256 * 1024)
	for _, p := range readPaths(t, payload) {
		t.Run(p.name, func(t *testing.T) {
			forgetUnreachable(t)
			shortHedgeDelay(t, 100*time.Millisecond)
			var stalling *chunkServer
			stalling = newChunkServer(t, stallsAfter(&stalling, p.body, len(p.body)/2, p.isGzipped))
			live := newChunkServer(t, answers(p.body, p.isGzipped))

			refreshed := 0
			got, took, err := fetch(context.Background(), p, len(payload), []string{stalling.url(), live.url()}, func() []string {
				refreshed++
				return nil
			})
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("got %d bytes, %v; want the %d byte chunk", len(got), err, len(payload))
			}
			if took < chunkReadStallDelay || took > chunkReadStallDelay+time.Second {
				t.Fatalf("read took %v, want a little over the %v hedge delay", took, chunkReadStallDelay)
			}
			waitFor(t, "the stalled request to be given up", func() bool { return stalling.abandoned.Load() == 1 })
			if n := live.requests.Load(); n != 1 {
				t.Fatalf("second location asked %d times, want once", n)
			}
			if refreshed != 0 {
				t.Fatalf("refresh called %d times; a stalled body is no reason to look the chunk up again", refreshed)
			}
			// a location that answered and then stalled is not unreachable
			assertOrder(t, ReachableFirst([]string{stalling.url(), live.url()}), stalling.url(), live.url())
		})
	}
}

// TestChunkReadHedgesALocationThatDoesNotAnswer: the first replica accepts the
// request and sends nothing, as one on a hung or dead host does. The read moves
// on a hedge delay later, the silent location is tried last afterwards, and the
// location list is looked up again, as after a failed location.
func TestChunkReadHedgesALocationThatDoesNotAnswer(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(64 * 1024)
	var silent *chunkServer
	silent = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) { silent.wait(r) })
	live := newChunkServer(t, answers(payload, false))
	p := readPath{name: "direct", body: payload, isFullChunk: true}

	urls := []string{silent.url(), live.url()}
	refreshed := 0
	got, took, err := fetch(context.Background(), p, len(payload), urls, func() []string {
		refreshed++
		return nil
	})
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if refreshed != 1 {
		t.Fatalf("refresh called %d times, want once after a location did not answer", refreshed)
	}
	if took < chunkReadStallDelay || took > chunkReadStallDelay+time.Second {
		t.Fatalf("read took %v, want a little over the %v hedge delay", took, chunkReadStallDelay)
	}
	assertOrder(t, ReachableFirst(urls), live.url(), silent.url())

	// the next read starts with the location that answers and does not wait
	got, took, err = fetch(context.Background(), p, len(payload), ReachableFirst(urls), nil)
	if err != nil || !bytes.Equal(got, payload) || took >= chunkReadStallDelay {
		t.Fatalf("second read: got %d bytes, %v in %v; want the chunk at once", len(got), err, took)
	}
	if n := silent.requests.Load(); n != 1 {
		t.Fatalf("silent location asked %d times, want once", n)
	}
}

// TestChunkReadDoesNotHedgeAReadThatKeepsArriving: a slow location that keeps
// sending is left alone, however long the whole chunk takes.
func TestChunkReadDoesNotHedgeAReadThatKeepsArriving(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(64 * 1024)
	const pieces = 8
	slow := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, payload, false)
		piece := len(payload) / pieces
		for i := 0; i < pieces; i++ {
			time.Sleep(chunkReadStallDelay / 3)
			w.Write(payload[i*piece : (i+1)*piece])
			flush(w)
		}
	})
	other := newChunkServer(t, answers(payload, false))

	for _, p := range readPaths(t, payload)[:2] {
		got, took, err := fetch(context.Background(), p, len(payload), []string{slow.url(), other.url()}, nil)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s: got %d bytes, %v; want the chunk", p.name, len(got), err)
		}
		if took < 2*chunkReadStallDelay {
			t.Fatalf("%s: read took %v; the slow location should have been read to the end", p.name, took)
		}
	}
	if n := other.requests.Load(); n != 0 {
		t.Fatalf("second location asked %d times, want never", n)
	}
}

// TestChunkReadKeepsTheFirstAnswerToArrive: once a hedge is under way, the
// first location may still win, and then the hedge is the one given up.
func TestChunkReadKeepsTheFirstAnswerToArrive(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(64 * 1024)
	recovering := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, payload, false)
		w.Write(payload[:len(payload)/2])
		flush(w)
		time.Sleep(chunkReadStallDelay * 3 / 2)
		w.Write(payload[len(payload)/2:])
	})
	var late *chunkServer
	late = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * chunkReadStallDelay):
			answers(payload, false)(n, w, r)
		case <-r.Context().Done():
			late.abandoned.Add(1)
		}
	})

	p := readPath{name: "direct", body: payload, isFullChunk: true}
	got, took, err := fetch(context.Background(), p, len(payload), []string{recovering.url(), late.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if took > 5*chunkReadStallDelay {
		t.Fatalf("read took %v; it should have ended when the first location recovered", took)
	}
	if n := late.requests.Load(); n != 1 {
		t.Fatalf("second location asked %d times, want once", n)
	}
	waitFor(t, "the hedge to be given up", func() bool { return late.abandoned.Load() == 1 })
}

// TestChunkReadHedgeWinnerReplacesThePartialRead: the first location wrote part
// of the chunk into the caller's buffer before it stalled. The chunk the hedge
// delivered must replace all of it, and the stalled request must not write the
// buffer again once the read returned.
func TestChunkReadHedgeWinnerReplacesThePartialRead(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(128 * 1024)
	stale := bytes.Repeat([]byte{'x'}, len(payload))
	release := make(chan struct{})
	stalling := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, stale, false)
		w.Write(stale[:len(stale)/2])
		flush(w)
		select {
		case <-release:
			w.Write(stale[len(stale)/2:])
		case <-r.Context().Done():
		}
	})
	live := newChunkServer(t, answers(payload, false))

	buffer := make([]byte, len(payload))
	n, err := RetriedFetchChunkData(context.Background(), buffer, []string{stalling.url(), live.url()}, nil, false, true, 0, "3,abc", nil)
	if err != nil || n != len(payload) || !bytes.Equal(buffer, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk from the second location", n, err)
	}
	close(release)
	time.Sleep(50 * time.Millisecond)
	if !bytes.Equal(buffer, payload) {
		t.Fatal("the buffer changed after the read returned")
	}
}

// TestChunkReadAsksTheOnlyLocationAgain: a chunk with a single location is
// asked again over another connection when its response stalls, since the
// stall belongs to that connection rather than to the server.
func TestChunkReadAsksTheOnlyLocationAgain(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(64 * 1024)
	var only *chunkServer
	only = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			stallsAfter(&only, payload, len(payload)/4, false)(n, w, r)
			return
		}
		answers(payload, false)(n, w, r)
	})

	for _, p := range readPaths(t, payload)[:2] {
		only.requests.Store(0)
		got, took, err := fetch(context.Background(), p, len(payload), []string{only.url()}, nil)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s: got %d bytes, %v; want the chunk", p.name, len(got), err)
		}
		if took > chunkReadStallDelay+time.Second {
			t.Fatalf("%s: read took %v, want a little over the %v hedge delay", p.name, took, chunkReadStallDelay)
		}
		if n := only.requests.Load(); n != 2 {
			t.Fatalf("%s: location asked %d times, want twice", p.name, n)
		}
	}
}

// TestChunkReadHedgesOnlyWithinBudget: with the budget spent, a stalled read is
// waited out as before instead of being asked elsewhere.
func TestChunkReadHedgesOnlyWithinBudget(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	hedgeTokens.Store(0)
	payload := testPayload(64 * 1024)
	recovering := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, payload, false)
		w.Write(payload[:len(payload)/2])
		flush(w)
		time.Sleep(4 * chunkReadStallDelay)
		w.Write(payload[len(payload)/2:])
	})
	other := newChunkServer(t, answers(payload, false))

	p := readPath{name: "direct", body: payload, isFullChunk: true}
	got, took, err := fetch(context.Background(), p, len(payload), []string{recovering.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if took < 4*chunkReadStallDelay {
		t.Fatalf("read took %v; without budget it should have waited for the first location", took)
	}
	if n := other.requests.Load(); n != 0 {
		t.Fatalf("second location asked %d times without budget, want never", n)
	}
}

// TestChunkReadHedgeBudgetIsATenthOfTheReads checks the budget arithmetic: the
// burst is available at once, after that every ten reads pay for one hedge.
func TestChunkReadHedgeBudgetIsATenthOfTheReads(t *testing.T) {
	shortHedgeDelay(t, time.Second)
	hedges := 0
	for spendHedgeToken() {
		hedges++
	}
	if hedges != hedgeTokenBurst/hedgeTokenScale {
		t.Fatalf("burst allowed %d hedges, want %d", hedges, hedgeTokenBurst/hedgeTokenScale)
	}
	for i := 0; i < 9; i++ {
		earnHedgeToken()
	}
	if spendHedgeToken() {
		t.Fatal("nine reads paid for a hedge")
	}
	earnHedgeToken()
	if !spendHedgeToken() {
		t.Fatal("ten reads did not pay for a hedge")
	}
	for i := 0; i < 1000; i++ {
		earnHedgeToken()
	}
	if got := hedgeTokens.Load(); got != hedgeTokenBurst {
		t.Fatalf("saved %d tokens, want at most the burst %d", got, hedgeTokenBurst)
	}
}

// TestChunkReadCancelledDuringAHedge: when the caller gives up while both the
// first request and its hedge are stalled, the read returns at once and no
// request writes the buffer afterwards.
func TestChunkReadCancelledDuringAHedge(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	release := make(chan struct{})
	stall := func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, payload, false)
		w.Write(payload[:1024])
		flush(w)
		select {
		case <-release:
			w.Write(payload[1024:])
		case <-r.Context().Done():
		}
	}
	first := newChunkServer(t, stall)
	second := newChunkServer(t, stall)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(4*chunkReadStallDelay, cancel)
	buffer := make([]byte, len(payload))
	start := time.Now()
	_, err := RetriedFetchChunkData(ctx, buffer, []string{first.url(), second.url()}, nil, false, true, 0, "3,abc", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want the cancellation", err)
	}
	if took := time.Since(start); took > 4*chunkReadStallDelay+time.Second {
		t.Fatalf("returned %v after the start, long after the cancellation", took)
	}
	if second.requests.Load() == 0 {
		t.Fatal("the stalled read was never hedged")
	}
	clear(buffer)
	close(release)
	time.Sleep(50 * time.Millisecond)
	if !bytes.Equal(buffer, make([]byte, len(payload))) {
		t.Fatal("the buffer changed after the read returned")
	}
}

// TestChunkReadStillMovesOnAtOnceAfterAFailure: a location that fails outright
// is not waited on for the hedge delay, and the failure still refreshes the
// location list as before.
func TestChunkReadStillMovesOnAtOnceAfterAFailure(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, time.Second)
	payload := testPayload(64 * 1024)
	failing := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		http.Error(w, "disk error", http.StatusInternalServerError)
	})
	live := newChunkServer(t, answers(payload, false))

	for _, p := range readPaths(t, payload)[:2] {
		refreshed := 0
		got, took, err := fetch(context.Background(), p, len(payload), []string{failing.url(), live.url()}, func() []string {
			refreshed++
			return nil
		})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s: got %d bytes, %v; want the chunk", p.name, len(got), err)
		}
		if took >= chunkReadStallDelay {
			t.Fatalf("%s: read took %v; a failure should move on at once", p.name, took)
		}
		if refreshed != 1 {
			t.Fatalf("%s: refresh called %d times after a failure, want once", p.name, refreshed)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestChunkReadGivesAnAnswerLongerThanABodyThatStops: before its response
// headers a server may still be reading the needle from a busy disk, so a
// location gets chunkReadAnswerDelay to answer, while a body that stops
// arriving is hedged after the shorter chunkReadStallDelay.
func TestChunkReadGivesAnAnswerLongerThanABodyThatStops(t *testing.T) {
	forgetUnreachable(t)
	hedgeDelays(t, 400*time.Millisecond, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	slowDisk := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		answers(payload, false)(n, w, r)
	})
	var stalling *chunkServer
	stalling = newChunkServer(t, stallsAfter(&stalling, payload, len(payload)/2, false))
	other := newChunkServer(t, answers(payload, false))
	p := readPath{name: "direct", body: payload, isFullChunk: true}

	got, took, err := fetch(context.Background(), p, len(payload), []string{slowDisk.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("slow answer: got %d bytes, %v; want the chunk", len(got), err)
	}
	if took < 150*time.Millisecond || other.requests.Load() != 0 {
		t.Fatalf("slow answer: read took %v and asked the other location %d times; want it waited for", took, other.requests.Load())
	}

	got, took, err = fetch(context.Background(), p, len(payload), []string{stalling.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("stalled body: got %d bytes, %v; want the chunk", len(got), err)
	}
	if took >= chunkReadAnswerDelay || other.requests.Load() != 1 {
		t.Fatalf("stalled body: read took %v and asked the other location %d times; want it hedged after %v", took, other.requests.Load(), chunkReadStallDelay)
	}
}

// TestChunkReadAsksAgainWhereTheServerAnswered: once every location has been
// asked, a stalled read is asked again of a location whose server answered,
// not of one that never did.
func TestChunkReadAsksAgainWhereTheServerAnswered(t *testing.T) {
	forgetUnreachable(t)
	hedgeDelays(t, 100*time.Millisecond, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	var silent, flaky *chunkServer
	silent = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) { silent.wait(r) })
	flaky = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			stallsAfter(&flaky, payload, len(payload)/2, false)(n, w, r)
			return
		}
		answers(payload, false)(n, w, r)
	})
	p := readPath{name: "direct", body: payload, isFullChunk: true}

	urls := []string{silent.url(), flaky.url()}
	got, _, err := fetch(context.Background(), p, len(payload), urls, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if s, f := silent.requests.Load(), flaky.requests.Load(); s != 1 || f != 2 {
		t.Fatalf("asked the silent location %d and the stalling one %d times, want 1 and 2", s, f)
	}
	assertOrder(t, ReachableFirst(urls), flaky.url(), silent.url())
}

// TestChunkReadAskedAgainIsNotUnreachable: the requests that asked a stalled
// location again over new connections had not answered when the first one
// recovered and delivered the chunk. That server answered, so it is not taken
// for one that cannot be reached.
func TestChunkReadAskedAgainIsNotUnreachable(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	var only *chunkServer
	only = newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		if n > 1 {
			only.wait(r)
			return
		}
		sendHeaders(w, payload, false)
		w.Write(payload[:len(payload)/2])
		flush(w)
		time.Sleep(5 * chunkReadStallDelay)
		w.Write(payload[len(payload)/2:])
	})
	p := readPath{name: "direct", body: payload, isFullChunk: true}
	refreshed := 0
	got, _, err := fetch(context.Background(), p, len(payload), []string{only.url()}, func() []string {
		refreshed++
		return nil
	})
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if n := only.requests.Load(); n < 2 {
		t.Fatalf("asked %d times, want it asked again", n)
	}
	if refreshed != 0 {
		t.Fatalf("refresh called %d times; the location answered", refreshed)
	}
	other := "http://127.0.0.1:1/3,abc"
	assertOrder(t, ReachableFirst([]string{only.url(), other}), only.url(), other)
}

// TestChunkReadWaitsForTheOnlyLocationToAnswer: with nowhere else to go, a
// location that has not answered yet is waited for, as before: asking its
// server again would only add to whatever keeps it busy.
func TestChunkReadWaitsForTheOnlyLocationToAnswer(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	slow := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		answers(payload, false)(n, w, r)
	})
	p := readPath{name: "direct", body: payload, isFullChunk: true}
	got, _, err := fetch(context.Background(), p, len(payload), []string{slow.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk", len(got), err)
	}
	if n := slow.requests.Load(); n != 1 {
		t.Fatalf("asked %d times, want once", n)
	}
}

// fakeScratchBudget has room for a hedge buffer while room is set, and
// counts what is reserved and returned.
type fakeScratchBudget struct {
	room               atomic.Bool
	reserved, released atomic.Int32
}

func (b *fakeScratchBudget) TryReserveScratch(size int) bool {
	if !b.room.Load() {
		return false
	}
	b.reserved.Add(1)
	return true
}

func (b *fakeScratchBudget) ReleaseScratch(size int) { b.released.Add(1) }

// TestChunkReadHedgeNeedsRoomInTheScratchBudget: a hedge reads into a buffer
// of its own, which counts against the caller's memory budget. Without room
// in it the stalled read is waited out as before; with room, every buffer
// taken is returned once its request has ended, abandoned ones included.
func TestChunkReadHedgeNeedsRoomInTheScratchBudget(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	recovering := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		sendHeaders(w, payload, false)
		w.Write(payload[:len(payload)/2])
		flush(w)
		select {
		case <-time.After(4 * chunkReadStallDelay):
			w.Write(payload[len(payload)/2:])
		case <-r.Context().Done():
		}
	})
	other := newChunkServer(t, answers(payload, false))
	p := readPath{name: "direct", body: payload, isFullChunk: true}
	budget := &fakeScratchBudget{}
	ctx := WithScratchBudget(context.Background(), budget)

	// without a hedge token, no room is taken: taking it can evict cached chunks
	budget.room.Store(true)
	hedgeTokens.Store(0)
	got, _, err := fetch(ctx, p, len(payload), []string{recovering.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("no token: got %d bytes, %v; want the chunk", len(got), err)
	}
	if r, o := budget.reserved.Load(), other.requests.Load(); r != 0 || o != 0 {
		t.Fatalf("no token: took room %d times and asked the other location %d times, want neither", r, o)
	}

	// without room, the token is given back
	budget.room.Store(false)
	hedgeTokens.Store(5 * hedgeTokenScale)
	got, took, err := fetch(ctx, p, len(payload), []string{recovering.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("no room: got %d bytes, %v; want the chunk", len(got), err)
	}
	if took < 4*chunkReadStallDelay || other.requests.Load() != 0 {
		t.Fatalf("no room: read took %v and asked the other location %d times; want it waited out", took, other.requests.Load())
	}
	if got, want := hedgeTokens.Load(), int64(5*hedgeTokenScale+hedgeTokenEarn); got != want {
		t.Fatalf("no room: %d thousandths of a token left, want %d: the hedge not sent keeps its token", got, want)
	}

	budget.room.Store(true)
	got, took, err = fetch(ctx, p, len(payload), []string{recovering.url(), other.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("room: got %d bytes, %v; want the chunk", len(got), err)
	}
	if took >= 4*chunkReadStallDelay || other.requests.Load() != 1 {
		t.Fatalf("room: read took %v and asked the other location %d times; want it hedged", took, other.requests.Load())
	}
	waitFor(t, "every hedge buffer to be returned", func() bool {
		return budget.reserved.Load() == 1 && budget.released.Load() == 1
	})
}

// TestChunkReadThrottledHedgeAsksTheNextLocation: a hedge whose server is too
// busy to serve it (429) says nothing about the other locations, so the read
// goes on to the next one instead of waiting out the stalled request.
func TestChunkReadThrottledHedgeAsksTheNextLocation(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 50*time.Millisecond)
	payload := testPayload(64 * 1024)
	var stalling *chunkServer
	stalling = newChunkServer(t, stallsAfter(&stalling, payload, len(payload)/2, false))
	busy := newChunkServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	})
	live := newChunkServer(t, answers(payload, false))
	p := readPath{name: "direct", body: payload, isFullChunk: true}

	got, took, err := fetch(context.Background(), p, len(payload), []string{stalling.url(), busy.url(), live.url()}, nil)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, %v; want the chunk from the third location", len(got), err)
	}
	if took > chunkReadStallDelay+time.Second {
		t.Fatalf("read took %v; it should not have waited for the stalled location", took)
	}
	if b, l := busy.requests.Load(), live.requests.Load(); b != 1 || l != 1 {
		t.Fatalf("asked the busy location %d and the live one %d times, want once each", b, l)
	}
}

// TestChunkReadHedgeCapCoversRetries: the hedges of one read are capped across
// the passes of its retries, not per pass.
func TestChunkReadHedgeCapCoversRetries(t *testing.T) {
	shortHedgeDelay(t, 20*time.Millisecond)
	var attempts atomic.Int32
	stallThenFail := func(ctx context.Context, url string, dst []byte, progress *chunkProgress) (int, bool, error) {
		attempts.Add(1)
		progress.answer()
		select {
		case <-time.After(10 * chunkReadStallDelay):
			return 0, true, errors.New("connection reset")
		case <-ctx.Done():
			return 0, false, ctx.Err()
		}
	}
	hedging := &readHedges{}
	buffer := make([]byte, 16)
	hedgeTokens.Store(5 * hedgeTokenScale)
	for pass := 0; pass < 2; pass++ {
		if _, _, _, err := hedgedChunkPass(context.Background(), []string{"http://volume:8080/3,abc"}, buffer, hedging, stallThenFail); err == nil {
			t.Fatalf("pass %d: want the failure", pass)
		}
	}
	if n := attempts.Load(); n != 1+maxHedgesPerRead+1 {
		t.Fatalf("%d requests over two passes, want %d: the first pass's request and hedges, then one", n, 1+maxHedgesPerRead+1)
	}
	if got, want := hedgeTokens.Load(), int64(5*hedgeTokenScale+hedgeTokenEarn-maxHedgesPerRead*hedgeTokenScale); got != want {
		t.Fatalf("%d thousandths of a token left, want %d: one share earned for the read, not one per pass", got, want)
	}
}

// TestChunkReadErrorThatAnsweredIsNotUnreachable: a location that sent the
// response headers of an error and then stalled its body did answer. When
// another location delivers the chunk first, it is not taken for one that
// cannot be reached.
func TestChunkReadErrorThatAnsweredIsNotUnreachable(t *testing.T) {
	forgetUnreachable(t)
	shortHedgeDelay(t, 100*time.Millisecond)
	payload := testPayload(64 * 1024)
	failing := stalledBodyServer(t, http.StatusInternalServerError, "disk ")
	live := newChunkServer(t, answers(payload, false))

	for _, p := range readPaths(t, payload)[:2] {
		forgetUnreachable(t)
		urls := []string{failing.URL + "/3,abc", live.url()}
		refreshed := 0
		got, _, err := fetch(context.Background(), p, len(payload), urls, func() []string {
			refreshed++
			return nil
		})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("%s: got %d bytes, %v; want the chunk from the second location", p.name, len(got), err)
		}
		if refreshed != 0 {
			t.Fatalf("%s: refresh called %d times; the first location answered", p.name, refreshed)
		}
		assertOrder(t, ReachableFirst(urls), urls...)
	}
}
