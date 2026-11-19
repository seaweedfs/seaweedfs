package http

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// unreachableRetryInterval is how long a server that failed to answer is tried
// last before it is probed again.
const unreachableRetryInterval = 30 * time.Second

// reachabilityProbeTimeout bounds the background request that checks whether
// an unanswering server is back.
const reachabilityProbeTimeout = 5 * time.Second

type unanswered struct {
	at   time.Time
	addr string
}

// unreachable maps a host, and a machine whose dial got no answer, to its last
// failure. A dead replica sits in the location list of every volume it held,
// so this is kept per host rather than per volume.
var unreachable sync.Map

// machineKey keys a machine apart from a host without an explicit port.
type machineKey string

var probes sync.WaitGroup

// recordUnreachable keys a failure by machine when the dial never reached it,
// so every server on a powered-off node is tried last after one failure.
func recordUnreachable(host string, err error) {
	failure := unanswered{at: time.Now(), addr: host}
	machine := machineOf(host)
	unreachable.Store(host, failure)
	if hostDown(err) {
		unreachable.Store(machine, failure)
	} else if connRefused(err) {
		unreachable.Delete(machine)
	}
}

// recordReachable clears the machine verdict too, but a sibling port that
// failed on its own stays last until it answers.
func recordReachable(host string) {
	unreachable.Delete(host)
	unreachable.Delete(machineOf(host))
}

// hostDown reports whether a dial got no answer from the machine at all, unlike
// a refused connection, which shows the machine is up.
func hostDown(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return false
	}
	return opErr.Timeout() || noRoute(err)
}

// ReachableFirst orders urls so that hosts which recently failed to answer
// come last, keeping the order within each group. Once the retry interval has
// passed, such a host is probed in the background rather than by a read, so a
// dead replica never costs a read the dial timeout again.
func ReachableFirst(urls []string) []string {
	var reachable, failed []string
	for _, u := range urls {
		if isUnanswering(hostOf(u)) {
			failed = append(failed, u)
		} else {
			reachable = append(reachable, u)
		}
	}
	if failed == nil {
		return urls
	}
	return append(reachable, failed...)
}

func isUnanswering(host string) bool {
	for _, key := range [2]any{host, machineOf(host)} {
		v, found := unreachable.Load(key)
		if !found {
			continue
		}
		failure := v.(unanswered)
		if time.Since(failure.at) >= unreachableRetryInterval &&
			unreachable.CompareAndSwap(key, failure, unanswered{at: time.Now(), addr: failure.addr}) {
			probes.Go(func() { probeReachability(failure.addr) })
		}
		return true
	}
	return false
}

// probeReachability asks for response headers, not just a connection, so a
// server that accepts but never answers stays last.
func probeReachability(addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), reachabilityProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, "http://"+addr+"/status", nil)
	if err == nil {
		var resp *http.Response
		if resp, err = GetGlobalHttpClient().Do(req); err == nil {
			resp.Body.Close()
			recordReachable(addr)
			return
		}
	}
	recordUnreachable(addr, err)
}

func hostOf(rawUrl string) string {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return ""
	}
	return parsed.Host
}

func machineOf(host string) machineKey {
	if machine, _, err := net.SplitHostPort(host); err == nil {
		return machineKey(machine)
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return machineKey(host)
}
