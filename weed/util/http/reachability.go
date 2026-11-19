package http

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// unreachableRetryInterval is how long a server that failed to answer is tried
// last before it is probed again.
const unreachableRetryInterval = 30 * time.Second

// reachabilityProbeTimeout bounds the background dial that checks whether an
// unanswering server is back.
const reachabilityProbeTimeout = 5 * time.Second

type unanswered struct {
	at   time.Time
	addr string
}

// unreachable maps a host to its last failure. A dead replica sits in the
// location list of every volume it held, so this is kept per host rather than
// per volume.
var unreachable sync.Map

func recordUnreachable(host string) {
	unreachable.Store(host, unanswered{at: time.Now(), addr: host})
}

func recordReachable(host string) {
	unreachable.Delete(host)
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
	v, found := unreachable.Load(host)
	if !found {
		return false
	}
	failure := v.(unanswered)
	if time.Since(failure.at) >= unreachableRetryInterval &&
		unreachable.CompareAndSwap(host, failure, unanswered{at: time.Now(), addr: failure.addr}) {
		go probeReachability(failure.addr)
	}
	return true
}

func probeReachability(addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), reachabilityProbeTimeout)
	defer cancel()
	conn, err := util.OutboundDialContext(ctx, "tcp", addr)
	if err != nil {
		recordUnreachable(addr)
		return
	}
	conn.Close()
	recordReachable(addr)
}

func hostOf(rawUrl string) string {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return ""
	}
	return parsed.Host
}
