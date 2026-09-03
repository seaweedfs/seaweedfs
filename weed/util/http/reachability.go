package http

import (
	"net/url"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// unreachableRetryInterval is how long a server that failed to answer is tried
// last before a read probes it again.
const unreachableRetryInterval = 30 * time.Second

// unreachable maps a host to when it last failed to answer a request. A dead
// replica sits in the location list of every volume it held, so this is kept
// per host rather than per volume.
var unreachable sync.Map

func recordUnreachable(host string) {
	unreachable.Store(host, time.Now())
}

func recordReachable(host string) {
	unreachable.Delete(host)
}

func lastUnanswered(host string) (time.Time, bool) {
	failedAt, failed := unreachable.Load(host)
	if !failed {
		return time.Time{}, false
	}
	return failedAt.(time.Time), true
}

// ReachableFirst returns urls with those on hosts that recently failed to
// answer moved to the back, keeping the order within each group. Once the
// retry interval has passed, the first read that would try such a host first
// probes it, and the others keep it last until that probe settles.
func ReachableFirst(urls []string) []string {
	front := make(map[string]bool, len(urls))
	for _, u := range urls {
		failedAt, failed := lastUnanswered(hostOf(u))
		if !failed || time.Since(failedAt) >= unreachableRetryInterval {
			front[u] = true
		}
	}
	if len(front) != len(urls) {
		urls = util.ReorderToFront(front, urls)
	}
	if len(urls) > 1 && !claimProbe(hostOf(urls[0])) {
		rotated := make([]string, 0, len(urls))
		rotated = append(rotated, urls[1:]...)
		urls = append(rotated, urls[0])
	}
	return urls
}

// claimProbe reports whether the caller is the one read that tries an expired
// host again; losing the race means another read is already probing it.
func claimProbe(host string) bool {
	failedAt, failed := lastUnanswered(host)
	if !failed || time.Since(failedAt) < unreachableRetryInterval {
		return true
	}
	return unreachable.CompareAndSwap(host, failedAt, time.Now())
}

func hostOf(rawUrl string) string {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return ""
	}
	return parsed.Host
}
