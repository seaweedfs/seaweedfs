package http

import (
	"net/url"
	"sync"
	"time"
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

// ReachableFirst orders urls so that hosts which recently failed to answer
// come last, keeping the order within each group. Once the retry interval has
// passed, the first read to ask claims the probe and tries that host first;
// the others keep it last until the probe settles.
func ReachableFirst(urls []string) []string {
	var probes, reachable, unanswered []string
	for _, u := range urls {
		host := hostOf(u)
		failedAt, failed := unreachable.Load(host)
		switch {
		case !failed:
			reachable = append(reachable, u)
		case time.Since(failedAt.(time.Time)) < unreachableRetryInterval:
			unanswered = append(unanswered, u)
		case unreachable.CompareAndSwap(host, failedAt, time.Now()):
			probes = append(probes, u)
		default:
			unanswered = append(unanswered, u)
		}
	}
	if len(probes) == 0 && len(unanswered) == 0 {
		return urls
	}
	return append(append(probes, reachable...), unanswered...)
}

func hostOf(rawUrl string) string {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return ""
	}
	return parsed.Host
}
