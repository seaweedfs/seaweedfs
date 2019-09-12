package weed_server

import (
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/stats"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	switch r.Method {
	case "GET":
		stats.FilerRequestCounter.WithLabelValues("get").Inc()
		fs.GetOrHeadHandler(w, r, true)
		stats.FilerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds())
	case "HEAD":
		stats.FilerRequestCounter.WithLabelValues("head").Inc()
		fs.GetOrHeadHandler(w, r, false)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	case "DELETE":
		stats.FilerRequestCounter.WithLabelValues("delete").Inc()
		fs.DeleteHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	case "PUT":
		stats.FilerRequestCounter.WithLabelValues("put").Inc()
		fs.PostHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds())
	case "POST":
		stats.FilerRequestCounter.WithLabelValues("post").Inc()
		fs.PostHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}
}

func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	switch r.Method {
	case "GET":
		stats.FilerRequestCounter.WithLabelValues("get").Inc()
		fs.GetOrHeadHandler(w, r, true)
		stats.FilerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds())
	case "HEAD":
		stats.FilerRequestCounter.WithLabelValues("head").Inc()
		fs.GetOrHeadHandler(w, r, false)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	}
}
