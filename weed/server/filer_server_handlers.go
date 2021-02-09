package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/stats"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()

	// proxy to volume servers
	var fileId string
	if strings.HasPrefix(r.RequestURI, "/?proxyChunkId=") {
		fileId = r.RequestURI[len("/?proxyChunkId="):]
	}
	if fileId != "" {
		stats.FilerRequestCounter.WithLabelValues("proxy").Inc()
		fs.proxyToVolumeServer(w, r, fileId)
		stats.FilerRequestHistogram.WithLabelValues("proxy").Observe(time.Since(start).Seconds())
		return
	}

	w.Header().Set("Server", "SeaweedFS Filer "+util.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
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
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.DeleteTaggingHandler(w, r)
		} else {
			fs.DeleteHandler(w, r)
		}
		stats.FilerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	case "PUT":
		stats.FilerRequestCounter.WithLabelValues("put").Inc()
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.PutTaggingHandler(w, r)
		} else {
			fs.PostHandler(w, r)
		}
		stats.FilerRequestHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds())
	case "POST":
		stats.FilerRequestCounter.WithLabelValues("post").Inc()
		fs.PostHandler(w, r)
		stats.FilerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds())
	case "OPTIONS":
		stats.FilerRequestCounter.WithLabelValues("options").Inc()
		OptionsHandler(w, r, false)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	}
}

func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Filer "+util.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
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
	case "OPTIONS":
		stats.FilerRequestCounter.WithLabelValues("options").Inc()
		OptionsHandler(w, r, true)
		stats.FilerRequestHistogram.WithLabelValues("head").Observe(time.Since(start).Seconds())
	}
}

func OptionsHandler(w http.ResponseWriter, r *http.Request, isReadOnly bool) {
	if isReadOnly {
		w.Header().Add("Access-Control-Allow-Methods", "GET, OPTIONS")
	} else {
		w.Header().Add("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
	}
	w.Header().Add("Access-Control-Allow-Headers", "*")
}
