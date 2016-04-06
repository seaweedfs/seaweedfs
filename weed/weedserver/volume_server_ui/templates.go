package master_ui

import (
	"fmt"
	"html/template"
	"strconv"
	"strings"
)

const (
	_          = iota // ignore first value by assigning to blank identifier
	KB float64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

func join(data []int64) string {
	var ret []string
	for _, d := range data {
		ret = append(ret, strconv.Itoa(int(d)))
	}
	return strings.Join(ret, ",")
}

func prettySize(byteSize uint64) string {
	sz := float64(byteSize)
	switch {
	case sz >= PB:
		return fmt.Sprintf("%.2f PB", sz/PB)
	case sz >= TB:
		return fmt.Sprintf("%.2f TB", sz/TB)
	case sz >= GB:
		return fmt.Sprintf("%.2f GB", sz/GB)
	case sz >= MB:
		return fmt.Sprintf("%.2f MB", sz/MB)
	case sz >= KB:
		return fmt.Sprintf("%.2f KB", sz/KB)
	}
	return fmt.Sprintf("%.0f Bytes", sz)
}

var funcMap = template.FuncMap{
	"join":       join,
	"prettySize": prettySize,
}

var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(`<!DOCTYPE html>
<html>
  <head>
    <title>SeaweedFS {{ .Version }}</title>
	<link rel="icon" href="http://7viirv.com1.z0.glb.clouddn.com/seaweed_favicon.png" sizes="32x32" />  
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
	<script type="text/javascript" src="https://code.jquery.com/jquery-2.1.3.min.js"></script>
	<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jquery-sparklines/2.1.2/jquery.sparkline.min.js"></script>
    <script type="text/javascript">
    $(function() {
		var periods = ['second', 'minute', 'hour', 'day'];
		for (i = 0; i < periods.length; i++) { 
		    var period = periods[i];
	        $('.inlinesparkline-'+period).sparkline('html', {
				type: 'line', 
				barColor: 'red', 
				tooltipSuffix:' request per '+period,
			}); 
		}
    });
    </script>
	<style>
	#jqstooltip{
		height: 28px !important;
		width: 150px !important;
	}
	</style>
  </head>
  <body>
    <div class="container">
      <div class="page-header">
	    <h1>
	      <img src="http://7viirv.com1.z0.glb.clouddn.com/seaweed50x50.png"></img>
          SeaweedFS <small>{{ .Version }}</small>
	    </h1>
      </div>

      <div class="row">
        <div class="col-sm-6">          
    	  <h2>Disk Stats</h2>
          <table class="table table-condensed table-striped">
          {{ range .DiskStatuses }}
            <tr>
              <th>{{ .Dir }}</th>
              <td>{{ .Free | prettySize }} Free</td>
            </tr>
          {{ end }}
          </table>
        </div>

        <div class="col-sm-6">
          <h2>System Stats</h2>
          <table class="table table-condensed table-striped">
            <tr>
              <th>Master</th>
              <td><a href="http://{{.Master}}/ui/index.html">{{.Master}}</a></td>
            </tr>
            <tr>
              <th>Weekly # ReadRequests</th>
              <td><span class="inlinesparkline-day">{{ .Counters.ReadRequests.WeekCounter.ToList | join }}</span></td>
            </tr>
            <tr>
              <th>Daily # ReadRequests</th>
              <td><span class="inlinesparkline-hour">{{ .Counters.ReadRequests.DayCounter.ToList | join }}</span></td>
            </tr>
            <tr>
              <th>Hourly # ReadRequests</th>
              <td><span class="inlinesparkline-minute">{{ .Counters.ReadRequests.HourCounter.ToList | join }}</span></td>
            </tr>
            <tr>
              <th>Last Minute # ReadRequests</th>
              <td><span class="inlinesparkline-second">{{ .Counters.ReadRequests.MinuteCounter.ToList | join }}</span></td>
            </tr>
            <tr>
              <th>Concurrent Connections</th>
              <td>{{ .Counters.Connections.WeekCounter.Sum }}</td>
            </tr>
          {{ range $key, $val := .Stats }}
            <tr>
              <th>{{ $key }}</th>
              <td>{{ $val }}</td>
            </tr>
          {{ end }}
          </table>
        </div>
      </div>

      <div class="row">
        <h2>Volumes</h2>
        <table class="table table-striped">
          <thead>
            <tr>
              <th>Id</th>
              <th>Collection</th>
              <th>Size</th>
              <th>Files</th>
              <th>Trash</th>
              <th>TTL</th>
            </tr>
          </thead>
          <tbody>
          {{ range .Volumes }}
            <tr>
              <td><code>{{ .Id }}</code></td>
              <td>{{ .Collection }}</td>
              <td>{{ .Size | prettySize }}</td>
              <td>{{ .FileCount }}</td>
              <td>{{ .DeleteCount }} / {{.DeletedByteCount | prettySize }}</td>
              <td>{{ .Ttl }}</td>
            </tr>
          {{ end }}
          </tbody>
        </table>
      </div>

    </div>
  </body>
</html>
`))
