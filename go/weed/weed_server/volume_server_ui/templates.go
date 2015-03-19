package master_ui

import (
	"html/template"
)

var StatusTpl = template.Must(template.New("status").Parse(`<!DOCTYPE html>
<html>
  <head>
    <title>Seaweed File System {{ .Version }}</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
  </head>
  <body>
    <div class="container">
      <div class="page-header">
        <h1>Seaweed File System <small>{{ .Version }}</small></h1>
      </div>

      <div class="row">
        <div class="col-sm-6">          
    	  <h2>Disk Stats</h2>
          <table class="table table-condensed table-striped">
          {{ range .DiskStatuses }}
            <tr>
              <th>{{ .Dir }}</th>
              <td>{{ .Free }}</td>
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
              <td>{{ .Size }}</td>
              <td>{{ .FileCount }}</td>
              <td>{{ .DeleteCount }} / {{.DeletedByteCount}} Bytes</td>
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
