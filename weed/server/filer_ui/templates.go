package master_ui

import (
	"html/template"
)

var StatusTpl = template.Must(template.New("status").Parse(`<!DOCTYPE html>
<html>
<head>
	<title>SeaweedFS Filer</title>
	<link rel="icon" href="http://7viirv.com1.z0.glb.clouddn.com/seaweed_favicon.png" sizes="32x32" />
	<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
</head>
<body>
	<div class="container">
		<div class="page-header">
			<h1>
				<img src="http://7viirv.com1.z0.glb.clouddn.com/seaweed50x50.png"></img>
				SeaweedFS Filer
			</h1>
		</div>
		<div class="row">
			{{.Path}}
		</div>

		<div class="row">
			<table width="90%">
				{{$path := .Path }}
				{{ range $entry_index, $entry := .Entries }}
				<tr>
					<td>
					{{if $entry.IsDirectory}}
						<img src="https://www.w3.org/TR/WWWicn/folder.gif" width="20" height="23">
						<a href={{ print $path  "/" $entry.Name  "/"}} >
							{{ $entry.Name }}
						</a>
					{{else}}
						<a href={{ print $path  "/" $entry.Name }} >
							{{ $entry.Name }}
						</a>
					{{end}}
					</td>
					<td align="right">
					{{if $entry.IsDirectory}}
					{{else}}
						{{ $entry.Size }} bytes
						&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
					{{end}}
					</td>
					<td>
						{{ $entry.Timestamp.Format "2006-01-02 15:04" }}
					</td>
				</tr>
				{{ end }}

			</table>
		</div>

		{{if .ShouldDisplayLoadMore}}
		<div class="row">
		<a href={{ print .Path "?limit=" .Limit	"&lastFileName=" .LastFileName}} >
		Load more
		</a>
		</div>
		{{end}}
	</div>
</body>
</html>
`))
