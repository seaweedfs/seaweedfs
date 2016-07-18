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
			<ul>
				{{$path := .Path }}
				{{ range $dirs_index, $dir := .Directories }}
				<li>
					<a href=		{{ print $path  $dir.Name  "/"}} >
						{{ $dir.Name }}
					</a>
				</li>
				{{ end }}

				{{ range $file_index, $file := .Files }}
				<li>
					{{ $file.Name }}
				</li>
				{{ end }}
			</ul>
		</div>

		{{if .NotAllFilesDisplayed}}
		<div class="row">
			Not all files are displayed.
		</div>
		{{end}}
	</div>
</body>
</html>
`))
