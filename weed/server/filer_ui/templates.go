package master_ui

import (
	"github.com/dustin/go-humanize"
	"html/template"
)

var funcMap = template.FuncMap{
	"humanizeBytes": humanize.Bytes,
}

var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(`<!DOCTYPE html>
<html>
<head>
	<title>SeaweedFS Filer</title>
	<link rel="stylesheet" href="/seaweedfsstatic/bootstrap/3.3.1/css/bootstrap.min.css">
<style>
#drop-area {
  border: 1px transparent;
}
#drop-area.highlight {
  border-color: purple;
  border: 2px dashed #ccc;
}
.button {
  display: inline-block;
  padding: 2px;
  background: #ccc;
  cursor: pointer;
  border-radius: 2px;
  border: 1px solid #ccc;
  float: right;
}
.button:hover {
  background: #ddd;
}
#fileElem {
  display: none;
}
</style>
</head>
<body>
	<div class="container">
		<div class="page-header">
			<h1>
				<a href="https://github.com/chrislusf/seaweedfs"><img src="/seaweedfsstatic/seaweed50x50.png"></img></a>
				SeaweedFS Filer
			</h1>
		</div>
		<div class="row">
			<div>
			{{ range $entry := .Breadcrumbs }}
				<a href={{ $entry.Link }} >
					{{ $entry.Name }}
				</a>
			{{ end }}
				<label class="button" for="fileElem">Upload</label>
			</div>
		</div>

		<div class="row" id="drop-area">
			<form class="upload-form">
				<input type="file" id="fileElem" multiple onchange="handleFiles(this.files)">

			<table width="90%">
				{{$path := .Path }}
				{{ range $entry_index, $entry := .Entries }}
				<tr>
					<td>
					{{if $entry.IsDirectory}}
						<img src="/seaweedfsstatic/images/folder.gif" width="20" height="23">
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
						{{ $entry.Mime }}
					{{end}}
					</td>
					<td align="right">
					{{if $entry.IsDirectory}}
					{{else}}
						{{ $entry.Size | humanizeBytes }}
						&nbsp;&nbsp;&nbsp;
					{{end}}
					</td>
					<td>
						{{ $entry.Timestamp.Format "2006-01-02 15:04" }}
					</td>
				</tr>
				{{ end }}

			</table>
			</form>
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
<script type="text/javascript">
// ************************ Drag and drop ***************** //
let dropArea = document.getElementById("drop-area")

// Prevent default drag behaviors
;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
  dropArea.addEventListener(eventName, preventDefaults, false)   
  document.body.addEventListener(eventName, preventDefaults, false)
})

// Highlight drop area when item is dragged over it
;['dragenter', 'dragover'].forEach(eventName => {
  dropArea.addEventListener(eventName, highlight, false)
})

;['dragleave', 'drop'].forEach(eventName => {
  dropArea.addEventListener(eventName, unhighlight, false)
})

// Handle dropped files
dropArea.addEventListener('drop', handleDrop, false)

function preventDefaults (e) {
  e.preventDefault()
  e.stopPropagation()
}

function highlight(e) {
  dropArea.classList.add('highlight')
}

function unhighlight(e) {
  dropArea.classList.remove('highlight')
}

function handleDrop(e) {
  var dt = e.dataTransfer
  var files = dt.files

  handleFiles(files)
}

function handleFiles(files) {
  files = [...files]
  files.forEach(uploadFile)
  window.location.reload()
}

function uploadFile(file, i) {
  var url = window.location.href
  var xhr = new XMLHttpRequest()
  var formData = new FormData()
  xhr.open('POST', url, true)

  formData.append('file', file)
  xhr.send(formData)
}
</script>
</html>
`))
