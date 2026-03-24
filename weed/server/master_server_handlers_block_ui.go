package weed_server

import (
	"html/template"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

var blockDashTemplate = template.Must(template.New("blockDash").Parse(blockLayoutHTML + blockDashContentHTML))
var blockOpsTemplate = template.Must(template.New("blockOps").Parse(blockLayoutHTML + blockOpsContentHTML))

type blockUIVolume struct {
	blockapi.VolumeInfo
	SizeMB     uint64
	WALHeadLSN uint64
	MaxWALLag  uint64 // max WAL lag across replicas
}

type blockUIData struct {
	Tab     string // "dashboard" or "ops"
	Volumes []blockUIVolume
	Servers []blockapi.ServerInfo
	// Dashboard stats
	TotalVolumes int
	ActiveCount  int
	PendingCount int
	TotalSizeMB  uint64
	// Observability (CP8-4)
	BarrierLagLSN      uint64
	PromotionsTotal    uint64
	FailoversTotal     uint64
	RebuildsTotal      uint64
	AssignmentQueueLen int
}

func (ms *MasterServer) buildBlockUIData(tab string) blockUIData {
	entries := ms.blockRegistry.ListAll()
	volumes := make([]blockUIVolume, len(entries))
	var totalSizeMB uint64
	var activeCount, pendingCount int
	for i := range entries {
		e := &entries[i]
		info := entryToVolumeInfo(e, ms.blockRegistry.IsBlockCapable(e.VolumeServer))
		mb := info.SizeBytes / (1024 * 1024)
		var maxLag uint64
		for _, ri := range e.Replicas {
			if ri.WALLag > maxLag {
				maxLag = ri.WALLag
			}
		}
		volumes[i] = blockUIVolume{
			VolumeInfo: info,
			SizeMB:     mb,
			WALHeadLSN: e.WALHeadLSN,
			MaxWALLag:  maxLag,
		}
		totalSizeMB += mb
		if e.Status == StatusActive {
			activeCount++
		} else {
			pendingCount++
		}
	}

	summaries := ms.blockRegistry.ServerSummaries()
	servers := make([]blockapi.ServerInfo, len(summaries))
	for i, s := range summaries {
		servers[i] = blockapi.ServerInfo{
			Address:      s.Address,
			VolumeCount:  s.VolumeCount,
			BlockCapable: s.BlockCapable,
		}
	}

	return blockUIData{
		Tab:                tab,
		Volumes:            volumes,
		Servers:            servers,
		TotalVolumes:       len(entries),
		ActiveCount:        activeCount,
		PendingCount:       pendingCount,
		TotalSizeMB:        totalSizeMB,
		BarrierLagLSN:      ms.blockRegistry.MaxBarrierLagLSN(),
		PromotionsTotal:    ms.blockRegistry.PromotionsTotal.Load(),
		FailoversTotal:     ms.blockRegistry.FailoversTotal.Load(),
		RebuildsTotal:      ms.blockRegistry.RebuildsTotal.Load(),
		AssignmentQueueLen: ms.blockAssignmentQueue.TotalPending(),
	}
}

// blockUIHandler serves the /block/ dashboard page.
func (ms *MasterServer) blockUIHandler(w http.ResponseWriter, r *http.Request) {
	data := ms.buildBlockUIData("dashboard")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := blockDashTemplate.Execute(w, data); err != nil {
		glog.V(0).Infof("block UI template error: %v", err)
	}
}

// blockOpsHandler serves the /block/ops operations page.
func (ms *MasterServer) blockOpsHandler(w http.ResponseWriter, r *http.Request) {
	data := ms.buildBlockUIData("ops")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := blockOpsTemplate.Execute(w, data); err != nil {
		glog.V(0).Infof("block UI template error: %v", err)
	}
}

// blockLayoutHTML is the shared HTML layout with tab navigation.
const blockLayoutHTML = `<!DOCTYPE html>
<html>
<head>
<title>SeaweedFS Block Storage</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 0; background: #f5f6fa; color: #333; }
  .header { background: #2d3436; color: white; padding: 16px 24px; }
  .header h1 { margin: 0; font-size: 20px; font-weight: 500; }
  .tabs { display: flex; background: #fff; border-bottom: 2px solid #dfe6e9; padding: 0 24px; }
  .tab { padding: 12px 20px; text-decoration: none; color: #636e72; font-size: 14px; font-weight: 500; border-bottom: 2px solid transparent; margin-bottom: -2px; }
  .tab:hover { color: #2d3436; }
  .tab.active { color: #0984e3; border-bottom-color: #0984e3; }
  .content { padding: 24px; max-width: 1400px; }
  .cards { display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
  .card { background: #fff; border-radius: 8px; padding: 20px; min-width: 180px; flex: 1; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
  .card .label { font-size: 12px; color: #636e72; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .value { font-size: 28px; font-weight: 600; margin-top: 4px; }
  .card .value.green { color: #00b894; }
  .card .value.orange { color: #fdcb6e; }
  .card .value.blue { color: #0984e3; }
  table { border-collapse: collapse; width: 100%; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 24px; }
  th { background: #dfe6e9; color: #2d3436; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; padding: 10px 12px; text-align: left; }
  td { padding: 10px 12px; border-top: 1px solid #f1f2f6; font-size: 13px; }
  tr:hover td { background: #f8f9fa; }
  h2 { font-size: 16px; color: #2d3436; margin: 24px 0 12px; }
  .form-section { background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 24px; }
  .form-section h3 { margin-top: 0; font-size: 15px; }
  .form-row { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
  .form-row label { width: 90px; font-size: 13px; color: #636e72; }
  .form-row input, .form-row select { padding: 6px 10px; border: 1px solid #dfe6e9; border-radius: 4px; font-size: 13px; }
  .form-row input[type="text"] { width: 200px; }
  .form-row input[type="number"] { width: 120px; }
  .btn-create { background: #0984e3; color: white; border: none; border-radius: 4px; padding: 8px 20px; cursor: pointer; font-size: 13px; }
  .btn-create:hover { background: #0874c9; }
  .btn-delete { background: #d63031; color: white; border: none; border-radius: 4px; font-size: 12px; padding: 4px 12px; cursor: pointer; }
  .btn-delete:hover { background: #c0392b; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .badge-active { background: #dff9ec; color: #00b894; }
  .badge-pending { background: #ffeaa7; color: #d68910; }
  .badge-primary { background: #dfe6e9; color: #2d3436; }
  .badge-replica { background: #e8daef; color: #6c3483; }
  .empty { color: #b2bec3; font-style: italic; padding: 20px; text-align: center; }
  .card .value.red { color: #d63031; }
  .card .value.gray { color: #636e72; }
  .section-label { font-size: 11px; color: #b2bec3; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
</style>
</head>
<body>
<div class="header"><h1>SeaweedFS Block Storage</h1></div>
<div class="tabs">
  <a class="tab {{if eq .Tab "dashboard"}}active{{end}}" href="/block/">Dashboard</a>
  <a class="tab {{if eq .Tab "ops"}}active{{end}}" href="/block/ops">Operations</a>
</div>
<div class="content">
`

// blockDashContentHTML is the Dashboard tab content.
const blockDashContentHTML = `
<div class="cards">
  <div class="card">
    <div class="label">Block Servers</div>
    <div class="value blue">{{len .Servers}}</div>
  </div>
  <div class="card">
    <div class="label">Total Volumes</div>
    <div class="value">{{.TotalVolumes}}</div>
  </div>
  <div class="card">
    <div class="label">Active</div>
    <div class="value green">{{.ActiveCount}}</div>
  </div>
  <div class="card">
    <div class="label">Pending</div>
    <div class="value orange">{{.PendingCount}}</div>
  </div>
  <div class="card">
    <div class="label">Total Capacity</div>
    <div class="value">{{.TotalSizeMB}} MB</div>
  </div>
</div>

<div class="section-label">Cluster Health</div>
<div class="cards">
  <div class="card">
    <div class="label">Barrier Lag LSN</div>
    <div class="value {{if gt .BarrierLagLSN 1000}}red{{else if gt .BarrierLagLSN 100}}orange{{else}}green{{end}}">{{.BarrierLagLSN}}</div>
  </div>
  <div class="card">
    <div class="label">Promotions</div>
    <div class="value gray">{{.PromotionsTotal}}</div>
  </div>
  <div class="card">
    <div class="label">Failovers</div>
    <div class="value {{if gt .FailoversTotal 0}}orange{{else}}gray{{end}}">{{.FailoversTotal}}</div>
  </div>
  <div class="card">
    <div class="label">Rebuilds</div>
    <div class="value {{if gt .RebuildsTotal 0}}orange{{else}}gray{{end}}">{{.RebuildsTotal}}</div>
  </div>
  <div class="card">
    <div class="label">Queue Depth</div>
    <div class="value {{if gt .AssignmentQueueLen 10}}red{{else if gt .AssignmentQueueLen 0}}orange{{else}}green{{end}}">{{.AssignmentQueueLen}}</div>
  </div>
</div>

<h2>Servers</h2>
{{if .Servers}}
<table>
  <tr><th>Address</th><th>Volume Count</th><th>Block Capable</th></tr>
  {{range .Servers}}
  <tr>
    <td>{{.Address}}</td>
    <td>{{.VolumeCount}}</td>
    <td>{{if .BlockCapable}}Yes{{else}}No{{end}}</td>
  </tr>
  {{end}}
</table>
{{else}}
<div class="empty">No block servers connected</div>
{{end}}

<h2>Volumes</h2>
{{if .Volumes}}
<table>
  <tr>
    <th>Name</th><th>Server</th><th>Size</th><th>Role</th><th>Status</th>
    <th>Durability</th><th>Health</th><th>WAL LSN</th><th>WAL Lag</th><th>Degraded</th>
    <th>Epoch</th><th>Replica</th>
  </tr>
  {{range .Volumes}}
  <tr>
    <td>{{.Name}}</td>
    <td>{{.VolumeServer}}</td>
    <td>{{.SizeMB}} MB</td>
    <td>{{if eq .Role "primary"}}<span class="badge badge-primary">primary</span>{{else if eq .Role "replica"}}<span class="badge badge-replica">replica</span>{{else}}{{.Role}}{{end}}</td>
    <td>{{if eq .Status "active"}}<span class="badge badge-active">active</span>{{else}}<span class="badge badge-pending">{{.Status}}</span>{{end}}</td>
    <td>{{if eq .DurabilityMode "sync_all"}}<span class="badge" style="background:#dfe6e9;color:#d63031">sync_all</span>{{else if eq .DurabilityMode "sync_quorum"}}<span class="badge" style="background:#dfe6e9;color:#e17055">sync_quorum</span>{{else}}best_effort{{end}}</td>
    <td style="color:{{if ge .HealthScore 0.9}}#00b894{{else if ge .HealthScore 0.5}}#fdcb6e{{else}}#d63031{{end}}">{{printf "%.2f" .HealthScore}}</td>
    <td>{{.WALHeadLSN}}</td>
    <td style="color:{{if gt .MaxWALLag 1000}}#d63031{{else if gt .MaxWALLag 100}}#fdcb6e{{else}}#00b894{{end}}">{{.MaxWALLag}}</td>
    <td>{{if .ReplicaDegraded}}<span class="badge" style="background:#ffeaa7;color:#d68910">yes</span>{{else}}-{{end}}</td>
    <td>{{.Epoch}}</td>
    <td>{{.ReplicaServer}}</td>
  </tr>
  {{end}}
</table>
{{else}}
<div class="empty">No block volumes</div>
{{end}}

</div></body></html>`

// blockOpsContentHTML is the Operations tab content.
const blockOpsContentHTML = `
<div class="form-section">
  <h3>Create Volume</h3>
  <form id="createForm">
    <div class="form-row">
      <label>Name:</label>
      <input type="text" id="cName" required placeholder="my-volume">
    </div>
    <div class="form-row">
      <label>Size (MB):</label>
      <input type="number" id="cSize" required min="1" placeholder="100">
    </div>
    <div class="form-row">
      <label>Placement:</label>
      <select id="cPlacement">
        <option value="000">000 - No replica</option>
        <option value="001" selected>001 - Different server</option>
        <option value="010">010 - Different rack</option>
        <option value="100">100 - Different DC</option>
      </select>
    </div>
    <div class="form-row">
      <label>Disk type:</label>
      <input type="text" id="cDisk" placeholder="ssd">
    </div>
    <div class="form-row">
      <label>RF:</label>
      <select id="cRF">
        <option value="0" selected>Default (2)</option>
        <option value="1">1 - No replica</option>
        <option value="2">2</option>
        <option value="3">3</option>
      </select>
    </div>
    <div class="form-row">
      <label>Durability:</label>
      <select id="cDurability">
        <option value="" selected>best_effort (default)</option>
        <option value="sync_all">sync_all</option>
        <option value="sync_quorum">sync_quorum</option>
      </select>
    </div>
    <div class="form-row">
      <label></label>
      <button type="submit" class="btn-create">Create Volume</button>
    </div>
  </form>
</div>

<h2>Volumes</h2>
{{if .Volumes}}
<table>
  <tr>
    <th>Name</th><th>Server</th><th>Size</th><th>Role</th><th>Status</th>
    <th>Durability</th><th>Health</th><th>WAL Lag</th><th>Degraded</th>
    <th>Epoch</th><th>Replica</th><th>Action</th>
  </tr>
  {{range .Volumes}}
  <tr>
    <td>{{.Name}}</td>
    <td>{{.VolumeServer}}</td>
    <td>{{.SizeMB}} MB</td>
    <td>{{if eq .Role "primary"}}<span class="badge badge-primary">primary</span>{{else if eq .Role "replica"}}<span class="badge badge-replica">replica</span>{{else}}{{.Role}}{{end}}</td>
    <td>{{if eq .Status "active"}}<span class="badge badge-active">active</span>{{else}}<span class="badge badge-pending">{{.Status}}</span>{{end}}</td>
    <td>{{if eq .DurabilityMode "sync_all"}}<span class="badge" style="background:#dfe6e9;color:#d63031">sync_all</span>{{else if eq .DurabilityMode "sync_quorum"}}<span class="badge" style="background:#dfe6e9;color:#e17055">sync_quorum</span>{{else}}best_effort{{end}}</td>
    <td style="color:{{if ge .HealthScore 0.9}}#00b894{{else if ge .HealthScore 0.5}}#fdcb6e{{else}}#d63031{{end}}">{{printf "%.2f" .HealthScore}}</td>
    <td style="color:{{if gt .MaxWALLag 1000}}#d63031{{else if gt .MaxWALLag 100}}#fdcb6e{{else}}#00b894{{end}}">{{.MaxWALLag}}</td>
    <td>{{if .ReplicaDegraded}}<span class="badge" style="background:#ffeaa7;color:#d68910">yes</span>{{else}}-{{end}}</td>
    <td>{{.Epoch}}</td>
    <td>{{.ReplicaServer}}</td>
    <td><button class="btn-delete" onclick="deleteVol('{{.Name}}')">Delete</button></td>
  </tr>
  {{end}}
</table>
{{else}}
<div class="empty">No block volumes</div>
{{end}}

<script>
document.getElementById('createForm').addEventListener('submit', function(e) {
  e.preventDefault();
  var btn = e.target.querySelector('button[type="submit"]');
  btn.disabled = true;
  btn.textContent = 'Creating...';
  fetch('/block/volume', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      name: document.getElementById('cName').value,
      size_bytes: parseInt(document.getElementById('cSize').value) * 1024 * 1024,
      replica_placement: document.getElementById('cPlacement').value,
      disk_type: document.getElementById('cDisk').value,
      replica_factor: parseInt(document.getElementById('cRF').value) || 0,
      durability_mode: document.getElementById('cDurability').value
    })
  }).then(function(r) {
    if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
    window.location.reload();
  }).catch(function(err) {
    alert('Create failed: ' + err.message);
    btn.disabled = false;
    btn.textContent = 'Create Volume';
  });
});
function deleteVol(name) {
  if (!confirm('Delete block volume "' + name + '"?')) return;
  fetch('/block/volume/' + encodeURIComponent(name), {method: 'DELETE'})
    .then(function(r) {
      if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || r.statusText); });
      window.location.reload();
    }).catch(function(err) { alert('Delete failed: ' + err.message); });
}
</script>
</div></body></html>`
