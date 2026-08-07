package dashboard

import (
	"net/http"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) ServeIndex(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SeaweedFS Telemetry Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #2196F3;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }
        .chart-container {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .chart-title {
            font-size: 1.2em;
            font-weight: bold;
            margin-bottom: 15px;
        }
        .chart-subtitle {
            color: #666;
            margin: -10px 0 15px;
        }
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        .cluster-lookup {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }
        .cluster-lookup input {
            flex: 1;
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-family: monospace;
        }
        .cluster-lookup button {
            padding: 8px 20px;
            border: none;
            border-radius: 4px;
            background: #2196F3;
            color: white;
            cursor: pointer;
        }
        .error {
            background: #ffebee;
            color: #c62828;
            padding: 15px;
            border-radius: 4px;
            margin: 10px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>SeaweedFS Telemetry Dashboard</h1>
            <p>Privacy-respecting usage analytics for SeaweedFS</p>
        </div>

        <div id="loading" class="loading">Loading telemetry data...</div>
        <div id="error" class="error" style="display: none;"></div>

        <div id="dashboard" style="display: none;">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="totalInstances">-</div>
                    <div class="stat-label">Total Instances (30 days)</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="activeInstances">-</div>
                    <div class="stat-label">Active Instances (7 days)</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="confirmedInstances">-</div>
                    <div class="stat-label">Confirmed Clusters (2+ days)</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="totalVersions">-</div>
                    <div class="stat-label">Different Versions</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="totalOS">-</div>
                    <div class="stat-label">Operating Systems</div>
                </div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Version Distribution</div>
                <div class="chart-subtitle" id="versionTotal"></div>
                <div style="position: relative; height: 420px;">
                    <canvas id="versionChart"></canvas>
                </div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Operating System Distribution</div>
                <canvas id="osChart" width="400" height="200"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Cluster Sizes Over Time</div>
                <div class="chart-subtitle" id="clusterSizesTotal"></div>
                <div style="position: relative; height: 420px;">
                    <canvas id="clusterSizeChart"></canvas>
                </div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Volume Servers Over Time</div>
                <div class="chart-subtitle" id="clusterServersTotal"></div>
                <div style="position: relative; height: 420px;">
                    <canvas id="serverChart"></canvas>
                </div>
            </div>

            <div class="chart-container">
                <div class="chart-title">Per-Cluster History</div>
                <div class="cluster-lookup">
                    <input type="text" id="clusterIdInput" placeholder="cluster UUID"
                           onkeydown="if (event.key === 'Enter') loadClusterHistory()">
                    <button onclick="loadClusterHistory()">Load</button>
                </div>
                <div id="clusterHistoryError" class="error" style="display: none;"></div>
                <div id="clusterHistoryCharts" style="display: none;">
                    <canvas id="clusterDiskChart" width="400" height="150"></canvas>
                    <canvas id="clusterVolumeChart" width="400" height="150"></canvas>
                </div>
            </div>
        </div>
    </div>

    <script>
        let charts = {};
        let clusterSizeIds = [];

        async function loadDashboard() {
            try {
                // Load stats
                const statsResponse = await fetch('/api/stats');
                const stats = await statsResponse.json();

                // Load per-cluster sizes over time
                const sizesResponse = await fetch('/api/cluster-sizes?days=30&limit=20');
                const sizes = await sizesResponse.json();

                // Load the fleet's version make-up over time
                const versionsResponse = await fetch('/api/versions?days=30&limit=8');
                const versions = await versionsResponse.json();

                updateStats(stats);
                createPieChart('osChart', stats.os_distribution || {});
                updateVersions(versions);
                updateClusterSizes(sizes);

                document.getElementById('loading').style.display = 'none';
                document.getElementById('dashboard').style.display = 'block';
            } catch (error) {
                console.error('Error loading dashboard:', error);
                showError('Failed to load telemetry data: ' + error.message);
            }
        }

        function updateStats(stats) {
            document.getElementById('totalInstances').textContent = stats.total_instances || 0;
            document.getElementById('activeInstances').textContent = stats.active_instances || 0;
            document.getElementById('confirmedInstances').textContent = stats.confirmed_instances || 0;
            document.getElementById('totalVersions').textContent = Object.keys(stats.versions || {}).length;
            document.getElementById('totalOS').textContent = Object.keys(stats.os_distribution || {}).length;
        }

        function createPieChart(canvasId, data) {
            const ctx = document.getElementById(canvasId).getContext('2d');
            
            if (charts[canvasId]) {
                charts[canvasId].destroy();
            }
            
            const labels = Object.keys(data);
            const values = Object.values(data);
            
            charts[canvasId] = new Chart(ctx, {
                type: 'pie',
                data: {
                    labels: labels,
                    datasets: [{
                        data: values,
                        backgroundColor: [
                            '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                            '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: {
                            position: 'bottom'
                        }
                    }
                }
            });
        }

        function createLineChart(canvasId, label, labels, data, color) {
            const ctx = document.getElementById(canvasId).getContext('2d');
            
            if (charts[canvasId]) {
                charts[canvasId].destroy();
            }
            
            charts[canvasId] = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: label,
                        data: data,
                        borderColor: color,
                        backgroundColor: color + '20',
                        fill: true,
                        tension: 0.1
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }

        // Decimal units, so the round numbers the chart picks for its ticks
        // come out as round labels.
        function formatBytes(bytes) {
            const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
            let value = bytes || 0, unit = 0;
            while (value >= 1000 && unit < units.length - 1) {
                value /= 1000;
                unit++;
            }
            return (unit === 0 ? value : value.toFixed(value >= 100 ? 0 : 1)) + ' ' + units[unit];
        }

        // Fixed hues rather than the evenly spaced ones the cluster stacks use:
        // a handful of versions is a set you read, and evenly spaced hues put
        // pairs next to each other that colour-blind readers can't separate.
        const versionColors = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100',
                               '#e87ba4', '#008300', '#4a3aa7', '#e34948'];

        // One stacked band per version: the band is how many clusters ran that
        // version that day, the top of the stack is the confirmed fleet, so the
        // chart shows both how it grows and what it upgrades to.
        function updateVersions(series) {
            const versions = series.versions || [];
            const dates = series.dates || [];
            const total = series.total_clusters || 0;
            document.getElementById('versionTotal').textContent =
                total + ' cluster' + (total === 1 ? '' : 's') + ' on ' + (dates[dates.length - 1] || 'no data');

            // Newest release on the floor, oldest on top: the current release
            // is the band being read, and one anchored to the baseline reads
            // straight off the axis instead of riding on everything below it.
            // Colours follow the same order, so a release keeps its colour as
            // older ones age out from the top.
            const datasets = versions.slice().reverse().map((v, i) =>
                band(v.version, v.clusters, '#ffffff', versionColors[i % versionColors.length], 2));
            if (series.other) {
                datasets.push(band('other (' + series.other.count + ' versions)', series.other.clusters,
                    '#ffffff', '#9a9a94', 2));
            }

            stackedArea('versionChart', dates, datasets, value => value, { plugins: [bandLabels] });
        }

        // Writes each version into its own band, so the chart reads without
        // matching colours against the legend. Bands too thin to hold the text
        // keep it, and the halo carries it over whatever it crosses.
        const bandLabels = {
            id: 'bandLabels',
            afterDatasetsDraw(chart) {
                const ctx = chart.ctx;
                ctx.save();
                ctx.font = '600 12px -apple-system, BlinkMacSystemFont, sans-serif';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.lineJoin = 'round';
                chart.data.datasets.forEach((dataset, d) => {
                    const meta = chart.getDatasetMeta(d);
                    if (meta.hidden) return;
                    // Label where the band is thickest, so the text has room.
                    let at = -1, thickest = 0;
                    dataset.data.forEach((value, i) => {
                        if (value > thickest) {
                            thickest = value;
                            at = i;
                        }
                    });
                    const point = at >= 0 && meta.data[at];
                    if (!point) return;
                    const below = d === 0 ? chart.scales.y.getPixelForValue(0)
                                          : chart.getDatasetMeta(d - 1).data[at].y;
                    if (below - point.y < 18) return;
                    ctx.lineWidth = 3;
                    ctx.strokeStyle = 'rgba(255, 255, 255, 0.85)';
                    ctx.strokeText(dataset.label, point.x, (point.y + below) / 2);
                    ctx.fillStyle = '#1a1a1a';
                    ctx.fillText(dataset.label, point.x, (point.y + below) / 2);
                });
                ctx.restore();
            }
        };

        // Disk usage and volume servers both drawn as one stacked band per
        // cluster over time: the band is that cluster's share, the top of the
        // stack is the fleet total. Clusters beyond the requested limit are
        // summed into a trailing "other" band so the stack still adds up.
        function updateClusterSizes(series) {
            const clusters = series.clusters || [];
            const dates = series.dates || [];
            const count = series.cluster_count || 0;
            const servers = series.total_servers || 0;
            const across = ' across ' + count + ' cluster' + (count === 1 ? '' : 's') +
                ' on ' + (dates[dates.length - 1] || 'no data');

            document.getElementById('clusterSizesTotal').textContent = formatBytes(series.total_disk) + across;
            document.getElementById('clusterServersTotal').textContent =
                servers + ' volume server' + (servers === 1 ? '' : 's') + across;

            clusterSizeIds = clusters.map(c => c.cluster_id);
            if (series.other) {
                clusterSizeIds.push(null);
            }

            stackedChart('clusterSizeChart', dates, clusters, series.other, 'disk', formatBytes);
            stackedChart('serverChart', dates, clusters, series.other, 'servers', value => value);
        }

        // The stacks share one cluster order, so a cluster keeps its colour and
        // its legend entry across both of them.
        function stackedChart(canvasId, dates, clusters, other, key, format) {
            const datasets = clusters.map((c, i) => {
                // Evenly spaced hues keep neighbouring bands distinguishable.
                const hue = Math.round(i * 360 / clusters.length);
                return band(c.cluster_id.slice(0, 8), c[key],
                    'hsl(' + hue + ', 65%, 45%)', 'hsla(' + hue + ', 65%, 55%, 0.75)');
            });
            if (other) {
                datasets.push(band('other (' + other.count + ')', other[key],
                    '#9E9E9E', 'rgba(158, 158, 158, 0.6)'));
            }

            stackedArea(canvasId, dates, datasets, format, {
                onClick: (event, elements) => {
                    const id = elements.length && clusterSizeIds[elements[0].datasetIndex];
                    if (id) {
                        document.getElementById('clusterIdInput').value = id;
                        loadClusterHistory();
                    }
                },
                // The legend shows shortened ids; the tooltip has room for the
                // full one to paste into the lookup box.
                label: item => (clusterSizeIds[item.datasetIndex] || item.dataset.label) + ': ' + format(item.raw)
            });
        }

        // Draws the bands as one stack, so their heights add up to the day's
        // total. hooks.onClick, hooks.label and hooks.plugins are optional.
        function stackedArea(canvasId, dates, datasets, format, hooks) {
            hooks = hooks || {};
            const ctx = document.getElementById(canvasId).getContext('2d');
            if (charts[canvasId]) {
                charts[canvasId].destroy();
            }
            charts[canvasId] = new Chart(ctx, {
                type: 'line',
                data: { labels: dates, datasets: datasets },
                plugins: hooks.plugins,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'band', intersect: false },
                    onClick: hooks.onClick,
                    plugins: {
                        legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } },
                        tooltip: {
                            callbacks: {
                                label: hooks.label || (item => item.dataset.label + ': ' + format(item.raw))
                            }
                        }
                    },
                    scales: {
                        x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            ticks: { precision: 0, callback: value => format(value) }
                        }
                    }
                }
            });
        }

        // Hover and click resolve to the band the pointer is inside. Chart.js's
        // built-in modes match the nearest line, which on a stack of thin bands
        // is rarely the one under the pointer.
        Chart.Interaction.modes.band = function(chart, event) {
            const last = chart.data.labels.length - 1;
            if (last < 0) return [];
            const index = Math.min(Math.max(Math.round(chart.scales.x.getValueForPixel(event.x)), 0), last);
            const value = chart.scales.y.getValueForPixel(event.y);
            let stacked = 0;
            for (let d = 0; d < chart.data.datasets.length; d++) {
                stacked += chart.data.datasets[d].data[index] || 0;
                if (value <= stacked) {
                    return [{ element: chart.getDatasetMeta(d).data[index], datasetIndex: d, index: index }];
                }
            }
            return [];
        };

        function band(label, data, borderColor, backgroundColor, borderWidth) {
            return {
                label: label,
                data: data,
                borderColor: borderColor,
                backgroundColor: backgroundColor,
                borderWidth: borderWidth || 1,
                pointRadius: 0,
                pointHitRadius: 8,
                fill: true,
                tension: 0.1
            };
        }

        async function loadClusterHistory() {
            const id = document.getElementById('clusterIdInput').value.trim();
            const errorDiv = document.getElementById('clusterHistoryError');
            const chartsDiv = document.getElementById('clusterHistoryCharts');
            if (!id) return;
            errorDiv.style.display = 'none';
            try {
                const resp = await fetch('/api/history?cluster_id=' + encodeURIComponent(id) + '&days=90');
                if (!resp.ok) {
                    throw new Error(resp.status === 404 ? 'Unknown cluster UUID' : 'Request failed: ' + resp.status);
                }
                const history = await resp.json();
                const samples = history.samples || [];
                if (samples.length === 0) {
                    throw new Error('No samples recorded for this cluster yet');
                }
                const dates = samples.map(s => new Date(s.ts * 1000).toISOString().slice(0, 10));
                chartsDiv.style.display = 'block';
                createLineChart('clusterDiskChart', 'Disk Usage (GB)', dates,
                    samples.map(s => Math.round(s.disk / (1024 * 1024 * 1024) * 100) / 100), '#FF9800');
                createLineChart('clusterVolumeChart', 'Volumes', dates,
                    samples.map(s => s.volumes), '#9C27B0');
            } catch (error) {
                chartsDiv.style.display = 'none';
                errorDiv.style.display = 'block';
                errorDiv.textContent = error.message;
            }
        }

        function showError(message) {
            document.getElementById('loading').style.display = 'none';
            document.getElementById('error').style.display = 'block';
            document.getElementById('error').textContent = message;
        }

        // Load dashboard on page load
        loadDashboard();
        
        // Refresh every 5 minutes
        setInterval(loadDashboard, 5 * 60 * 1000);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}
