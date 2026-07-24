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
                <canvas id="versionChart" width="400" height="200"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Operating System Distribution</div>
                <canvas id="osChart" width="400" height="200"></canvas>
            </div>

            

            <div class="chart-container">
                <div class="chart-title">Volume Servers Over Time</div>
                <canvas id="serverChart" width="400" height="200"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Total Disk Usage Over Time</div>
                <canvas id="diskChart" width="400" height="200"></canvas>
            </div>

            <div class="chart-container">
                <div class="chart-title">Cluster Sizes Over Time</div>
                <div class="chart-subtitle" id="clusterSizesTotal"></div>
                <div style="position: relative; height: 420px;">
                    <canvas id="clusterSizeChart"></canvas>
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
                
                // Load metrics
                const metricsResponse = await fetch('/api/metrics?days=30');
                const metrics = await metricsResponse.json();

                // Load per-cluster sizes over time
                const sizesResponse = await fetch('/api/cluster-sizes?days=30&limit=20');
                const sizes = await sizesResponse.json();

                updateStats(stats);
                updateCharts(stats, metrics);
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

        function updateCharts(stats, metrics) {
            // Version chart
            createPieChart('versionChart', 'Version Distribution', stats.versions || {});
            
            // OS chart
            createPieChart('osChart', 'Operating System Distribution', stats.os_distribution || {});
            

            
            // Server count over time
            if (metrics.dates && metrics.server_counts) {
                createLineChart('serverChart', 'Volume Servers', metrics.dates, metrics.server_counts, '#2196F3');
            }
            
            // Disk usage over time
            if (metrics.dates && metrics.disk_usage) {
                const diskUsageGB = metrics.disk_usage.map(bytes => Math.round(bytes / (1024 * 1024 * 1024)));
                createLineChart('diskChart', 'Disk Usage (GB)', metrics.dates, diskUsageGB, '#4CAF50');
            }
        }

        function createPieChart(canvasId, title, data) {
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

        // One stacked band per cluster over time: the band is that cluster's
        // size, the top of the stack is the fleet total. Clusters beyond the
        // requested limit are summed into a trailing "other" band so the stack
        // still adds up.
        function updateClusterSizes(series) {
            const clusters = series.clusters || [];
            const dates = series.dates || [];
            const count = series.cluster_count || 0;

            document.getElementById('clusterSizesTotal').textContent =
                formatBytes(series.total_disk) + ' across ' + count + ' cluster' + (count === 1 ? '' : 's') +
                ' on ' + (dates[dates.length - 1] || 'no data');

            clusterSizeIds = clusters.map(c => c.cluster_id);
            const datasets = clusters.map((c, i) => {
                // Evenly spaced hues keep neighbouring bands distinguishable.
                const hue = Math.round(i * 360 / clusters.length);
                return band(c.cluster_id.slice(0, 8), c.disk,
                    'hsl(' + hue + ', 65%, 45%)', 'hsla(' + hue + ', 65%, 55%, 0.75)');
            });
            if (series.other) {
                clusterSizeIds.push(null);
                datasets.push(band('other (' + series.other.count + ')', series.other.disk,
                    '#9E9E9E', 'rgba(158, 158, 158, 0.6)'));
            }

            const ctx = document.getElementById('clusterSizeChart').getContext('2d');
            if (charts.clusterSizeChart) {
                charts.clusterSizeChart.destroy();
            }
            charts.clusterSizeChart = new Chart(ctx, {
                type: 'line',
                data: { labels: dates, datasets: datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'band', intersect: false },
                    onClick: (event, elements) => {
                        const id = elements.length && clusterSizeIds[elements[0].datasetIndex];
                        if (id) {
                            document.getElementById('clusterIdInput').value = id;
                            loadClusterHistory();
                        }
                    },
                    plugins: {
                        legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } },
                        tooltip: {
                            callbacks: {
                                label: item => {
                                    const id = clusterSizeIds[item.datasetIndex];
                                    return (id || item.dataset.label) + ': ' + formatBytes(item.raw);
                                }
                            }
                        }
                    },
                    scales: {
                        x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            ticks: { callback: value => formatBytes(value) }
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

        function band(label, data, borderColor, backgroundColor) {
            return {
                label: label,
                data: data,
                borderColor: borderColor,
                backgroundColor: backgroundColor,
                borderWidth: 1,
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
