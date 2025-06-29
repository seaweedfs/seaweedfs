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
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
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
        </div>
    </div>

    <script>
        let charts = {};

        async function loadDashboard() {
            try {
                // Load stats
                const statsResponse = await fetch('/api/stats');
                const stats = await statsResponse.json();
                
                // Load metrics
                const metricsResponse = await fetch('/api/metrics?days=30');
                const metrics = await metricsResponse.json();

                updateStats(stats);
                updateCharts(stats, metrics);
                
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
