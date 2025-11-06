#!/bin/bash

# Setup monitoring for Kafka Client Load Test
# This script sets up Prometheus and Grafana configurations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MONITORING_DIR="$PROJECT_DIR/monitoring"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Create monitoring directory structure
setup_directories() {
    log_info "Setting up monitoring directories..."
    
    mkdir -p "$MONITORING_DIR/prometheus"
    mkdir -p "$MONITORING_DIR/grafana/dashboards"
    mkdir -p "$MONITORING_DIR/grafana/provisioning/dashboards"
    mkdir -p "$MONITORING_DIR/grafana/provisioning/datasources"
    
    log_success "Directories created"
}

# Create Prometheus configuration
create_prometheus_config() {
    log_info "Creating Prometheus configuration..."
    
    cat > "$MONITORING_DIR/prometheus/prometheus.yml" << 'EOF'
# Prometheus configuration for Kafka Load Test monitoring

global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

scrape_configs:
  # Scrape Prometheus itself
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Scrape load test metrics
  - job_name: 'kafka-loadtest'
    static_configs:
      - targets: ['kafka-client-loadtest-runner:8080']
    scrape_interval: 5s
    metrics_path: '/metrics'

  # Scrape SeaweedFS Master metrics
  - job_name: 'seaweedfs-master'
    static_configs:
      - targets: ['seaweedfs-master:9333']
    metrics_path: '/metrics'

  # Scrape SeaweedFS Volume metrics  
  - job_name: 'seaweedfs-volume'
    static_configs:
      - targets: ['seaweedfs-volume:8080']
    metrics_path: '/metrics'

  # Scrape SeaweedFS Filer metrics
  - job_name: 'seaweedfs-filer'
    static_configs:
      - targets: ['seaweedfs-filer:8888']
    metrics_path: '/metrics'

  # Scrape SeaweedFS MQ Broker metrics (if available)
  - job_name: 'seaweedfs-mq-broker'
    static_configs:
      - targets: ['seaweedfs-mq-broker:17777']
    metrics_path: '/metrics'
    scrape_interval: 10s

  # Scrape Kafka Gateway metrics (if available)
  - job_name: 'kafka-gateway'
    static_configs:
      - targets: ['kafka-gateway:9093']
    metrics_path: '/metrics'
    scrape_interval: 10s
EOF

    log_success "Prometheus configuration created"
}

# Create Grafana datasource configuration
create_grafana_datasource() {
    log_info "Creating Grafana datasource configuration..."
    
    cat > "$MONITORING_DIR/grafana/provisioning/datasources/datasource.yml" << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    orgId: 1
    url: http://prometheus:9090
    basicAuth: false
    isDefault: true
    editable: true
    version: 1
EOF

    log_success "Grafana datasource configuration created"
}

# Create Grafana dashboard provisioning
create_grafana_dashboard_provisioning() {
    log_info "Creating Grafana dashboard provisioning..."
    
    cat > "$MONITORING_DIR/grafana/provisioning/dashboards/dashboard.yml" << 'EOF'
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /var/lib/grafana/dashboards
EOF

    log_success "Grafana dashboard provisioning created"
}

# Create Kafka Load Test dashboard
create_loadtest_dashboard() {
    log_info "Creating Kafka Load Test Grafana dashboard..."
    
    cat > "$MONITORING_DIR/grafana/dashboards/kafka-loadtest.json" << 'EOF'
{
  "dashboard": {
    "id": null,
    "title": "Kafka Client Load Test Dashboard",
    "tags": ["kafka", "loadtest", "seaweedfs"],
    "timezone": "browser",
    "panels": [
      {
        "id": 1,
        "title": "Messages Produced/Consumed",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(kafka_loadtest_messages_produced_total[5m])",
            "legendFormat": "Produced/sec"
          },
          {
            "expr": "rate(kafka_loadtest_messages_consumed_total[5m])",
            "legendFormat": "Consumed/sec"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
      },
      {
        "id": 2,
        "title": "Message Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, kafka_loadtest_message_latency_seconds)",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.99, kafka_loadtest_message_latency_seconds)",
            "legendFormat": "99th percentile"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
      },
      {
        "id": 3,
        "title": "Error Rates",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_loadtest_producer_errors_total[5m])",
            "legendFormat": "Producer Errors/sec"
          },
          {
            "expr": "rate(kafka_loadtest_consumer_errors_total[5m])",
            "legendFormat": "Consumer Errors/sec"
          }
        ],
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8}
      },
      {
        "id": 4,
        "title": "Throughput (MB/s)",
        "type": "graph", 
        "targets": [
          {
            "expr": "rate(kafka_loadtest_bytes_produced_total[5m]) / 1024 / 1024",
            "legendFormat": "Produced MB/s"
          },
          {
            "expr": "rate(kafka_loadtest_bytes_consumed_total[5m]) / 1024 / 1024", 
            "legendFormat": "Consumed MB/s"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16}
      },
      {
        "id": 5,
        "title": "Active Clients",
        "type": "stat",
        "targets": [
          {
            "expr": "kafka_loadtest_active_producers",
            "legendFormat": "Producers"
          },
          {
            "expr": "kafka_loadtest_active_consumers", 
            "legendFormat": "Consumers"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16}
      },
      {
        "id": 6,
        "title": "Consumer Lag",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_loadtest_consumer_lag_messages",
            "legendFormat": "{{consumer_group}}-{{topic}}-{{partition}}"
          }
        ],
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 24}
      }
    ],
    "time": {"from": "now-30m", "to": "now"},
    "refresh": "5s",
    "schemaVersion": 16,
    "version": 0
  }
}
EOF

    log_success "Kafka Load Test dashboard created"
}

# Create SeaweedFS dashboard
create_seaweedfs_dashboard() {
    log_info "Creating SeaweedFS Grafana dashboard..."
    
    cat > "$MONITORING_DIR/grafana/dashboards/seaweedfs.json" << 'EOF'
{
  "dashboard": {
    "id": null,
    "title": "SeaweedFS Cluster Dashboard",
    "tags": ["seaweedfs", "storage"],
    "timezone": "browser", 
    "panels": [
      {
        "id": 1,
        "title": "Master Status",
        "type": "stat",
        "targets": [
          {
            "expr": "up{job=\"seaweedfs-master\"}",
            "legendFormat": "Master Up"
          }
        ],
        "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0}
      },
      {
        "id": 2, 
        "title": "Volume Status",
        "type": "stat",
        "targets": [
          {
            "expr": "up{job=\"seaweedfs-volume\"}",
            "legendFormat": "Volume Up"
          }
        ],
        "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0}
      },
      {
        "id": 3,
        "title": "Filer Status", 
        "type": "stat",
        "targets": [
          {
            "expr": "up{job=\"seaweedfs-filer\"}",
            "legendFormat": "Filer Up"
          }
        ],
        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0}
      },
      {
        "id": 4,
        "title": "MQ Broker Status",
        "type": "stat", 
        "targets": [
          {
            "expr": "up{job=\"seaweedfs-mq-broker\"}",
            "legendFormat": "MQ Broker Up"
          }
        ],
        "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0}
      }
    ],
    "time": {"from": "now-30m", "to": "now"},
    "refresh": "10s",
    "schemaVersion": 16,
    "version": 0
  }
}
EOF

    log_success "SeaweedFS dashboard created"
}

# Main setup function
main() {
    log_info "Setting up monitoring for Kafka Client Load Test..."
    
    setup_directories
    create_prometheus_config
    create_grafana_datasource 
    create_grafana_dashboard_provisioning
    create_loadtest_dashboard
    create_seaweedfs_dashboard
    
    log_success "Monitoring setup completed!"
    log_info "You can now start the monitoring stack with:"
    log_info "  ./scripts/run-loadtest.sh monitor"
    log_info ""
    log_info "After starting, access:"
    log_info "  Prometheus: http://localhost:9090"
    log_info "  Grafana:    http://localhost:3000 (admin/admin)"
}

main "$@"
