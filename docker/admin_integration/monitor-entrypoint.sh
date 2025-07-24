#!/bin/sh

set -e

echo "Starting Cluster Monitor..."
echo "Master Address: $MASTER_ADDRESS"
echo "Admin Address: $ADMIN_ADDRESS"
echo "Filer Address: $FILER_ADDRESS"
echo "Monitor Interval: $MONITOR_INTERVAL"

# Wait for core services to be ready
echo "Waiting for core services to be ready..."

echo "Waiting for master..."
until curl -f http://$MASTER_ADDRESS/cluster/status > /dev/null 2>&1; do
    echo "Master not ready, waiting..."
    sleep 5
done
echo "Master is ready!"

echo "Waiting for admin..."
until curl -f http://$ADMIN_ADDRESS/health > /dev/null 2>&1; do
    echo "Admin not ready, waiting..."
    sleep 5
done
echo "Admin is ready!"

echo "Waiting for filer..."
until curl -f http://$FILER_ADDRESS/ > /dev/null 2>&1; do
    echo "Filer not ready, waiting..."
    sleep 5
done
echo "Filer is ready!"

echo "All services ready! Starting monitor..."

# Start the monitor
exec ./monitor 