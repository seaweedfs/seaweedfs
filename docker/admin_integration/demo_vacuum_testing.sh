#!/bin/sh

echo "🧪 SeaweedFS Vacuum Task Testing Demo"
echo "======================================"
echo ""

# Check if SeaweedFS is running
echo "📋 Checking SeaweedFS status..."
MASTER_URL="${MASTER_HOST:-master:9333}"
ADMIN_URL="${ADMIN_HOST:-admin:23646}"

if ! curl -s http://$MASTER_URL/cluster/status > /dev/null; then
    echo "❌ SeaweedFS master not running at $MASTER_URL"
    echo "   Please ensure Docker cluster is running: make start"
    exit 1
fi

if ! curl -s http://volume1:8080/status > /dev/null; then
    echo "❌ SeaweedFS volume servers not running"
    echo "   Please ensure Docker cluster is running: make start"
    exit 1
fi

if ! curl -s http://$ADMIN_URL/ > /dev/null; then
    echo "❌ SeaweedFS admin server not running at $ADMIN_URL"
    echo "   Please ensure Docker cluster is running: make start"
    exit 1
fi

echo "✅ All SeaweedFS components are running"
echo ""

# Phase 1: Create test data
echo "📁 Phase 1: Creating test data with garbage..."
go run create_vacuum_test_data.go -master=$MASTER_URL -files=15 -delete=0.5 -size=150
echo ""

# Phase 2: Check initial status
echo "📊 Phase 2: Checking initial volume status..."
go run create_vacuum_test_data.go -master=$MASTER_URL -files=0
echo ""

# Phase 3: Configure vacuum
echo "⚙️  Phase 3: Vacuum configuration instructions..."
echo "   1. Visit: http://localhost:23646/maintenance/config/vacuum"
echo "   2. Set these values for testing:"
echo "      - Enable Vacuum Tasks: ✅ Checked"
echo "      - Garbage Threshold: 0.30"
echo "      - Scan Interval: [30] [Seconds]"
echo "      - Min Volume Age: [0] [Minutes]"
echo "      - Max Concurrent: 2"
echo "   3. Click 'Save Configuration'"
echo ""

read -p "   Press ENTER after configuring vacuum settings..."
echo ""

# Phase 4: Monitor tasks
echo "🎯 Phase 4: Monitoring vacuum tasks..."
echo "   Visit: http://localhost:23646/maintenance"
echo "   You should see vacuum tasks appear within 30 seconds"
echo ""

echo "   Waiting 60 seconds for vacuum detection and execution..."
for i in {60..1}; do
    printf "\r   Countdown: %02d seconds" $i
    sleep 1
done
echo ""
echo ""

# Phase 5: Check results
echo "📈 Phase 5: Checking results after vacuum..."
go run create_vacuum_test_data.go -master=$MASTER_URL -files=0
echo ""

# Phase 6: Create more garbage for continuous testing
echo "🔄 Phase 6: Creating additional garbage for continuous testing..."
echo "   Running 3 rounds of garbage creation..."

for round in {1..3}; do
    echo "   Round $round: Creating garbage..."
    go run create_vacuum_test_data.go -master=$MASTER_URL -files=8 -delete=0.6 -size=100
    echo "   Waiting 30 seconds before next round..."
    sleep 30
done

echo ""
echo "📊 Final volume status:"
go run create_vacuum_test_data.go -master=$MASTER_URL -files=0
echo ""

echo "🎉 Demo Complete!"
echo ""
echo "🔍 Things to check:"
echo "   1. Maintenance Queue: http://localhost:23646/maintenance"
echo "   2. Volume Status: http://localhost:9333/vol/status"
echo "   3. Admin Dashboard: http://localhost:23646"
echo ""
echo "💡 Next Steps:"
echo "   - Try different garbage thresholds (0.10, 0.50, 0.80)"
echo "   - Adjust scan intervals (10s, 1m, 5m)"
echo "   - Monitor logs for vacuum operations"
echo "   - Test with multiple volumes"
echo "" 