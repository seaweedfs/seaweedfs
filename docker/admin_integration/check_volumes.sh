#!/bin/sh

echo "📊 Quick Volume Status Check"
echo "============================"
echo ""

# Check if master is running
MASTER_URL="${MASTER_HOST:-master:9333}"
if ! curl -s http://$MASTER_URL/cluster/status > /dev/null; then
    echo "❌ Master server not available at $MASTER_URL"
    exit 1
fi

echo "🔍 Fetching volume status from master..."
curl -s "http://$MASTER_URL/vol/status" | jq -r '
if .Volumes and .Volumes.DataCenters then
  .Volumes.DataCenters | to_entries[] | .value | to_entries[] | .value | to_entries[] | .value | if . then .[] else empty end |
  "Volume \(.Id):
    Size: \(.Size | if . < 1024 then "\(.) B" elif . < 1048576 then "\(. / 1024 | floor) KB" elif . < 1073741824 then "\(. / 1048576 * 100 | floor / 100) MB" else "\(. / 1073741824 * 100 | floor / 100) GB" end)
    Files: \(.FileCount) active, \(.DeleteCount) deleted
    Garbage: \(.DeletedByteCount | if . < 1024 then "\(.) B" elif . < 1048576 then "\(. / 1024 | floor) KB" elif . < 1073741824 then "\(. / 1048576 * 100 | floor / 100) MB" else "\(. / 1073741824 * 100 | floor / 100) GB" end) (\(if .Size > 0 then (.DeletedByteCount / .Size * 100 | floor) else 0 end)%)
    Status: \(if (.DeletedByteCount / .Size * 100) > 30 then "🎯 NEEDS VACUUM" else "✅ OK" end)
"
else
  "No volumes found"
end'

echo ""
echo "💡 Legend:"
echo "   🎯 NEEDS VACUUM: >30% garbage ratio"
echo "   ✅ OK: <30% garbage ratio" 
echo "" 