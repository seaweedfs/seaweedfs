#!/bin/sh

set -e

echo "Starting Load Generator..."
echo "Filer Address: $FILER_ADDRESS"
echo "Write Rate: $WRITE_RATE files/sec"
echo "Delete Rate: $DELETE_RATE files/sec"
echo "File Size Range: $FILE_SIZE_MIN - $FILE_SIZE_MAX"
echo "Test Duration: $TEST_DURATION seconds"

# Wait for filer to be ready
echo "Waiting for filer to be ready..."
until curl -f http://$FILER_ADDRESS/ > /dev/null 2>&1; do
    echo "Filer not ready, waiting..."
    sleep 5
done
echo "Filer is ready!"

# Start the load generator
exec ./load-generator 