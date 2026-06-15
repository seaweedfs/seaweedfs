#! /bin/sh

RXE_DEV=rxe_eth0

# Remove existing devices if any
sudo rdma link delete $RXE_DEV

set -o errexit
set -o nounset
set -o xtrace

if [ `ifconfig -s | grep -c '^e'` -eq 0 ]; then
    echo "no eth device"
    exit 1
elif [ `ifconfig -s | grep -c '^e'` -gt 1 ]; then
    echo "multiple eth devices, select the first one"
    ifconfig -s | grep '^e'
fi

ETH_DEV=`ifconfig -s | grep '^e' | cut -d ' ' -f 1 | head -n 1`
ETH_IP=`ifconfig $ETH_DEV | grep inet | grep -v inet6 | awk '{print $2}' | tr -d "addr:"`
CM_PORT=7471
# Setup soft-roce device
sudo rdma link add $RXE_DEV type rxe netdev $ETH_DEV
rdma link | grep $RXE_DEV

cargo test --all
cargo run --example server &
sleep 1 &&
cargo run --example client $ETH_IP $CM_PORT