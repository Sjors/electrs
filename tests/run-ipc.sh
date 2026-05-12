#!/bin/bash
# Integration test variant that exercises the experimental Cap'n Proto IPC
# backend introduced for Bitcoin Core PR #29409. Requires a multiprocess
# `bitcoin` multi-call binary (resolved via $BITCOIN_MULTIPROCESS_BIN or
# `bitcoin` on PATH) and a freshly built electrs.
set -euo pipefail

BITCOIN_MULTIPROCESS_BIN="${BITCOIN_MULTIPROCESS_BIN:-$(command -v bitcoin || true)}"

if [ -z "$BITCOIN_MULTIPROCESS_BIN" ] || [ ! -x "$BITCOIN_MULTIPROCESS_BIN" ]; then
  echo "ERROR: multiprocess 'bitcoin' multi-call binary not found." >&2
  echo "Set BITCOIN_MULTIPROCESS_BIN to the binary built from bitcoin/bitcoin#29409," >&2
  echo "or place it on PATH as 'bitcoin'." >&2
  exit 1
fi

rm -rf data/
mkdir -p data/{bitcoin,electrum,electrs}
touch data/electrs/regtest-debug.log data/electrum/regtest-debug.log

cleanup() {
  trap - SIGTERM SIGINT
  set +e +o pipefail
  jobs
  for j in `jobs -rp`
  do
  	kill $j
  	wait $j
  done
}
trap cleanup SIGINT SIGTERM EXIT

wait_for() {
  CMD=$1
  shift
  TEST_ARGS=$*
  for _ in `seq 0 9`; do
    if test "$($CMD | jq -c .)" $TEST_ARGS; then
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for condition: $CMD $TEST_ARGS" >&2
  return 1
}

wait_for_log() {
  LOG_FILE=$1
  PATTERN=$2
  DESCRIPTION=$3
  for _ in `seq 0 99`; do
    if grep -Fq "$PATTERN" "$LOG_FILE"; then
      return 0
    fi
    sleep 0.2
  done

  echo "Timed out waiting for $DESCRIPTION in $LOG_FILE" >&2
  tail -n +1 "$LOG_FILE" >&2 || true
  return 1
}

wait_for_http() {
  URL=$1
  OUTPUT=$2
  for _ in `seq 0 99`; do
    if curl --silent --show-error --fail "$URL" -o "$OUTPUT"; then
      return 0
    fi
    sleep 0.2
  done

  echo "Timed out waiting for HTTP endpoint $URL" >&2
  return 1
}

BTC="$BITCOIN_MULTIPROCESS_BIN rpc -chain=regtest -datadir=$PWD/data/bitcoin"
ELECTRUM="electrum --regtest"
EL="$ELECTRUM --wallet=data/electrum/wallet"
SOCK="$PWD/data/bitcoin/regtest/node.sock"

echo "Starting $($BITCOIN_MULTIPROCESS_BIN node -version | head -n1) (multiprocess, IPC enabled)..."
$BITCOIN_MULTIPROCESS_BIN node \
  -regtest -datadir=$PWD/data/bitcoin \
  -ipcbind=unix \
  -listen=0 \
  -printtoconsole=0 \
  -fallbackfee=0.0001 &
BITCOIND_PID=$!

# Wait for both RPC and the IPC socket.
$BTC -rpcwait getblockcount > /dev/null
for _ in `seq 0 50`; do
  test -S "$SOCK" && break || sleep 0.2
done
test -S "$SOCK" || { echo "IPC socket $SOCK never appeared" >&2; exit 1; }
echo "IPC socket ready: $SOCK"

echo "Creating Electrum `electrum version --offline` wallet..."
WALLET=`$EL --offline create --seed_type=segwit`
MINING_ADDR=`$EL --offline getunusedaddress`

$BTC generatetoaddress 110 $MINING_ADDR > /dev/null
echo `$BTC getblockchaininfo | jq -r '"Generated \(.blocks) regtest blocks (\(.size_on_disk/1e3) kB)"'` to $MINING_ADDR

TIP=`$BTC getbestblockhash`

export RUST_LOG=electrs=debug
electrs \
  --db-dir=data/electrs \
  --daemon-dir=data/bitcoin \
  --network=regtest \
  --daemon-ipc-socket="$SOCK" \
  2> data/electrs/regtest-debug.log &
ELECTRS_PID=$!
wait_for_log data/electrs/regtest-debug.log "serving Electrum RPC" "electrs Electrum RPC startup"
wait_for_log data/electrs/regtest-debug.log "connecting to bitcoin-node IPC socket" "electrs IPC backend connection"

wait_for_http http://localhost:24224 metrics.txt

$ELECTRUM daemon --server localhost:60401:t -1 -vDEBUG 2> data/electrum/regtest-debug.log &
ELECTRUM_PID=$!
wait_for_log data/electrum/regtest-debug.log "connection established" "Electrum daemon connection"
$EL getinfo | jq .

echo "Loading Electrum wallet..."
$EL load_wallet

echo "Running integration tests (IPC backend):"

echo " * getbalance"
wait_for "$EL getbalance" == '{"confirmed":"550","unmatured":"4950"}'

echo " * getunusedaddress"
NEW_ADDR=`$EL getunusedaddress`

echo " * payto & broadcast"
TXID=$($EL broadcast $($EL payto $NEW_ADDR 123 --fee 0.001 --password=''))

echo " * get_tx_status"
wait_for "$EL get_tx_status $TXID" == '{"confirmations":0}'

echo " * getaddresshistory"
test "`$EL getaddresshistory $NEW_ADDR | jq -c .`" == "[{\"fee\":100000,\"height\":0,\"tx_hash\":\"$TXID\"}]"

echo " * getbalance"
test "`$EL getbalance | jq -c .`" == '{"confirmed":"549.999","unmatured":"4950"}'

echo "Generating bitcoin block..."
$BTC generatetoaddress 1 $MINING_ADDR > /dev/null
$BTC getblockcount > /dev/null

echo " * wait for new block"
kill -USR1 $ELECTRS_PID  # notify server to index new block
wait_for_log data/electrum/regtest-debug.log "verified $TXID" "Electrum wallet verification of mined transaction"

echo " * get_tx_status"
test "`$EL get_tx_status $TXID | jq -c .`" == '{"confirmations":1}'

echo " * getaddresshistory"
test "`$EL getaddresshistory $NEW_ADDR | jq -c .`" == "[{\"height\":111,\"tx_hash\":\"$TXID\"}]"

echo " * getbalance"
test "`$EL getbalance | jq -c .`" == '{"confirmed":"599.999","unmatured":"4950.001"}'

echo "Electrum `$EL stop`"  # disconnect wallet
wait $ELECTRUM_PID

kill -INT $ELECTRS_PID  # close server
wait_for_log data/electrs/regtest-debug.log "electrs stopped" "electrs shutdown"
wait $ELECTRS_PID

# When the multiprocess node has IPC clients still attached at shutdown, the
# RPC `stop` request hangs indefinitely waiting for them to disconnect. The
# IPC client lives in electrs, which has already stopped above but whose
# capnp connection drop is not flushed back to the node. Skip the polite
# stop and just kill the node — this is a regtest fixture.
kill $BITCOIND_PID 2>/dev/null || true
wait $BITCOIND_PID 2>/dev/null || true

echo "=== PASSED (IPC) ==="
