//! Cap'n Proto IPC client for Bitcoin Core's `interfaces::Chain`.
//!
//! This is an experimental backend that talks to a multiprocess `bitcoin-node`
//! (Bitcoin Core PR #29409) over a unix socket using the libmultiprocess
//! Cap'n Proto framing. When configured, electrs uses this in place of the P2P
//! protocol for fetching raw blocks and tip information.
//!
//! The Cap'n Proto stack is asynchronous and not `Send` (the generated client
//! types contain `Rc<RefCell<...>>`). To bridge it to electrs's synchronous,
//! threaded code we run a dedicated worker thread that owns a current-thread
//! tokio runtime + `LocalSet`, and communicate via an mpsc channel of
//! type-erased async closures returning their results through a oneshot.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::thread as stdthread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bitcoin::blockdata::block::Header as BlockHeader;
use bitcoin::{
    consensus::deserialize, consensus::Decodable, hashes::Hash, Amount, BlockHash, OutPoint,
};
use bitcoin_capnp_types::{
    chain_capnp::{chain, chain_notifications},
    init_capnp::init,
    proxy_capnp::{thread as proxy_thread, thread_map},
};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty::VatNetwork, RpcSystem};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use futures::{future::LocalBoxFuture, io::BufReader, FutureExt};
use parking_lot::Mutex;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::chain::{Chain, NewHeader};
use crate::mempool::MempoolEvent;
use crate::types::SerBlock;

/// Boxed (non-Send) future returned by a job. The closure itself must be
/// `Send` so it can be shipped from caller threads to the IPC worker, but
/// once on the worker it runs on a single-threaded LocalSet, so the future
/// it produces does not need to be `Send`. Each job receives an owned `Ctx`
/// (a cheap pair of `Rc`-backed capnp clients), letting the worker spawn
/// jobs concurrently on the LocalSet without lifetime gymnastics.
type Job = Box<dyn FnOnce(Ctx) -> LocalBoxFuture<'static, ()> + Send + 'static>;

/// IPC context held by the worker thread: the chain client and the proxy
/// thread handle that every call must reference. Cloning is cheap (both
/// fields are `Rc`-backed capnp clients).
#[derive(Clone)]
struct Ctx {
    chain: chain::Client,
    thread: proxy_thread::Client,
}

/// Sync handle to the IPC worker thread. Cloneable; clones share the same
/// connection and the same header-fetch block cache.
#[derive(Clone)]
pub(crate) struct IpcChain {
    tx: mpsc::UnboundedSender<Job>,
    /// Blocks that were downloaded by [`IpcChain::get_new_headers`] (which
    /// has to fetch the full block payload over IPC just to extract the 80
    /// header bytes, since the Chain interface exposes no header-only
    /// fetch). The very next thing electrs's indexing pipeline does is call
    /// [`Daemon::for_blocks`] over the same hashes, so handing those
    /// payloads back from the cache avoids a duplicate IPC round-trip per
    /// block. Bounded by total bytes; on overflow, inserts are skipped
    /// (best-effort) so the cache never grows without bound during initial
    /// sync.
    header_fetch_cache: Arc<Mutex<BlockCache>>,
}

pub(crate) struct IpcCoin {
    pub(crate) value: Amount,
    pub(crate) height: u32,
}

/// In-memory cache of recently fetched blocks, drained by
/// [`IpcChain::get_block`] on hit. Keyed by block hash, bounded by total
/// payload bytes.
#[derive(Default)]
struct BlockCache {
    map: HashMap<BlockHash, SerBlock>,
    bytes: usize,
}

/// Soft upper bound on the bytes held in the header-fetch cache. Sized to
/// comfortably hold one [`IpcChain::get_new_headers`] batch (capped at
/// `MAX_HEADERS` blocks) under typical mainnet block sizes (~2 MiB), with
/// headroom for a few outliers; oversized batches simply skip insertion
/// and refetch on demand.
const HEADER_FETCH_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

impl BlockCache {
    /// Best-effort insert. Skips the block if the cache is already full or
    /// if the block alone exceeds the budget; the caller's get_block path
    /// will simply refetch on the (rare) miss that follows.
    fn try_insert(&mut self, hash: BlockHash, block: SerBlock) {
        let block_bytes = block.len();
        if block_bytes > HEADER_FETCH_CACHE_MAX_BYTES {
            return;
        }
        if self.map.contains_key(&hash) {
            return;
        }
        if self.bytes + block_bytes > HEADER_FETCH_CACHE_MAX_BYTES {
            return;
        }
        self.bytes += block_bytes;
        self.map.insert(hash, block);
    }

    /// Remove and return a cached block, decreasing the byte count.
    fn take(&mut self, hash: &BlockHash) -> Option<SerBlock> {
        let block = self.map.remove(hash)?;
        self.bytes = self.bytes.saturating_sub(block.len());
        Some(block)
    }
}

impl IpcChain {
    /// Spawn the worker thread, connect to the unix socket, and bootstrap an
    /// `Init` client + `Chain` client. Blocks until the connection is ready.
    pub(crate) fn connect(socket: &Path) -> Result<Self> {
        let socket = socket.to_path_buf();
        let (tx, rx) = mpsc::unbounded_channel::<Job>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();
        stdthread::Builder::new()
            .name("electrs-ipc".into())
            .spawn(move || worker(socket, rx, ready_tx))
            .context("failed to spawn IPC worker thread")?;
        ready_rx
            .recv()
            .context("IPC worker thread exited before signalling readiness")??;
        Ok(Self {
            tx,
            header_fetch_cache: Arc::new(Mutex::new(BlockCache::default())),
        })
    }

    /// Submit a job to the worker and block on its result.
    fn call<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(Ctx) -> LocalBoxFuture<'static, Result<R>> + Send + 'static,
    {
        let (otx, orx) = oneshot::channel::<Result<R>>();
        let job: Job = Box::new(move |ctx| {
            async move {
                let res = f(ctx).await;
                let _ = otx.send(res);
            }
            .boxed_local()
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow!("IPC worker thread is gone"))?;
        orx.blocking_recv()
            .map_err(|_| anyhow!("IPC worker dropped the response"))?
    }

    pub(crate) fn get_height(&self) -> Result<Option<i32>> {
        self.call(|ctx| {
            async move {
                let mut req = ctx.chain.get_height_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                let resp = req.send().promise.await?;
                let r = resp.get()?;
                Ok(if r.get_has_result() {
                    Some(r.get_result())
                } else {
                    None
                })
            }
            .boxed_local()
        })
    }

    pub(crate) fn is_initial_block_download(&self) -> Result<bool> {
        self.call(|ctx| {
            async move {
                let mut req = ctx.chain.is_initial_block_download_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                let resp = req.send().promise.await?;
                Ok(resp.get()?.get_result())
            }
            .boxed_local()
        })
    }

    /// Register a `ChainNotifications` handler and request a one-shot replay of
    /// the node's current mempool. The returned receivers are fed by the IPC
    /// worker thread for as long as the chain notification handler is alive.
    pub(crate) fn start_notifications(&self) -> Result<(Receiver<MempoolEvent>, Receiver<()>)> {
        let (mempool_tx, mempool_rx) = unbounded::<MempoolEvent>();
        let (block_tx, block_rx) = bounded::<()>(1);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
        let job: Job = Box::new(move |ctx| {
            async move {
                register_notifications(ctx, mempool_tx, block_tx, ready_tx).await;
            }
            .boxed_local()
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow!("IPC worker thread is gone"))?;
        ready_rx
            .blocking_recv()
            .map_err(|_| anyhow!("IPC worker dropped notification setup response"))??;
        Ok((mempool_rx, block_rx))
    }

    pub(crate) fn have_pruned(&self) -> Result<bool> {
        self.call(|ctx| {
            async move {
                let mut req = ctx.chain.have_pruned_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                let resp = req.send().promise.await?;
                Ok(resp.get()?.get_result())
            }
            .boxed_local()
        })
    }

    pub(crate) fn get_block_hash(&self, height: i32) -> Result<BlockHash> {
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.get_block_hash_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                req.get().set_height(height);
                let resp = req.send().promise.await?;
                let bytes = resp.get()?.get_result()?;
                if bytes.len() != 32 {
                    bail!("unexpected block hash length {}", bytes.len());
                }
                let mut buf = [0u8; 32];
                buf.copy_from_slice(bytes);
                Ok(BlockHash::from_byte_array(buf))
            }
            .boxed_local()
        })
    }

    /// Fetch a serialized block by hash via `Chain.findBlock(wantData=true)`.
    /// Returns `Ok(None)` if the block is not known to the node.
    ///
    /// Consults the header-fetch cache first: blocks downloaded earlier by
    /// [`Self::get_new_headers`] are handed back without an IPC round-trip,
    /// and removed from the cache (consume-once semantics).
    pub(crate) fn get_block(&self, hash: BlockHash) -> Result<Option<SerBlock>> {
        if let Some(block) = self.header_fetch_cache.lock().take(&hash) {
            return Ok(Some(block));
        }
        self.fetch_block(hash)
    }

    /// Unconditional `findBlock(wantData=true)` fetch with no cache
    /// interaction; used internally by [`Self::get_block`] (on cache miss)
    /// and by [`Self::get_new_headers`] (which inserts the result into the
    /// cache for the immediately-following `for_blocks` pass).
    fn fetch_block(&self, hash: BlockHash) -> Result<Option<SerBlock>> {
        let raw: [u8; 32] = hash.to_byte_array();
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.find_block_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                req.get().set_hash(&raw);
                {
                    let mut params = req.get().init_block();
                    params.set_want_data(true);
                }
                let resp = req.send().promise.await?;
                let r = resp.get()?;
                if !r.get_result() {
                    return Ok(None);
                }
                let block = r.get_block()?;
                let data = block.get_data()?.to_vec();
                Ok(Some(data))
            }
            .boxed_local()
        })
    }

    /// Broadcast a serialized transaction via `Chain.broadcastTransaction`,
    /// using the same `MEMPOOL_AND_BROADCAST_TO_ALL` semantics as the
    /// JSON-RPC `sendrawtransaction` call. `max_tx_fee` matches the RPC
    /// default (`DEFAULT_MAX_RAW_TX_FEE = 0.10 BTC`). On failure the node's
    /// error string is propagated.
    pub(crate) fn broadcast_transaction(&self, tx_bytes: Vec<u8>) -> Result<()> {
        // node::TxBroadcast::MEMPOOL_AND_BROADCAST_TO_ALL = 0
        const BROADCAST_METHOD_DEFAULT: i32 = 0;
        // src/policy/policy.h: DEFAULT_MAX_RAW_TX_FEE = 0.10 * COIN
        const MAX_TX_FEE_DEFAULT: i64 = 10_000_000;
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.broadcast_transaction_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                {
                    let mut params = req.get();
                    params.set_tx(&tx_bytes);
                    params.set_max_tx_fee(MAX_TX_FEE_DEFAULT);
                    params.set_broadcast_method(BROADCAST_METHOD_DEFAULT);
                }
                let resp = req.send().promise.await?;
                let r = resp.get()?;
                if r.get_result() {
                    Ok(())
                } else {
                    let err = r.get_error()?.to_str()?;
                    if err.is_empty() {
                        bail!("Chain.broadcastTransaction failed");
                    }
                    bail!("Chain.broadcastTransaction failed: {err}");
                }
            }
            .boxed_local()
        })
    }

    /// Get the node's minimum relay feerate via `Chain.relayMinFee`. The
    /// reply is a serialized `CFeeRate` blob (see [`decode_fee_rate_kvb`]);
    /// this method returns the rate as satoshis per kilo-vbyte.
    ///
    /// Replaces the JSON-RPC `getnetworkinfo.relayfee` lookup used by
    /// [`Daemon::get_relay_fee`] when IPC is configured.
    pub(crate) fn relay_min_fee_sat_per_kvb(&self) -> Result<i64> {
        self.call(|ctx| {
            async move {
                let mut req = ctx.chain.relay_min_fee_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                let resp = req.send().promise.await?;
                let blob = resp.get()?.get_result()?;
                Ok(decode_fee_rate_kvb(blob)?.unwrap_or(0))
            }
            .boxed_local()
        })
    }

    /// Smart fee estimate for confirmation within `nblocks`, via
    /// `Chain.estimateSmartFee`. Returns `Ok(None)` if the node has no
    /// estimate available (a `CFeeRate{}` with `size == 0`), otherwise the
    /// rate as satoshis per kilo-vbyte.
    ///
    /// Replaces the JSON-RPC `estimatesmartfee` call used by
    /// [`Daemon::estimate_fee`] when IPC is configured.
    pub(crate) fn estimate_smart_fee_sat_per_kvb(&self, nblocks: i32) -> Result<Option<i64>> {
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.estimate_smart_fee_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                {
                    let mut params = req.get();
                    params.set_num_blocks(nblocks);
                    // Match the JSON-RPC default for `estimatesmartfee`:
                    // economical (non-conservative) mode.
                    params.set_conservative(false);
                    // We don't surface FeeCalculation diagnostics to clients.
                    params.set_want_calc(false);
                }
                let resp = req.send().promise.await?;
                let blob = resp.get()?.get_result()?;
                decode_fee_rate_kvb(blob)
            }
            .boxed_local()
        })
    }

    /// Look up prevout coins through `Chain.findCoins`. The interface takes a
    /// `std::map<COutPoint, Coin>&`, encoded as key/value byte pairs. The
    /// incoming `Coin` values are placeholders; the node overwrites them with
    /// coins from the active UTXO set or mempool.
    pub(crate) fn find_coins(&self, outpoints: Vec<OutPoint>) -> Result<Vec<IpcCoin>> {
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.find_coins_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                {
                    let mut coins = req.get().init_coins(outpoints.len() as u32);
                    for (i, outpoint) in outpoints.iter().enumerate() {
                        let mut pair = coins.reborrow().get(i as u32);
                        pair.set_key(&serialize_outpoint(outpoint)[..])?;
                        pair.set_value(DUMMY_COIN)?;
                    }
                }
                let resp = req.send().promise.await?;
                let coins = resp.get()?.get_coins()?;
                if coins.len() != outpoints.len() as u32 {
                    bail!(
                        "Chain.findCoins returned {} coins, expected {}",
                        coins.len(),
                        outpoints.len()
                    );
                }

                let mut result = Vec::with_capacity(outpoints.len());
                for i in 0..coins.len() {
                    let pair = coins.get(i);
                    result.push(decode_coin(pair.get_value()?)?);
                }
                Ok(result)
            }
            .boxed_local()
        })
    }

    /// Find the height of the highest block in `locator` that is part of the
    /// node's active chain, via `Chain.findLocatorFork`. Returns `None` if no
    /// hash in the locator is on the active chain (e.g. completely diverged
    /// chain), in which case the caller should fall back to walking from
    /// genesis.
    ///
    /// `locator` is the list of block hashes returned by [`Chain::locator`],
    /// ordered tip→genesis with exponentially increasing gaps.
    pub(crate) fn find_locator_fork(&self, locator: &[BlockHash]) -> Result<Option<i32>> {
        // Bitcoin Core CBlockLocator wire format: int32 LE version
        // (DUMMY_VERSION = 70016) + CompactSize count + 32*count hashes.
        let mut buf = Vec::with_capacity(4 + 9 + locator.len() * 32);
        buf.extend_from_slice(&70016i32.to_le_bytes());
        write_compact_size(&mut buf, locator.len() as u64);
        for hash in locator {
            buf.extend_from_slice(hash.as_byte_array());
        }
        self.call(move |ctx| {
            async move {
                let mut req = ctx.chain.find_locator_fork_request();
                req.get().get_context()?.set_thread(ctx.thread.clone());
                req.get().set_locator(&buf);
                let resp = req.send().promise.await?;
                let r = resp.get()?;
                Ok(if r.get_has_result() {
                    Some(r.get_result())
                } else {
                    None
                })
            }
            .boxed_local()
        })
    }

    /// Replacement for the P2P `getheaders` exchange in [`Daemon::get_new_headers`].
    ///
    /// Walks the IPC chain forward from the fork point between the locator
    /// (taken from `chain`) and the node's active chain, returning every new
    /// header up to the node's current tip. Each header is fetched by way of
    /// `findBlock(wantData=true)` followed by parsing the first 80 bytes as a
    /// `BlockHeader`; the chain interface does not expose a header-only
    /// fetch, so the full block payload is downloaded. To avoid an immediate
    /// duplicate fetch, the payload is stashed in the header-fetch cache and
    /// handed back to the subsequent `for_blocks` indexing pass.
    ///
    /// Caps the response at [`MAX_HEADERS`] per call so the caller can drive
    /// multiple iterations during initial sync. The cap is small (vs.
    /// the 2000-header P2P `getheaders` batch) for two reasons: (1) each
    /// header costs a full block over IPC until upstream grows a header-only
    /// fetch, so larger batches blow past the in-process block cache budget
    /// on mainnet; (2) over a local unix socket, round-trip cost is dominated
    /// by per-block work, not per-batch overhead, so the historical P2P
    /// argument for huge batches doesn't carry over.
    pub(crate) fn get_new_headers(&self, chain: &Chain) -> Result<Vec<NewHeader>> {
        /// Headers per `get_new_headers` call. See the function-level
        /// comment above for why this is much smaller than the P2P value.
        const MAX_HEADERS: usize = 100;

        let tip_height = match self.get_height()? {
            Some(h) if h >= 0 => h as usize,
            _ => return Ok(vec![]),
        };

        // Find the fork height between our chain's locator and the node's
        // active chain.
        let locator = chain.locator();
        let fork_height = self
            .find_locator_fork(&locator)?
            .ok_or_else(|| anyhow!("Chain.findLocatorFork: no common ancestor"))?;
        if fork_height < 0 {
            bail!("Chain.findLocatorFork returned negative fork height {fork_height}");
        }
        let fork_height = fork_height as usize;

        if tip_height <= fork_height {
            return Ok(vec![]);
        }

        let new_first = fork_height + 1;
        let new_last = tip_height.min(fork_height + MAX_HEADERS);
        let mut headers = Vec::with_capacity(new_last - new_first + 1);
        for h in new_first..=new_last {
            let hash = self.get_block_hash(h as i32)?;
            let block = self
                .fetch_block(hash)?
                .ok_or_else(|| anyhow!("findBlock: missing block {hash} at height {h}"))?;
            if block.len() < 80 {
                bail!(
                    "findBlock returned {} bytes for {hash}; need >=80",
                    block.len()
                );
            }
            let header = BlockHeader::consensus_decode(&mut &block[..80])
                .with_context(|| format!("parsing header at height {h}"))?;
            headers.push(NewHeader::from((header, h)));
            // Stash the payload for the for_blocks pass that will follow
            // immediately. Best-effort: when the cache is full, additional
            // blocks will simply be re-fetched on demand.
            self.header_fetch_cache.lock().try_insert(hash, block);
        }
        Ok(headers)
    }

    /// Spawn a long-running task on the worker that watches for tip changes
    /// via `Chain.waitForNotificationsIfTipChanged`, and returns a receiver
    /// that fires once per detected tip change.
    ///
    /// This replaces the P2P `inv`-watching path in `src/p2p.rs` for IPC
    /// deployments. The receiver is bounded to capacity 1 with `try_send`
    /// semantics: if the consumer is busy, additional notifications are
    /// coalesced into the pending one.
    pub(crate) fn start_block_notifier(&self) -> Result<Receiver<()>> {
        let (notif_tx, notif_rx) = bounded::<()>(1);
        let job: Job = Box::new(move |ctx| {
            async move {
                loop {
                    // Snapshot the current tip hash so we can ask the node
                    // to wake us when it changes.
                    let tip_hash = match snapshot_tip_hash(&ctx).await {
                        Ok(h) => h,
                        Err(e) => {
                            warn!("IPC notifier: tip snapshot failed: {e:#}");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };

                    // Block until the tip differs from `tip_hash`. Returns
                    // immediately if it already does.
                    let mut req = ctx.chain.wait_for_notifications_if_tip_changed_request();
                    match req.get().get_context() {
                        Ok(mut c) => c.set_thread(ctx.thread.clone()),
                        Err(e) => {
                            warn!("IPC notifier: failed to set thread: {e:#}");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                    req.get().set_old_tip(&tip_hash);
                    if let Err(e) = req.send().promise.await {
                        warn!("IPC notifier: waitForNotifications failed: {e:#}");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }

                    // Coalesce: if the receiver is already pending, drop this
                    // signal (the indexer will see all changes on its next
                    // sync iteration regardless).
                    let _ = notif_tx.try_send(());
                }
            }
            .boxed_local()
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow!("IPC worker thread is gone"))?;
        Ok(notif_rx)
    }
}

struct ChainNotificationHandler {
    mempool_tx: Sender<MempoolEvent>,
    block_tx: Sender<()>,
}

impl ChainNotificationHandler {
    fn send_mempool_event(&self, event: MempoolEvent) {
        if let Err(e) = self.mempool_tx.try_send(event) {
            warn!("IPC notification: dropping mempool event: {e}");
        }
    }

    fn send_block_event(&self) {
        let _ = self.block_tx.try_send(());
    }
}

impl chain_notifications::Server for ChainNotificationHandler {
    fn destroy(
        self: Rc<Self>,
        _: chain_notifications::DestroyParams,
        _: chain_notifications::DestroyResults,
    ) -> impl std::future::Future<Output = std::result::Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    async fn transaction_added_to_mempool(
        self: Rc<Self>,
        params: chain_notifications::TransactionAddedToMempoolParams,
        _: chain_notifications::TransactionAddedToMempoolResults,
    ) -> std::result::Result<(), capnp::Error> {
        let p = params.get()?;
        match deserialize(p.get_tx()?) {
            Ok(tx) => self.send_mempool_event(MempoolEvent::Added(tx)),
            Err(e) => warn!("IPC notification: invalid mempool transaction: {e}"),
        }
        Ok(())
    }

    async fn transaction_removed_from_mempool(
        self: Rc<Self>,
        params: chain_notifications::TransactionRemovedFromMempoolParams,
        _: chain_notifications::TransactionRemovedFromMempoolResults,
    ) -> std::result::Result<(), capnp::Error> {
        let p = params.get()?;
        match deserialize::<bitcoin::Transaction>(p.get_tx()?) {
            Ok(tx) => self.send_mempool_event(MempoolEvent::Removed(tx.compute_txid())),
            Err(e) => warn!("IPC notification: invalid removed mempool transaction: {e}"),
        }
        Ok(())
    }

    fn block_connected(
        self: Rc<Self>,
        _: chain_notifications::BlockConnectedParams,
        _: chain_notifications::BlockConnectedResults,
    ) -> impl std::future::Future<Output = std::result::Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn block_disconnected(
        self: Rc<Self>,
        _: chain_notifications::BlockDisconnectedParams,
        _: chain_notifications::BlockDisconnectedResults,
    ) -> impl std::future::Future<Output = std::result::Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn updated_block_tip(
        self: Rc<Self>,
        _: chain_notifications::UpdatedBlockTipParams,
        _: chain_notifications::UpdatedBlockTipResults,
    ) -> impl std::future::Future<Output = std::result::Result<(), capnp::Error>> + 'static {
        self.send_block_event();
        std::future::ready(Ok(()))
    }

    fn chain_state_flushed(
        self: Rc<Self>,
        _: chain_notifications::ChainStateFlushedParams,
        _: chain_notifications::ChainStateFlushedResults,
    ) -> impl std::future::Future<Output = std::result::Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }
}

async fn register_notifications(
    ctx: Ctx,
    mempool_tx: Sender<MempoolEvent>,
    block_tx: Sender<()>,
    ready_tx: oneshot::Sender<Result<()>>,
) {
    let setup = async {
        let handler = Rc::new(ChainNotificationHandler {
            mempool_tx,
            block_tx,
        });
        let notifications: chain_notifications::Client = capnp_rpc::new_client_from_rc(handler);

        let mut req = ctx.chain.handle_notifications_request();
        req.get().get_context()?.set_thread(ctx.thread.clone());
        req.get().set_notifications(notifications.clone());
        let resp = req.send().promise.await?;
        let _handler = resp.get()?.get_result()?;

        let mut req = ctx.chain.request_mempool_transactions_request();
        req.get().get_context()?.set_thread(ctx.thread.clone());
        req.get().set_notifications(notifications);
        req.send().promise.await?;

        Ok::<_, anyhow::Error>(_handler)
    }
    .await;

    match setup {
        Ok(_handler) => {
            let _ = ready_tx.send(Ok(()));
            futures::future::pending::<()>().await;
        }
        Err(e) => {
            let _ = ready_tx.send(Err(e));
        }
    }
}

/// Helper for the notifier loop: read the current tip hash via
/// `getHeight` + `getBlockHash`. Returns the all-zeros hash when the node
/// reports no tip yet (genesis only), which still works for
/// `waitForNotificationsIfTipChanged` (it'll wake us as soon as a real
/// tip exists).
async fn snapshot_tip_hash(ctx: &Ctx) -> Result<[u8; 32]> {
    let mut req = ctx.chain.get_height_request();
    req.get().get_context()?.set_thread(ctx.thread.clone());
    let resp = req.send().promise.await?;
    let r = resp.get()?;
    if !r.get_has_result() {
        return Ok([0u8; 32]);
    }
    let height = r.get_result();
    let mut req = ctx.chain.get_block_hash_request();
    req.get().get_context()?.set_thread(ctx.thread.clone());
    req.get().set_height(height);
    let resp = req.send().promise.await?;
    let bytes = resp.get()?.get_result()?;
    if bytes.len() != 32 {
        bail!("unexpected block hash length {}", bytes.len());
    }
    let mut buf = [0u8; 32];
    buf.copy_from_slice(bytes);
    Ok(buf)
}

/// Decode a serialized Bitcoin Core `CFeeRate` blob into satoshis per
/// kilo-vbyte (matching `CFeeRate::GetFeePerK()`). Wire format is
/// `FeeFrac { int64_t fee; int32_t size; }` (12 bytes LE), per
/// `SERIALIZE_METHODS(CFeeRate, ...)`. Returns `Ok(None)` for the empty
/// `CFeeRate{}` case (`size == 0`), which Bitcoin Core uses to mean "no
/// estimate available".
fn decode_fee_rate_kvb(blob: &[u8]) -> Result<Option<i64>> {
    if blob.len() != 12 {
        bail!(
            "expected serialized CFeeRate to be 12 bytes (int64 fee + int32 size), got {}",
            blob.len()
        );
    }
    let fee = i64::from_le_bytes(blob[0..8].try_into().expect("len 8"));
    let size = i32::from_le_bytes(blob[8..12].try_into().expect("len 4"));
    if size == 0 {
        return Ok(None);
    }
    Ok(Some(fee.saturating_mul(1000) / size as i64))
}

/// Serialized `Coin{CTxOut{0, empty script}, height=0, coinbase=false}`.
/// `findCoins` treats request coin values as placeholders, but the map
/// decoder still needs each value to deserialize as a valid `Coin`.
const DUMMY_COIN: &[u8] = &[0x00, 0x00, 0x06];

fn serialize_outpoint(outpoint: &OutPoint) -> [u8; 36] {
    let mut buf = [0u8; 36];
    buf[..32].copy_from_slice(outpoint.txid.as_byte_array());
    buf[32..].copy_from_slice(&outpoint.vout.to_le_bytes());
    buf
}

fn decode_coin(blob: &[u8]) -> Result<IpcCoin> {
    let mut pos = 0;
    let code = read_core_varint(blob, &mut pos)?;
    let compressed_amount = read_core_varint(blob, &mut pos)?;
    let script_size = read_core_varint(blob, &mut pos)?;
    let script_bytes = match script_size {
        0 | 1 => 20,
        2..=5 => 32,
        n => n
            .checked_sub(6)
            .ok_or_else(|| anyhow!("invalid compressed script size code {n}"))?,
    } as usize;
    if blob.len().saturating_sub(pos) != script_bytes {
        bail!(
            "serialized Coin script has {} bytes, expected {}",
            blob.len().saturating_sub(pos),
            script_bytes
        );
    }
    Ok(IpcCoin {
        value: Amount::from_sat(decompress_amount(compressed_amount)),
        height: (code >> 1)
            .try_into()
            .context("serialized Coin height overflow")?,
    })
}

/// Bitcoin Core VARINT encoding (base-128 with subtract-one continuation).
fn read_core_varint(blob: &[u8], pos: &mut usize) -> Result<u64> {
    let mut n = 0u64;
    loop {
        let Some(&ch) = blob.get(*pos) else {
            bail!("unexpected end of Bitcoin Core VARINT");
        };
        *pos += 1;
        n = n
            .checked_shl(7)
            .and_then(|n| n.checked_add((ch & 0x7f) as u64))
            .ok_or_else(|| anyhow!("Bitcoin Core VARINT overflow"))?;
        if ch & 0x80 == 0 {
            return Ok(n);
        }
        n = n
            .checked_add(1)
            .ok_or_else(|| anyhow!("Bitcoin Core VARINT overflow"))?;
    }
}

/// Inverse of Bitcoin Core's `CompressAmount`.
fn decompress_amount(mut x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    x -= 1;
    let e = x % 10;
    x /= 10;
    let mut n = if e < 9 {
        let d = (x % 9) + 1;
        x /= 9;
        x * 10 + d
    } else {
        x + 1
    };
    for _ in 0..e {
        n *= 10;
    }
    n
}

/// Bitcoin Core CompactSize encoding (src/serialize.h).
fn write_compact_size(buf: &mut Vec<u8>, n: u64) {
    if n < 253 {
        buf.push(n as u8);
    } else if n <= u16::MAX as u64 {
        buf.push(253);
        buf.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= u32::MAX as u64 {
        buf.push(254);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
    } else {
        buf.push(255);
        buf.extend_from_slice(&n.to_le_bytes());
    }
}

/// Worker thread entry point. Owns the tokio runtime, the capnp RPC system,
/// and the dispatch loop. Never returns under normal operation.
fn worker(
    socket: PathBuf,
    mut rx: mpsc::UnboundedReceiver<Job>,
    ready: std::sync::mpsc::Sender<Result<()>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready.send(Err(anyhow!("tokio runtime build: {e}")));
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let ctx = match bootstrap(&socket).await {
            Ok(ctx) => ctx,
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };
        let _ = ready.send(Ok(()));
        while let Some(job) = rx.recv().await {
            // Spawn each job onto the LocalSet so long-running jobs (e.g. the
            // block-tip notifier loop) do not block other in-flight calls.
            tokio::task::spawn_local(job(ctx.clone()));
        }
    }));
}

async fn bootstrap(socket: &Path) -> Result<Ctx> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting to bitcoin-node IPC socket {}", socket.display()))?;
    let (reader, writer) = stream.into_split();
    let buf_reader = BufReader::new(reader.compat());
    let buf_writer = futures::io::BufWriter::new(writer.compat_write());
    let network = VatNetwork::new(buf_reader, buf_writer, Side::Client, Default::default());
    let mut rpc_system = RpcSystem::new(Box::new(network), None);
    let init: init::Client = rpc_system.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc_system);

    // Construct + obtain a thread handle.
    let resp = init
        .construct_request()
        .send()
        .promise
        .await
        .context("Init.construct failed")?;
    let thread_map: thread_map::Client = resp
        .get()
        .context("Init.construct response")?
        .get_thread_map()
        .context("get_thread_map")?;
    let resp = thread_map
        .make_thread_request()
        .send()
        .promise
        .await
        .context("ThreadMap.makeThread failed")?;
    let thread: proxy_thread::Client = resp
        .get()
        .context("makeThread response")?
        .get_result()
        .context("makeThread result")?;

    // Make a Chain client.
    let mut req = init.make_chain_request();
    req.get()
        .get_context()
        .context("makeChain context")?
        .set_thread(thread.clone());
    let resp = req.send().promise.await.context("Init.makeChain failed")?;
    let chain: chain::Client = resp
        .get()
        .context("makeChain response")?
        .get_result()
        .context("makeChain result")?;

    Ok(Ctx { chain, thread })
}

/// Decode a serialized block. Helper that keeps callers free of the
/// `bitcoin::consensus` import.
pub(crate) fn decode_block(buf: &[u8]) -> Result<bitcoin::Block> {
    let mut cursor = std::io::Cursor::new(buf);
    bitcoin::Block::consensus_decode(&mut cursor)
        .map_err(|e| anyhow!("failed to decode block: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{decompress_amount, read_core_varint};

    #[test]
    fn decodes_core_varint_examples() {
        for (bytes, expected) in [
            (&[0x00][..], 0),
            (&[0x7f][..], 0x7f),
            (&[0x80, 0x00][..], 0x80),
            (&[0xa3, 0x34][..], 0x1234),
            (&[0x82, 0xfe, 0x7f][..], 0xffff),
        ] {
            let mut pos = 0;
            assert_eq!(read_core_varint(bytes, &mut pos).unwrap(), expected);
            assert_eq!(pos, bytes.len());
        }
    }

    #[test]
    fn decompresses_bitcoin_amounts() {
        for (compressed, sats) in [
            (0, 0),
            (1, 1),
            (7, 1000000),
            (9, 100000000),
            (50, 5000000000),
        ] {
            assert_eq!(decompress_amount(compressed), sats);
        }
    }
}
