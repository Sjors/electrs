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

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::thread as stdthread;

use anyhow::{anyhow, bail, Context, Result};
use bitcoin::blockdata::block::Header as BlockHeader;
use bitcoin::{consensus::Decodable, hashes::Hash, BlockHash};
use bitcoin_capnp_types::{
    chain_capnp::chain,
    init_capnp::init,
    proxy_capnp::{thread as proxy_thread, thread_map},
};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty::VatNetwork, RpcSystem};
use futures::{future::LocalBoxFuture, io::BufReader, FutureExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::chain::{Chain, NewHeader};
use crate::types::SerBlock;

/// Boxed (non-Send) future returned by a job. The closure itself must be
/// `Send` so it can be shipped from caller threads to the IPC worker, but
/// once on the worker it runs on a single-threaded LocalSet, so the future
/// it produces does not need to be `Send`.
type Job = Box<dyn for<'a> FnOnce(&'a Ctx) -> LocalBoxFuture<'a, ()> + Send + 'static>;

/// IPC context held by the worker thread: the chain client and the proxy
/// thread handle that every call must reference.
struct Ctx {
    chain: chain::Client,
    thread: proxy_thread::Client,
}

/// Sync handle to the IPC worker thread. Cloneable; clones share the same
/// connection.
#[derive(Clone)]
pub(crate) struct IpcChain {
    tx: mpsc::UnboundedSender<Job>,
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
        Ok(Self { tx })
    }

    /// Submit a job to the worker and block on its result.
    fn call<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a Ctx) -> LocalBoxFuture<'a, Result<R>> + Send + 'static,
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
    pub(crate) fn get_block(&self, hash: BlockHash) -> Result<Option<SerBlock>> {
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
    /// `BlockHeader`; the chain interface does not expose a header-only fetch,
    /// so the full block payload is downloaded and the body is discarded.
    /// The returned headers are immediately re-fetched in full by the
    /// subsequent `for_blocks` indexing pass — acceptable for an experimental
    /// backend over a local unix socket, but worth noting.
    ///
    /// Caps the response at 2000 headers per call to mirror the P2P
    /// `getheaders` semantics, so the caller can drive multiple iterations
    /// during initial sync.
    pub(crate) fn get_new_headers(&self, chain: &Chain) -> Result<Vec<NewHeader>> {
        const MAX_HEADERS: usize = 2_000;

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
                .get_block(hash)?
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
        }
        Ok(headers)
    }
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
            let fut: Pin<Box<dyn futures::Future<Output = ()>>> = job(&ctx);
            fut.await;
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
