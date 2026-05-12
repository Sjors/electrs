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
