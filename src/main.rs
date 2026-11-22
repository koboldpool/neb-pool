//! neb-pool: a NEB mining pool server (spec/pool.md, protocol neb-pool/1). Takes jobs from its node's work service,
//! checks every share with the node's own code (neb-pow) and sends blocks and chain shares back to the node.
//! Accepted shares go to <data>/shares.jsonl, blocks to blocks.jsonl, stats.json every 10 s; payouts are payout.py.

mod common;

use common::*;
use neb_block::Target;
use neb_chain::ShareRecord;
use neb_node::p2p;
use neb_node::params::Network;
use neb_pow::shares::{Model, ShareBuffers, SharedDataset};
use neb_work::frames::{self, FrameOut, Hello, Job, MSG_JOB, MSG_MODEL, MSG_RESULT, ResultMsg, Submit};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const MAX_LINE: usize = 262_144;
const KEEP_JOBS: usize = 32;
/// Pool target = share target * 2^k. Vardiff keeps k in this range.
const K_MAX: u32 = 48;
/// Vardiff aims at about one pool share every 10 s: it retunes after VARDIFF_WINDOW.
const VARDIFF_WINDOW: Duration = Duration::from_secs(60);
const RATE_LOW: usize = 3; // fewer per window: easier
const RATE_HIGH: usize = 12; // more per window: harder
const MAX_SUBMITS_PER_SEC: usize = 25;
const MAX_BAD_IN_A_ROW: u32 = 30;
const HASHRATE_WINDOW: Duration = Duration::from_secs(600);

struct Config {
    network: Network,
    listen: String,
    /// Every login on this port mines solo (nebx-miner has no password option to choose it).
    listen_solo: String,
    node_work: String,
    node_p2p: String,
    dataset: PathBuf,
    data: PathBuf,
    start_k: u32,
    name: String,
    /// 32-byte Ed25519 seed: with it, every job to a miner that sent a lock challenge is signed (pool lock).
    lock_key: PathBuf,
}

/// The pool lock key (--lock-key), set once at start.
static LOCK_KEY: OnceLock<SigningKey> = OnceLock::new();

fn lock_key(path: &std::path::Path) -> Result<SigningKey, String> {
    let b = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let seed: [u8; 32] = b.as_slice().try_into().map_err(|_| format!("{}: not 32 bytes", path.display()))?;
    Ok(SigningKey::from_bytes(&seed))
}

/// What the pool signs for a locked miner: the miner's challenge and every job field that fixes the header.
fn lock_message(challenge: &[u8; 32], job_id: &str, n: &Job, weights_hash: &[u8; 32]) -> Vec<u8> {
    let mut m = b"kobold-pool-lock/1/neb".to_vec();
    m.extend_from_slice(challenge);
    for v in [n.version, n.time_start, n.time_window, n.bits, n.tx_count] {
        m.extend_from_slice(&v.to_le_bytes());
    }
    m.extend_from_slice(&n.parent);
    for b in [&n.coinbase_prefix, &n.coinbase_suffix] {
        m.extend_from_slice(&(b.len() as u32).to_le_bytes());
        m.extend_from_slice(b);
    }
    m.extend_from_slice(&(n.branch.len() as u32).to_le_bytes());
    for b in &n.branch {
        m.extend_from_slice(b);
    }
    m.extend_from_slice(&n.shares_root);
    m.extend_from_slice(weights_hash);
    m.extend_from_slice(job_id.as_bytes());
    m
}

fn config() -> Config {
    let mut c = Config {
        listen: "0.0.0.0:3460".into(),
        listen_solo: "0.0.0.0:3470".into(),
        network: Network::Mainnet,
        node_work: String::new(), // default: the network's work port on 127.0.0.1
        node_p2p: String::new(),  // default: the network's peer port on 127.0.0.1
        dataset: "neb-datasets.bin".into(),
        data: "pool-data".into(),
        start_k: 10,
        lock_key: PathBuf::new(),
        name: "neb-pool".into(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--network" => {
                c.network = match v.as_str() {
                    "mainnet" => Network::Mainnet,
                    "testnet" => Network::Testnet,
                    other => panic!("--network mainnet|testnet, not {other}"),
                }
            }
            "--listen" => c.listen = v,
            "--listen-solo" => c.listen_solo = v,
            "--node-work" => c.node_work = v,
            "--node-p2p" => c.node_p2p = v,
            "--dataset" => c.dataset = v.into(),
            "--data" => c.data = v.into(),
            "--start-k" => c.start_k = v.parse().expect("--start-k N"),
            "--name" => c.name = v,
            "--lock-key" => c.lock_key = v.into(),
            "--lock-pubkey" => {
                let key = lock_key(std::path::Path::new(&v)).expect("--lock-pubkey <key file>");
                println!("{}", hex(key.verifying_key().as_bytes()));
                std::process::exit(0);
            }
            "--help" | "-h" => {
                println!(
                    "neb-pool [--network mainnet|testnet] [--listen 0.0.0.0:3460] [--listen-solo 0.0.0.0:3470] [--node-work 127.0.0.1:3441] [--node-p2p 127.0.0.1:3440] \
                     [--dataset neb-datasets.bin] [--data pool-data] [--start-k 10] [--name neb-pool] [--lock-key <file>]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown option {other}"),
        }
        i += 2;
    }
    c
}

/// One job of the pool: a node job with the model it is evaluated on.
struct PoolJob {
    id: String,
    node: Job,
    slot: u8,
    model: Arc<Model>,
    weights_hash: [u8; 32],
    weights_hex: Arc<String>,
    clean: bool,
    created: Instant,
}

struct BlockCand {
    height: u32,
    id: [u8; 32],
    finder: String,
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    rejected: AtomicU64,
    chain_shares: AtomicU64,
    blocks_submitted: AtomicU64,
    blocks_accepted: AtomicU64,
}

struct Shared {
    cfg: Config,
    datasets: Vec<SharedDataset>,
    dataset_hash: [u8; 32],
    chain_id: [u8; 32],
    /// From the network's parameters (neb-node 1.0.0 NetworkParams).
    magic: [u8; 4],
    protocol_version: u32,
    address_prefix: &'static str,
    jobs: Mutex<VecDeque<Arc<PoolJob>>>,
    /// Headers already submitted, per parent (duplicates are found on the header, spec/pool.md 7.4).
    seen: Mutex<HashMap<[u8; 32], HashSet<[u8; 80]>>>,
    sessions: Mutex<Vec<Arc<Session>>>,
    node_out: Mutex<Option<TcpStream>>,
    p2p_tx: Mutex<mpsc::Sender<([u8; 32], ShareRecord)>>,
    pending_blocks: Mutex<VecDeque<BlockCand>>,
    /// Recent accepted pool shares for the hash rate: (time, address, worker, work, solo).
    recent: Mutex<VecDeque<(Instant, String, String, f64, bool)>>,
    next_session: AtomicU64,
    next_job: AtomicU64,
    next_en1: AtomicU32,
    node_up: AtomicBool,
    p2p_up: AtomicBool,
    /// Fees in basis points (100 = 1 %), from <data>/pool-config.json, re-read every 10 s.
    /// Each found block records the fee in force at that moment; payouts use that value.
    fee_bp: AtomicU32,
    solo_fee_bp: AtomicU32,
    /// Pool-share work since the pool's last block: effort = round work / block work.
    round_work: Mutex<f64>,
    counters: Counters,
    files: Mutex<(File, File, File)>, // shares.jsonl, blocks.jsonl, pool.log
    started: u64,
}

impl Shared {
    fn log(&self, msg: &str) {
        let line = format!("{} {msg}\n", now());
        print!("{line}");
        if let Ok(mut f) = self.files.lock() {
            let _ = f.2.write_all(line.as_bytes());
        }
    }

    fn record(&self, which: usize, v: Value) {
        if let Ok(mut f) = self.files.lock() {
            let file = if which == 0 { &mut f.0 } else { &mut f.1 };
            let _ = writeln!(file, "{v}");
        }
    }

    fn latest_job(&self) -> Option<Arc<PoolJob>> {
        self.jobs.lock().unwrap().back().cloned()
    }

    fn job(&self, id: &str) -> Option<Arc<PoolJob>> {
        self.jobs.lock().unwrap().iter().rev().find(|j| j.id == id).cloned()
    }

    /// Parents a chain share may still be relayed for: those of the newest jobs.
    fn live_parents(&self) -> Vec<[u8; 32]> {
        let jobs = self.jobs.lock().unwrap();
        let mut out: Vec<[u8; 32]> = Vec::new();
        for j in jobs.iter().rev() {
            if !out.contains(&j.node.parent) {
                out.push(j.node.parent);
                if out.len() == 2 {
                    break;
                }
            }
        }
        out
    }
}

struct SessState {
    subscribed: bool,
    en1: [u8; 4],
    en2_size: usize,
    logins: HashSet<String>,
    /// Logins that mine in solo mode (authorized with a password containing "solo").
    solo: HashSet<String>,
    k: u32,
    k_prev: u32,
    k_changed: Instant,
    sent_models: VecDeque<[u8; 32]>,
    sent_target: Option<[u8; 32]>,
    window_start: Instant,
    window_count: usize,
    second: (Instant, usize),
    bad_in_a_row: u32,
    /// The miner's lock challenge from mining.subscribe: its jobs carry the pool's signature over it.
    lock: Option<[u8; 32]>,
}

struct Session {
    id: u64,
    peer: String,
    /// Connected on the solo port: every login of the session mines solo.
    solo_port: bool,
    out: Mutex<TcpStream>,
    st: Mutex<SessState>,
    alive: AtomicBool,
}

impl Session {
    fn send(&self, v: &Value) {
        let mut line = v.to_string();
        line.push('\n');
        let ok = self.out.lock().map(|mut s| s.write_all(line.as_bytes()).is_ok()).unwrap_or(false);
        if !ok {
            self.alive.store(false, Ordering::Relaxed);
        }
    }

    fn notify(&self, method: &str, params: Value) {
        self.send(&json!({"id": null, "method": method, "params": params}));
    }
}

fn shifted(base: &[u8; 32], k: u32) -> [u8; 32] {
    Target(*base).shifted_left(k).map(|t| t.0).unwrap_or([0xff; 32])
}

/// Sends what a session needs before a job: its target, the model if new, then the job.
fn push_job(sess: &Session, job: &PoolJob, clean: bool) {
    let (target, need_model, lock) = {
        let mut st = sess.st.lock().unwrap();
        if st.logins.is_empty() {
            return;
        }
        let target = shifted(&job.node.share_target, st.k);
        let target = if st.sent_target == Some(target) { None } else { Some(target) };
        if let Some(t) = target {
            st.sent_target = Some(t);
        }
        let need_model = !st.sent_models.contains(&job.weights_hash);
        if need_model {
            st.sent_models.push_back(job.weights_hash);
            if st.sent_models.len() > 8 {
                st.sent_models.pop_front();
            }
        }
        (target, need_model, st.lock)
    };
    if let Some(t) = target {
        sess.notify("mining.set_target", json!([hex(&t)]));
    }
    if need_model {
        sess.notify("mining.set_model", json!([job.slot, hex(&job.weights_hash), job.weights_hex.as_str()]));
    }
    let n = &job.node;
    let mut msg = json!([{
            "job_id": job.id, "clean": clean, "version": n.version, "parent": hex(&n.parent),
            "time_start": n.time_start, "time_window": n.time_window, "bits": n.bits,
            "target": hex(&n.target), "share_target": hex(&n.share_target),
            "coinbase_prefix": hex(&n.coinbase_prefix), "coinbase_suffix": hex(&n.coinbase_suffix),
            "branch": n.branch.iter().map(|b| hex(b)).collect::<Vec<_>>(), "tx_count": n.tx_count,
            "shares_root": hex(&n.shares_root), "height": n.height, "slot": job.slot,
            "weights_hash": hex(&job.weights_hash), "expiry": n.expiry
        }]);
    if let (Some(key), Some(ch)) = (LOCK_KEY.get(), lock) {
        msg[0]["sig"] = json!(hex(&key.sign(&lock_message(&ch, &job.id.to_string(), n, &job.weights_hash)).to_bytes()));
    }
    sess.notify("mining.notify", msg);
}

// ------------------------------------------------------------------------------------- node link

fn node_loop(sh: Arc<Shared>) {
    loop {
        match TcpStream::connect(&sh.cfg.node_work) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                if let Ok(w) = stream.try_clone() {
                    let hello = Hello { version: 1, name: format!("{}/{}", sh.cfg.name, env!("CARGO_PKG_VERSION")) };
                    let mut w2 = w;
                    // Hello::encode is the payload only; the other frames' encode() include the header.
                    let _ = w2.write_all(&frames::frame(frames::MSG_HELLO, &hello.encode()));
                    *sh.node_out.lock().unwrap() = Some(w2);
                }
                sh.node_up.store(true, Ordering::Relaxed);
                sh.log(&format!("connected to the node work service {}", sh.cfg.node_work));
                read_node(&sh, stream);
                sh.node_up.store(false, Ordering::Relaxed);
                *sh.node_out.lock().unwrap() = None;
                sh.log("lost the node work service; retrying in 3 s");
            }
            Err(e) => sh.log(&format!("node work service {} not reachable: {e}", sh.cfg.node_work)),
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn read_node(sh: &Arc<Shared>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = vec![0u8; 65536];
    let mut model: Option<(u8, Arc<Model>, [u8; 32], Arc<String>)> = None;
    let mut last_parent: Option<[u8; 32]> = None;
    loop {
        match stream.read(&mut tmp) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
        loop {
            match frames::parse_frame(&buf) {
                Ok(FrameOut::Need(_)) => break,
                Ok(FrameOut::Message { msg_type, payload, consumed }) => {
                    match msg_type {
                        MSG_MODEL => match frames::decode_model(payload) {
                            Ok((slot, m)) if payload.len() > 33 => {
                                let wh: [u8; 32] = payload[1..33].try_into().unwrap();
                                if m.weights_hash() != wh {
                                    sh.log("node sent a model whose weights hash does not match; ignored");
                                } else {
                                    model = Some((slot, Arc::new(m), wh, Arc::new(hex(&payload[33..]))));
                                }
                            }
                            _ => {
                                sh.log("node sent a model frame that does not decode");
                                return;
                            }
                        },
                        MSG_JOB => {
                            let Ok(job) = Job::decode(payload) else {
                                sh.log("node sent a job that does not decode");
                                return;
                            };
                            let Some((slot, m, wh, wx)) = model.clone() else { continue };
                            if job.extranonce_size < 8 {
                                sh.log("node job extranonce below 8 bytes; not used");
                            } else {
                                let clean = last_parent != Some(job.parent);
                                last_parent = Some(job.parent);
                                install_job(sh, job, slot, m, wh, wx, clean);
                            }
                        }
                        MSG_RESULT => {
                            if let Ok(r) = ResultMsg::decode(payload) {
                                block_result(sh, r);
                            }
                        }
                        _ => {}
                    }
                    buf.drain(..consumed);
                }
                Err(e) => {
                    sh.log(&format!("bad frame from the node: {e:?}"));
                    return;
                }
            }
        }
    }
}

fn install_job(sh: &Arc<Shared>, node: Job, slot: u8, model: Arc<Model>, wh: [u8; 32], wx: Arc<String>, clean: bool) {
    if (slot as usize) >= sh.datasets.len() || sh.datasets[slot as usize].arch() != model.arch() {
        sh.log(&format!("job on slot {slot} does not fit the dataset file; not used (wrong dataset?)"));
        return;
    }
    let id = format!("{:x}", sh.next_job.fetch_add(1, Ordering::Relaxed));
    let job = Arc::new(PoolJob { id, node, slot, model, weights_hash: wh, weights_hex: wx, clean, created: Instant::now() });
    {
        let mut jobs = sh.jobs.lock().unwrap();
        jobs.push_back(job.clone());
        while jobs.len() > KEEP_JOBS {
            jobs.pop_front();
        }
        let parents: HashSet<[u8; 32]> = jobs.iter().map(|j| j.node.parent).collect();
        sh.seen.lock().unwrap().retain(|p, _| parents.contains(p));
    }
    if clean {
        sh.log(&format!("new block template: height {} slot {} bits {:08x}", job.node.height, job.slot, job.node.bits));
    }
    let sessions: Vec<Arc<Session>> = sh.sessions.lock().unwrap().clone();
    for s in sessions {
        if s.alive.load(Ordering::Relaxed) {
            push_job(&s, &job, job.clean);
        }
    }
}

fn block_result(sh: &Arc<Shared>, r: ResultMsg) {
    let cand = sh.pending_blocks.lock().unwrap().pop_front();
    let Some(c) = cand else { return };
    if r.accepted {
        sh.counters.blocks_accepted.fetch_add(1, Ordering::Relaxed);
    }
    sh.log(&format!(
        "node {} block {} at height {} (code {})",
        if r.accepted { "ACCEPTED" } else { "refused" },
        hex(&c.id),
        c.height,
        r.code
    ));
    sh.record(1, json!({"ts": now(), "event": "result", "height": c.height, "id": hex(&c.id), "finder": c.finder,
                       "accepted": r.accepted, "code": r.code}));
}

// ------------------------------------------------------------------------------------- peer link

/// Holds a peer session with the node to relay chain shares (spec/pool.md 7.6). Answers pings,
/// ignores everything else. Only records whose parent is still live are sent: the node bans a
/// peer that sends an invalid record.
fn p2p_loop(sh: Arc<Shared>, rx: mpsc::Receiver<([u8; 32], ShareRecord)>) {
    let magic = sh.magic;
    loop {
        let stream = match TcpStream::connect(&sh.cfg.node_p2p) {
            Ok(s) => s,
            Err(e) => {
                sh.log(&format!("node peer port {} not reachable: {e}", sh.cfg.node_p2p));
                thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        let out = Arc::new(Mutex::new(stream.try_clone().expect("clone")));
        let mut v = Vec::new();
        v.extend_from_slice(&sh.protocol_version.to_le_bytes());
        v.extend_from_slice(&0u32.to_le_bytes());
        let nonce = u64::from_le_bytes(blake3::hash(format!("{}{:?}", now(), Instant::now()).as_bytes()).as_bytes()[..8].try_into().unwrap());
        v.extend_from_slice(&nonce.to_le_bytes());
        v.extend_from_slice(&sh.chain_id);
        v.extend_from_slice(&0u16.to_le_bytes());
        let ua = format!("{}/{}", sh.cfg.name, env!("CARGO_PKG_VERSION"));
        v.push(ua.len() as u8);
        v.extend_from_slice(ua.as_bytes());
        if out.lock().unwrap().write_all(&p2p::encode_message(magic, "version", &v).unwrap()).is_err() {
            thread::sleep(Duration::from_secs(5));
            continue;
        }
        let up = Arc::new(AtomicBool::new(true));
        {
            let (sh2, out2, up2) = (sh.clone(), out.clone(), up.clone());
            thread::spawn(move || p2p_reader(sh2, stream, out2, up2, magic));
        }
        sh.log(&format!("peer session with the node {} open", sh.cfg.node_p2p));
        while up.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok((parent, rec)) => {
                    let mut batch: HashMap<[u8; 32], Vec<ShareRecord>> = HashMap::new();
                    batch.entry(parent).or_default().push(rec);
                    while let Ok((p, r)) = rx.try_recv() {
                        batch.entry(p).or_default().push(r);
                    }
                    let live = sh.live_parents();
                    for (p, mut recs) in batch {
                        if !live.contains(&p) {
                            continue;
                        }
                        recs.sort_by_key(|r| r.to_bytes());
                        recs.dedup_by_key(|r| r.to_bytes());
                        let n = recs.len();
                        let frame = p2p::shares_frame(magic, &p, &recs).unwrap();
                        if out.lock().unwrap().write_all(&frame).is_err() {
                            up.store(false, Ordering::Relaxed);
                        } else {
                            sh.counters.chain_shares.fetch_add(n as u64, Ordering::Relaxed);
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
        sh.p2p_up.store(false, Ordering::Relaxed);
        sh.log("peer session with the node closed; reopening in 5 s");
        thread::sleep(Duration::from_secs(5));
    }
}

fn p2p_reader(sh: Arc<Shared>, mut stream: TcpStream, out: Arc<Mutex<TcpStream>>, up: Arc<AtomicBool>, magic: [u8; 4]) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(180)));
    let mut header = [0u8; 24];
    loop {
        if stream.read_exact(&mut header).is_err() || header[..4] != magic {
            break;
        }
        let end = header[4..16].iter().position(|b| *b == 0).unwrap_or(12);
        let cmd = String::from_utf8_lossy(&header[4..4 + end]).to_string();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        if len > p2p::MAX_PAYLOAD {
            break;
        }
        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).is_err() {
            break;
        }
        let reply = match cmd.as_str() {
            "version" => {
                sh.p2p_up.store(true, Ordering::Relaxed);
                Some(p2p::encode_message(magic, "verack", &[]).unwrap())
            }
            "ping" => Some(p2p::encode_message(magic, "pong", &payload).unwrap()),
            _ => None,
        };
        if let Some(r) = reply {
            if out.lock().unwrap().write_all(&r).is_err() {
                break;
            }
        }
    }
    let _ = out.lock().unwrap().shutdown(Shutdown::Both);
    up.store(false, Ordering::Relaxed);
}

// ------------------------------------------------------------------------------------- miners

fn err(id: &Value, code: u32, msg: &str) -> Value {
    json!({"id": id, "result": null, "error": [code, msg]})
}

fn ok(id: &Value, result: Value) -> Value {
    json!({"id": id, "result": result, "error": null})
}

fn valid_login(prefix: &str, login: &str) -> Option<(String, String)> {
    let (addr, worker) = match login.split_once('.') {
        Some((a, w)) => (a, w),
        None => (login, ""),
    };
    if worker.len() > 32 || !worker.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return None;
    }
    neb_wallet::address::decode_address(prefix, addr).ok()?;
    Some((addr.to_string(), if worker.is_empty() { "default".into() } else { worker.to_string() }))
}

fn serve_miner(sh: Arc<Shared>, stream: TcpStream, solo_port: bool) {
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(600)));
    let Ok(out) = stream.try_clone() else { return };
    let id = sh.next_session.fetch_add(1, Ordering::Relaxed);
    let en1 = sh.next_en1.fetch_add(1, Ordering::Relaxed).to_le_bytes();
    let sess = Arc::new(Session {
        id,
        peer: peer.clone(),
        solo_port,
        out: Mutex::new(out),
        alive: AtomicBool::new(true),
        st: Mutex::new(SessState {
            subscribed: false,
            en1,
            en2_size: 0,
            logins: HashSet::new(),
            solo: HashSet::new(),
            k: sh.cfg.start_k,
            k_prev: sh.cfg.start_k,
            k_changed: Instant::now(),
            sent_models: VecDeque::new(),
            sent_target: None,
            window_start: Instant::now(),
            window_count: 0,
            second: (Instant::now(), 0),
            bad_in_a_row: 0,
            lock: None,
        }),
    });
    sh.sessions.lock().unwrap().push(sess.clone());
    let mut reader = BufReader::new(stream);
    let mut buffers = ShareBuffers::new();
    let mut line = String::new();
    loop {
        line.clear();
        match (&mut reader).take(MAX_LINE as u64 + 1).read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(n) if n > MAX_LINE => break,
            Ok(_) => {}
        }
        if line.trim().is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(&line) else { break };
        let rid = req.get("id").cloned().unwrap_or(Value::Null);
        let p = req.get("params").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let reply = match method {
            "mining.subscribe" => subscribe(&sh, &sess, &rid, &p),
            "mining.authorize" => authorize(&sh, &sess, &rid, &p),
            "mining.submit" => submit(&sh, &sess, &rid, &p, &mut buffers),
            // Sent by some miners (nebx-miner --suggest-diff); vardiff sets the target, so it's accepted and ignored.
            "mining.suggest_difficulty" | "mining.suggest_target" | "mining.extranonce.subscribe" => ok(&rid, json!(true)),
            _ => err(&rid, 20, "unknown method"),
        };
        sess.send(&reply);
        if !sess.alive.load(Ordering::Relaxed) {
            break;
        }
        if method == "mining.authorize" && reply.get("result") == Some(&Value::Bool(true)) {
            let first = sess.st.lock().unwrap().logins.len() == 1;
            if first {
                if let Some(job) = sh.latest_job() {
                    push_job(&sess, &job, true);
                }
            }
        }
    }
    sess.alive.store(false, Ordering::Relaxed);
    sh.sessions.lock().unwrap().retain(|s| s.id != sess.id);
    let logins = sess.st.lock().unwrap().logins.iter().cloned().collect::<Vec<_>>().join(",");
    sh.log(&format!("session {} {} closed ({})", sess.id, sess.peer, logins));
}

fn subscribe(sh: &Shared, sess: &Session, rid: &Value, p: &[Value]) -> Value {
    let Some(job) = sh.latest_job() else { return err(rid, 20, "pool has no work from its node yet") };
    let mut st = sess.st.lock().unwrap();
    st.subscribed = true;
    st.lock = p.get(2).and_then(|o| o.get("lock")).and_then(|v| v.as_str()).and_then(unhex32);
    st.en2_size = job.node.extranonce_size as usize - 4;
    ok(
        rid,
        json!({"session": format!("{:x}", sess.id), "extranonce1": hex(&st.en1), "extranonce2_size": st.en2_size,
               "chain_id": hex(&sh.chain_id), "dataset_hash": hex(&sh.dataset_hash)}),
    )
}

fn authorize(sh: &Shared, sess: &Session, rid: &Value, p: &[Value]) -> Value {
    let mut st = sess.st.lock().unwrap();
    if !st.subscribed {
        return err(rid, 25, "not subscribed");
    }
    let login = p.first().and_then(|v| v.as_str()).unwrap_or("");
    if valid_login(sh.address_prefix, login).is_none() {
        return err(rid, 24, "login is not a valid neb1 address (with an optional .worker)");
    }
    if st.logins.len() >= 8 && !st.logins.contains(login) {
        return err(rid, 24, "too many logins in one session");
    }
    st.logins.insert(login.to_string());
    // Solo mode: the password (unused by the protocol, spec/pool.md 4) carries pool options.
    let solo = sess.solo_port
        || p.get(1).and_then(|v| v.as_str()).is_some_and(|pw| pw.to_ascii_lowercase().contains("solo"));
    if solo {
        st.solo.insert(login.to_string());
    } else {
        st.solo.remove(login);
    }
    drop(st);
    sh.log(&format!("session {} {} authorized {login}{}", sess.id, sess.peer, if solo { " (solo)" } else { "" }));
    ok(rid, json!(true))
}

fn submit(sh: &Shared, sess: &Session, rid: &Value, p: &[Value], buffers: &mut ShareBuffers) -> Value {
    let login = p.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
    let job_id = p.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let en2 = p.get(2).and_then(|v| v.as_str()).and_then(unhex);
    let time = p.get(3).and_then(|v| v.as_u64());
    let nonce = p.get(4).and_then(|v| v.as_u64());
    let (en1, en2_size, pool_k, solo) = {
        let mut st = sess.st.lock().unwrap();
        if !st.subscribed {
            return err(rid, 25, "not subscribed");
        }
        if !st.logins.contains(&login) {
            return err(rid, 24, "login not authorized");
        }
        // Rate limit: a few submits per second at most.
        let now_i = Instant::now();
        if now_i.duration_since(st.second.0) >= Duration::from_secs(1) {
            st.second = (now_i, 0);
        }
        st.second.1 += 1;
        if st.second.1 > MAX_SUBMITS_PER_SEC {
            sess.alive.store(false, Ordering::Relaxed);
            return err(rid, 20, "too many submits");
        }
        // Accept the previous, easier target for 30 s after vardiff made it harder.
        let k = if now_i.duration_since(st.k_changed) < Duration::from_secs(30) { st.k.max(st.k_prev) } else { st.k };
        (st.en1, st.en2_size, k, st.solo.contains(&login))
    };
    let Some(job) = sh.job(job_id).filter(|j| (now() as u32) <= j.node.expiry) else {
        return err(rid, 21, "job not found or past its expiry");
    };
    let Some(en2) = en2.filter(|e| e.len() == en2_size && en2_size + 4 == job.node.extranonce_size as usize) else {
        return err(rid, 27, "extranonce2 of the wrong length");
    };
    let (Some(time), Some(nonce)) = (time, nonce) else { return err(rid, 20, "bad params") };
    let (time, nonce) = (time as u32, nonce as u32);
    if time < job.node.time_start || time > job.node.time_start.saturating_add(job.node.time_window) {
        return err(rid, 26, "time outside the window of the job");
    }
    let mut extranonce = en1.to_vec();
    extranonce.extend_from_slice(&en2);
    let header = job.node.header(&extranonce, time, nonce).encode();
    {
        let mut seen = sh.seen.lock().unwrap();
        if !seen.entry(job.node.parent).or_default().insert(header) {
            return err(rid, 22, "duplicate attempt");
        }
    }
    let key = *blake3::hash(&header).as_bytes();
    let ds = &sh.datasets[job.slot as usize];
    let ticket = buffers.ticket(&job.model, ds.as_ref(), &key);
    let pool_target = shifted(&job.node.share_target, pool_k);
    if !Target(pool_target).admits(&ticket) {
        sh.counters.rejected.fetch_add(1, Ordering::Relaxed);
        let mut st = sess.st.lock().unwrap();
        st.bad_in_a_row += 1;
        if st.bad_in_a_row > MAX_BAD_IN_A_ROW {
            sess.alive.store(false, Ordering::Relaxed);
        }
        return err(rid, 23, "ticket above the pool target");
    }
    let chain_share = Target(job.node.share_target).admits(&ticket);
    let block = Target(job.node.target).admits(&ticket);
    let work = work_of(&pool_target);
    let (addr, worker) = valid_login(sh.address_prefix, &login).unwrap_or_default();
    sh.counters.accepted.fetch_add(1, Ordering::Relaxed);
    let mode = if solo { "solo" } else { "pplns" };
    sh.record(0, json!({"ts": now(), "addr": addr, "worker": worker, "work": work, "height": job.node.height,
                        "chain_share": chain_share, "block": block, "mode": mode}));
    sh.recent.lock().unwrap().push_back((Instant::now(), addr.clone(), worker.clone(), work, solo));
    let effort = {
        let mut rw = sh.round_work.lock().unwrap();
        *rw += work;
        let e = *rw / work_of(&job.node.target);
        if block {
            *rw = 0.0;
        }
        e
    };
    if chain_share {
        let rec = ShareRecord { body_root: job.node.body_root(&extranonce), time, nonce };
        let _ = sh.p2p_tx.lock().unwrap().send((job.node.parent, rec));
    }
    if block {
        let frame = Submit { id: job.node.id, extranonce: extranonce.clone(), time, nonce }.encode();
        let sent = sh.node_out.lock().unwrap().as_mut().map(|s| s.write_all(&frame).is_ok()).unwrap_or(false);
        let id = *blake3::hash(&header).as_bytes();
        sh.counters.blocks_submitted.fetch_add(1, Ordering::Relaxed);
        if sent {
            sh.pending_blocks.lock().unwrap().push_back(BlockCand { height: job.node.height, id, finder: login.clone() });
        }
        sh.log(&format!("BLOCK found by {login} at height {} id {} (sent to node: {sent})", job.node.height, hex(&id)));
        sh.record(1, json!({"ts": now(), "event": "found", "height": job.node.height, "id": hex(&id),
                            "parent": hex(&job.node.parent), "finder": login, "sent": sent,
                            "block_work": work_of(&job.node.target), "mode": mode, "effort": effort,
                            "fee_bp": if solo { sh.solo_fee_bp.load(Ordering::Relaxed) } else { sh.fee_bp.load(Ordering::Relaxed) }}));
    }
    vardiff(sh, sess, &job);
    ok(rid, json!(true))
}

fn vardiff(sh: &Shared, sess: &Session, job: &PoolJob) {
    let changed = {
        let mut st = sess.st.lock().unwrap();
        st.bad_in_a_row = 0;
        st.window_count += 1;
        let elapsed = st.window_start.elapsed();
        let mut changed = false;
        if st.window_count > RATE_HIGH * 2 || elapsed >= VARDIFF_WINDOW {
            let scaled = st.window_count as f64 * VARDIFF_WINDOW.as_secs_f64() / elapsed.as_secs_f64().max(1.0);
            let old = st.k;
            if scaled > RATE_HIGH as f64 && st.k > 0 {
                // Harder: drop k by log2 of how far above the aim the rate is.
                let steps = ((scaled / 6.0).log2().floor() as u32).clamp(1, st.k);
                st.k -= steps;
            } else if scaled < RATE_LOW as f64 && st.k < K_MAX {
                st.k += 1;
            }
            if st.k != old {
                st.k_prev = old;
                st.k_changed = Instant::now();
                changed = true;
            }
            st.window_start = Instant::now();
            st.window_count = 0;
        }
        changed
    };
    if changed {
        let _ = sh;
        push_job(sess, job, false);
    }
}

// ------------------------------------------------------------------------------------- stats

/// Reads the fees from <data>/pool-config.json: {"fee_percent": 0.0, "solo_fee_percent": 0.0}.
/// A missing file is created with 0 % for both. A broken file or out-of-range value keeps the
/// current fees and is logged, so a typo never changes the fee by accident.
fn load_fees(sh: &Shared) {
    let path = sh.cfg.data.join("pool-config.json");
    if !path.exists() {
        let _ = std::fs::write(&path, "{\n  \"fee_percent\": 0.0,\n  \"solo_fee_percent\": 0.0\n}\n");
    }
    let parsed = std::fs::read_to_string(&path).ok().and_then(|t| serde_json::from_str::<Value>(&t).ok());
    let Some(cfg) = parsed else {
        sh.log("pool-config.json does not parse; fees unchanged");
        return;
    };
    for (key, slot) in [("fee_percent", &sh.fee_bp), ("solo_fee_percent", &sh.solo_fee_bp)] {
        let Some(pct) = cfg.get(key).and_then(|v| v.as_f64()) else { continue };
        if !(0.0..=10.0).contains(&pct) {
            sh.log(&format!("pool-config.json: {key} {pct} outside 0..10 %; unchanged"));
            continue;
        }
        let bp = (pct * 100.0).round() as u32;
        let old = slot.swap(bp, Ordering::Relaxed);
        if old != bp {
            sh.log(&format!("{key} is now {:.2} % (was {:.2} %)", bp as f64 / 100.0, old as f64 / 100.0));
        }
    }
}

fn stats_loop(sh: Arc<Shared>) {
    loop {
        thread::sleep(Duration::from_secs(10));
        load_fees(&sh);
        let mut by_addr: HashMap<String, (f64, HashMap<String, f64>, bool)> = HashMap::new();
        let total = {
            let mut recent = sh.recent.lock().unwrap();
            while recent.front().is_some_and(|r| r.0.elapsed() > HASHRATE_WINDOW) {
                recent.pop_front();
            }
            let mut total = 0.0;
            for (_, a, w, work, solo) in recent.iter() {
                total += work;
                let e = by_addr.entry(a.clone()).or_default();
                e.0 += work;
                *e.1.entry(w.clone()).or_default() += work;
                e.2 |= *solo;
            }
            total
        };
        let secs = HASHRATE_WINDOW.as_secs_f64();
        let job = sh.latest_job();
        let miners: Vec<Value> = by_addr
            .iter()
            .map(|(a, (w, ws, solo))| {
                json!({"addr": a, "hashrate": w / secs, "solo": solo,
                       "workers": ws.iter().map(|(n, x)| json!({"name": n, "hashrate": x / secs})).collect::<Vec<_>>()})
            })
            .collect();
        let st = json!({
            "updated": now(), "started": sh.started, "name": sh.cfg.name,
            "node_online": sh.node_up.load(Ordering::Relaxed), "peer_online": sh.p2p_up.load(Ordering::Relaxed),
            "sessions": sh.sessions.lock().unwrap().len(),
            "hashrate": total / secs, "unit": "attempts/s",
            "fee_percent": sh.fee_bp.load(Ordering::Relaxed) as f64 / 100.0,
            "solo_fee_percent": sh.solo_fee_bp.load(Ordering::Relaxed) as f64 / 100.0,
            "height": job.as_ref().map(|j| j.node.height), "bits": job.as_ref().map(|j| format!("{:08x}", j.node.bits)),
            "network_work_per_block": job.as_ref().map(|j| work_of(&j.node.target)),
            "round_effort": job.as_ref().map(|j| *sh.round_work.lock().unwrap() / work_of(&j.node.target)),
            "accepted": sh.counters.accepted.load(Ordering::Relaxed),
            "rejected": sh.counters.rejected.load(Ordering::Relaxed),
            "chain_shares_relayed": sh.counters.chain_shares.load(Ordering::Relaxed),
            "blocks_submitted": sh.counters.blocks_submitted.load(Ordering::Relaxed),
            "blocks_accepted": sh.counters.blocks_accepted.load(Ordering::Relaxed),
            "miners": miners,
            "job_age_s": job.as_ref().map(|j| j.created.elapsed().as_secs()),
        });
        let tmp = sh.cfg.data.join("stats.json.tmp");
        if std::fs::write(&tmp, serde_json::to_vec_pretty(&st).unwrap()).is_ok() {
            let _ = std::fs::rename(&tmp, sh.cfg.data.join("stats.json"));
        }
    }
}

fn main() {
    let cfg = config();
    std::fs::create_dir_all(&cfg.data).expect("create the data directory");
    let bytes = std::fs::read(&cfg.dataset).unwrap_or_else(|e| panic!("read {}: {e}", cfg.dataset.display()));
    let dataset_hash = *blake3::hash(&bytes).as_bytes();
    drop(bytes);
    let mut cfg = cfg;
    let datasets = cfg.network.load_dataset(&cfg.dataset).unwrap_or_else(|e| panic!("dataset: {e}"));
    let np = cfg.network.params(datasets.clone());
    if cfg.node_work.is_empty() {
        cfg.node_work = format!("127.0.0.1:{}", np.work_port);
    }
    if cfg.node_p2p.is_empty() {
        cfg.node_p2p = format!("127.0.0.1:{}", np.p2p_port);
    }
    let open = |n: &str| OpenOptions::new().create(true).append(true).open(cfg.data.join(n)).expect("open log file");
    let files = (open("shares.jsonl"), open("blocks.jsonl"), open("pool.log"));
    let (tx, rx) = mpsc::channel();
    if !cfg.lock_key.as_os_str().is_empty() {
        let _ = LOCK_KEY.set(lock_key(&cfg.lock_key).expect("--lock-key"));
    }
    let sh = Arc::new(Shared {
        datasets,
        dataset_hash,
        chain_id: np.chain_id(),
        magic: np.magic,
        protocol_version: np.protocol_version,
        address_prefix: np.address_prefix,
        jobs: Mutex::new(VecDeque::new()),
        seen: Mutex::new(HashMap::new()),
        sessions: Mutex::new(Vec::new()),
        node_out: Mutex::new(None),
        p2p_tx: Mutex::new(tx),
        pending_blocks: Mutex::new(VecDeque::new()),
        recent: Mutex::new(VecDeque::new()),
        next_session: AtomicU64::new(1),
        next_job: AtomicU64::new(1),
        next_en1: AtomicU32::new(u32::from_le_bytes(dataset_hash[..4].try_into().unwrap()) ^ now() as u32),
        node_up: AtomicBool::new(false),
        p2p_up: AtomicBool::new(false),
        fee_bp: AtomicU32::new(0),
        solo_fee_bp: AtomicU32::new(0),
        round_work: Mutex::new(0.0),
        counters: Counters::default(),
        files: Mutex::new(files),
        started: now(),
        cfg,
    });
    load_fees(&sh);
    sh.log(&format!(
        "{} {} starting: chain {} dataset {}",
        sh.cfg.name,
        env!("CARGO_PKG_VERSION"),
        hex(&sh.chain_id),
        hex(&sh.dataset_hash)
    ));
    {
        let s = sh.clone();
        thread::spawn(move || node_loop(s));
    }
    {
        let s = sh.clone();
        thread::spawn(move || p2p_loop(s, rx));
    }
    {
        let s = sh.clone();
        thread::spawn(move || stats_loop(s));
    }
    let accept = |addr: String, solo: bool, sh: Arc<Shared>| {
        let listener = TcpListener::bind(&addr).unwrap_or_else(|e| panic!("listen {addr}: {e}"));
        sh.log(&format!("stratum (neb-pool/1) listening on {addr} ({})", if solo { "solo" } else { "PPLNS" }));
        for conn in listener.incoming() {
            match conn {
                Ok(s) => {
                    let sh2 = sh.clone();
                    thread::spawn(move || serve_miner(sh2, s, solo));
                }
                Err(e) => sh.log(&format!("accept failed: {e}")),
            }
        }
    };
    {
        let (sh2, addr) = (sh.clone(), sh.cfg.listen_solo.clone());
        thread::spawn(move || accept(addr, true, sh2));
    }
    accept(sh.cfg.listen.clone(), false, sh.clone());
}
