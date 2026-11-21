//! kobold-neb-proxy: lets NEB's official miners mine on a neb-pool/1 pool by posing as a node's work service.
//! The pool's extranonce1 and all but the last byte of extranonce2 are folded into the coinbase prefix, so the miner's
//! one zero extranonce byte completes the pool header and each share maps to one mining.submit. The same pool job goes
//! out again with the next extranonce2 every --refresh seconds; a Model frame only when the model changes.

mod common;

use common::*;
use neb_pow::shares::Model;
use neb_work::frames::{self, FrameOut, Job, MSG_HELLO, MSG_SHARE, MSG_SUBMIT, ResultMsg, ShareMsg, Submit};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Miner jobs remembered for shares that arrive after a newer job was sent.
const KEEP_JOBS: usize = 512;

fn arg(name: &str, default: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1).cloned()).unwrap_or_else(|| default.to_string())
}

/// A job of the pool, as received in `mining.notify`.
#[derive(Clone)]
struct PoolJob {
    job_id: String,
    slot: u8,
    weights_hash: [u8; 32],
    clean: bool,
    template: Job,
}

/// What a miner job id stands for at the pool.
#[derive(Clone)]
struct JobRef {
    pool_job_id: String,
    en2: Vec<u8>,
    time: u32,
}

/// A request sent to the pool, waiting for its answer.
struct Pending {
    /// (miner connection, miner job id, nonce) when a Submit (solution) frame waits for this answer.
    solution: Option<u64>,
    key: (u64, u32),
}

struct Miner {
    id: u64,
    writer: TcpStream,
    /// Distinguishes the extranonce2 of this connection from other miners on the same proxy.
    tag: u8,
    /// Weights hash of the last Model frame sent to this miner. NEB's miner reloads the model (and rebuilds the
    /// table of the table kernel, seconds of CPU work) on every Model frame, so one is sent only when it changes.
    sent_model: Option<[u8; 32]>,
}

#[derive(Default)]
struct Counters {
    jobs: AtomicU64,
    shares: AtomicU64,
    accepted: AtomicU64,
    rejected: AtomicU64,
    solutions: AtomicU64,
}

struct State {
    pool: Option<TcpStream>,
    en1: Vec<u8>,
    en2_size: usize,
    target: Option<[u8; 32]>,
    models: HashMap<[u8; 32], Arc<Model>>,
    job: Option<PoolJob>,
    miners: Vec<Miner>,
    next_job_id: u64,
    next_req: u64,
    counter: u64,
    jobs: HashMap<u64, JobRef>,
    order: VecDeque<u64>,
    pending: HashMap<u64, Pending>,
    /// Pool answers per (miner job id, nonce), so that a solution after its share frame gets the share's answer.
    answers: HashMap<(u64, u32), bool>,
    submitted: HashMap<(u64, u32), u64>,
}

struct Proxy {
    st: Mutex<State>,
    stats: Counters,
    user: String,
    password: String,
}

impl Proxy {
    fn send_pool(st: &mut State, v: Value) -> bool {
        let Some(w) = st.pool.as_mut() else { return false };
        let mut line = v.to_string();
        line.push('\n');
        w.write_all(line.as_bytes()).is_ok()
    }

    /// The miner job for connection `m` on the current pool job, with a fresh extranonce2, or None if there is
    /// nothing to mine yet: (weights hash, Model frame, Job frame).
    fn make_job(&self, st: &mut State, tag: u8) -> Option<([u8; 32], Vec<u8>, Vec<u8>)> {
        let pj = st.job.clone()?;
        let model = st.models.get(&pj.weights_hash)?.clone();
        if st.en2_size < 2 || (now() as u32) > pj.template.expiry {
            return None;
        }
        st.counter += 1;
        // extranonce2 = tag || counter (little-endian, as many bytes as fit) || 0x00 (the miner's own zero byte)
        let mut en2 = vec![0u8; st.en2_size];
        en2[0] = tag;
        let c = st.counter.to_le_bytes();
        let n = (st.en2_size - 2).min(8);
        en2[1..1 + n].copy_from_slice(&c[..n]);
        let mut job = pj.template.clone();
        st.next_job_id += 1;
        job.id = st.next_job_id;
        job.clean = true;
        job.coinbase_prefix.extend_from_slice(&st.en1);
        job.coinbase_prefix.extend_from_slice(&en2[..st.en2_size - 1]);
        job.extranonce_size = 1;
        if let Some(t) = st.target {
            job.share_target = t;
        }
        st.jobs.insert(job.id, JobRef { pool_job_id: pj.job_id.clone(), en2, time: job.time_start });
        st.order.push_back(job.id);
        while st.order.len() > KEEP_JOBS {
            if let Some(old) = st.order.pop_front() {
                st.jobs.remove(&old);
                st.answers.retain(|k, _| k.0 != old);
                st.submitted.retain(|k, _| k.0 != old);
            }
        }
        self.stats.jobs.fetch_add(1, Ordering::Relaxed);
        Some((pj.weights_hash, frames::model_frame(pj.slot, &model), job.encode()))
    }

    /// Sends every miner a new job on the current pool job, preceded by a Model frame only when the miner does not
    /// hold that model yet.
    fn push_all(&self) {
        let mut st = self.st.lock().unwrap();
        let tags: Vec<(u64, u8)> = st.miners.iter().map(|m| (m.id, m.tag)).collect();
        let mut dead = Vec::new();
        for (id, tag) in tags {
            let Some((wh, model, job)) = self.make_job(&mut st, tag) else { return };
            let m = st.miners.iter_mut().find(|m| m.id == id).unwrap();
            let mut out = Vec::new();
            if m.sent_model != Some(wh) {
                out = model;
                m.sent_model = Some(wh);
            }
            out.extend_from_slice(&job);
            if m.writer.write_all(&out).is_err() {
                dead.push(id);
            }
        }
        st.miners.retain(|m| !dead.contains(&m.id));
    }

    /// A share (or solution) of miner job `job_id`: one mining.submit, unless that nonce was already submitted.
    fn submit(&self, job_id: u64, nonce: u32, solution: Option<u64>) {
        let mut st = self.st.lock().unwrap();
        let key = (job_id, nonce);
        if let Some(conn) = solution {
            self.stats.solutions.fetch_add(1, Ordering::Relaxed);
            if let Some(&accepted) = st.answers.get(&key) {
                answer_miner(&mut st, conn, accepted);
                return;
            }
            if let Some(&req) = st.submitted.get(&key) {
                if let Some(p) = st.pending.get_mut(&req) {
                    p.solution = Some(conn);
                }
                return;
            }
        } else if st.submitted.contains_key(&key) {
            return;
        }
        let Some(jr) = st.jobs.get(&job_id).cloned() else {
            eprintln!("share for an unknown or old job {job_id}; dropped");
            return;
        };
        st.next_req += 1;
        let req = st.next_req + 100;
        st.pending.insert(req, Pending { solution, key });
        st.submitted.insert(key, req);
        let msg = json!({"id": req, "method": "mining.submit",
                         "params": [self.user, jr.pool_job_id, hex(&jr.en2), jr.time, nonce]});
        self.stats.shares.fetch_add(1, Ordering::Relaxed);
        if !Proxy::send_pool(&mut st, msg) {
            eprintln!("pool not connected; share lost");
        }
    }
}

fn answer_miner(st: &mut State, conn: u64, accepted: bool) {
    if let Some(m) = st.miners.iter_mut().find(|m| m.id == conn) {
        let _ = m.writer.write_all(&ResultMsg { accepted, code: if accepted { 0 } else { 3 } }.encode());
    }
}

fn parse_job(j: &Value) -> Option<PoolJob> {
    let h = |k: &str| unhex(j[k].as_str()?);
    let h32 = |k: &str| unhex32(j[k].as_str()?);
    let u = |k: &str| j[k].as_u64().map(|v| v as u32);
    let template = Job {
        id: 0,
        clean: j["clean"].as_bool()?,
        version: u("version")?,
        parent: h32("parent")?,
        time_start: u("time_start")?,
        time_window: u("time_window")?,
        bits: u("bits")?,
        target: h32("target")?,
        share_target: h32("share_target")?,
        coinbase_prefix: h("coinbase_prefix")?,
        coinbase_suffix: h("coinbase_suffix")?,
        extranonce_size: 1,
        branch: j["branch"].as_array()?.iter().map(|b| b.as_str().and_then(unhex32)).collect::<Option<_>>()?,
        tx_count: u("tx_count")?,
        shares_root: h32("shares_root")?,
        height: u("height")?,
        expiry: u("expiry")?,
    };
    Some(PoolJob {
        job_id: j["job_id"].as_str()?.to_string(),
        slot: j["slot"].as_u64()? as u8,
        weights_hash: h32("weights_hash")?,
        clean: template.clean,
        template,
    })
}

/// Connects to the pool and reads it until the connection ends.
fn pool_session(px: &Arc<Proxy>, addr: &str) -> Result<(), String> {
    let stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream.set_nodelay(true).ok();
    {
        let mut st = px.st.lock().unwrap();
        st.pool = Some(stream.try_clone().map_err(|e| e.to_string())?);
        st.job = None;
        st.target = None;
        let agent = concat!("kobold-neb-proxy/", env!("CARGO_PKG_VERSION"));
        Proxy::send_pool(&mut st, json!({"id": 1, "method": "mining.subscribe", "params": [agent, "neb-pool/1"]}));
    }
    for line in BufReader::new(stream).lines() {
        let line = line.map_err(|e| e.to_string())?;
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        match v.get("method").and_then(|m| m.as_str()) {
            Some("mining.set_target") => {
                let t = v["params"][0].as_str().and_then(unhex32).ok_or("bad set_target")?;
                px.st.lock().unwrap().target = Some(t);
                println!("[pool] target set (work per share {:.0})", work_of(&t));
            }
            Some("mining.set_model") => {
                let p = &v["params"];
                let (Some(slot), Some(wh), Some(w)) =
                    (p[0].as_u64(), p[1].as_str().and_then(unhex32), p[2].as_str().and_then(unhex))
                else {
                    return Err("bad set_model".into());
                };
                let mut payload = vec![slot as u8];
                payload.extend_from_slice(&wh);
                payload.extend_from_slice(&w);
                let (_, model) = frames::decode_model(&payload).map_err(|e| format!("model: {e}"))?;
                px.st.lock().unwrap().models.insert(wh, Arc::new(model));
            }
            Some("mining.notify") => {
                let job = parse_job(&v["params"][0]).ok_or("bad notify")?;
                println!("[pool] job {} height {} slot {}{}", job.job_id, job.template.height, job.slot,
                         if job.clean { " (clean)" } else { "" });
                px.st.lock().unwrap().job = Some(job);
                px.push_all();
            }
            _ => {
                let id = v["id"].as_u64().unwrap_or(0);
                if id == 1 {
                    let r = &v["result"];
                    let mut st = px.st.lock().unwrap();
                    st.en1 = r["extranonce1"].as_str().and_then(unhex).ok_or("subscribe refused")?;
                    st.en2_size = r["extranonce2_size"].as_u64().unwrap_or(0) as usize;
                    println!("[pool] subscribed: extranonce1 {} extranonce2 {} bytes, chain {}",
                             hex(&st.en1), st.en2_size, r["chain_id"]);
                    if st.en2_size < 2 {
                        return Err("the pool's extranonce2 is too short for this proxy".into());
                    }
                    let (user, pass) = (px.user.clone(), px.password.clone());
                    Proxy::send_pool(&mut st, json!({"id": 2, "method": "mining.authorize", "params": [user, pass]}));
                } else if id == 2 {
                    if v["result"] != Value::Bool(true) {
                        return Err(format!("authorize refused: {}", v["error"]));
                    }
                    println!("[pool] authorized as {}", px.user);
                } else {
                    let ok = v["error"].is_null() && v["result"] == Value::Bool(true);
                    let mut st = px.st.lock().unwrap();
                    if let Some(p) = st.pending.remove(&id) {
                        st.answers.insert(p.key, ok);
                        if let Some(conn) = p.solution {
                            answer_miner(&mut st, conn, ok);
                        }
                    }
                    if ok {
                        px.stats.accepted.fetch_add(1, Ordering::Relaxed);
                    } else {
                        px.stats.rejected.fetch_add(1, Ordering::Relaxed);
                        println!("[pool] share REJECTED: {}", v["error"]);
                    }
                }
            }
        }
    }
    Err("the pool closed the connection".into())
}

/// Reads the frames of one miner connection until it closes.
fn serve_miner(px: &Arc<Proxy>, mut stream: TcpStream, id: u64) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 65536];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
        loop {
            match frames::parse_frame(&buf) {
                Ok(FrameOut::Need(_)) => break,
                Ok(FrameOut::Message { msg_type, payload, consumed }) => {
                    match msg_type {
                        MSG_HELLO => {
                            if let Ok(h) = frames::Hello::decode(payload) {
                                println!("[miner {id}] {}", h.name);
                            }
                        }
                        MSG_SHARE => {
                            if let Ok(s) = ShareMsg::decode(payload) {
                                px.submit(s.job_id, s.nonce, None);
                            }
                        }
                        MSG_SUBMIT => {
                            if let Ok(s) = Submit::decode(payload) {
                                println!("[miner {id}] solution on job {} (nonce {})", s.id, s.nonce);
                                px.submit(s.id, s.nonce, Some(id));
                            }
                        }
                        _ => {}
                    }
                    buf.drain(..consumed);
                }
                Err(e) => {
                    eprintln!("[miner {id}] bad frame: {e}");
                    let _ = stream.shutdown(Shutdown::Both);
                    return;
                }
            }
        }
    }
    println!("[miner {id}] disconnected");
    px.st.lock().unwrap().miners.retain(|m| m.id != id);
}

fn main() {
    let pool = arg("--pool", "127.0.0.1:3460");
    let user = arg("--user", "");
    let password = arg("--password", "");
    let listen = arg("--listen", "127.0.0.1:3451");
    let refresh: u64 = arg("--refresh", "30").parse().unwrap_or(30).max(1);
    if user.is_empty() {
        eprintln!("usage: kobold-neb-proxy --pool HOST:PORT --user NEB1ADDRESS[.worker] [--password X] [--listen 127.0.0.1:3451] [--refresh SECONDS]");
        std::process::exit(2);
    }
    let px = Arc::new(Proxy {
        st: Mutex::new(State {
            pool: None,
            en1: vec![],
            en2_size: 0,
            target: None,
            models: HashMap::new(),
            job: None,
            miners: vec![],
            next_job_id: 0,
            next_req: 0,
            counter: 0,
            jobs: HashMap::new(),
            order: VecDeque::new(),
            pending: HashMap::new(),
            answers: HashMap::new(),
            submitted: HashMap::new(),
        }),
        stats: Counters::default(),
        user,
        password,
    });
    // pool connection, reconnected with a back-off
    {
        let px = px.clone();
        thread::spawn(move || {
            let mut wait = 2;
            loop {
                match pool_session(&px, &pool) {
                    Ok(()) => {}
                    Err(e) => eprintln!("[pool] {e}; reconnecting in {wait} s"),
                }
                px.st.lock().unwrap().pool = None;
                thread::sleep(Duration::from_secs(wait));
                wait = (wait * 2).min(60);
            }
        });
    }
    // fresh extranonce2 for every miner every `refresh` seconds
    {
        let px = px.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(refresh));
            px.push_all();
        });
    }
    // status line
    {
        let px = px.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(30));
            let s = &px.stats;
            let st = px.st.lock().unwrap();
            println!("[stats] miners {}  pool {}  jobs {}  shares {}  accepted {}  rejected {}  solutions {}",
                     st.miners.len(), if st.pool.is_some() { "up" } else { "DOWN" },
                     s.jobs.load(Ordering::Relaxed), s.shares.load(Ordering::Relaxed),
                     s.accepted.load(Ordering::Relaxed), s.rejected.load(Ordering::Relaxed),
                     s.solutions.load(Ordering::Relaxed));
        });
    }
    let listener = TcpListener::bind(&listen).unwrap_or_else(|e| {
        eprintln!("cannot listen on {listen}: {e}");
        std::process::exit(1);
    });
    println!("kobold-neb-proxy: miners connect to {listen} (neb-miner-gpu --node {listen}), pool {}", arg("--pool", ""));
    let mut next_conn = 0u64;
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        stream.set_nodelay(true).ok();
        next_conn += 1;
        let id = next_conn;
        let Ok(writer) = stream.try_clone() else { continue };
        {
            let mut st = px.st.lock().unwrap();
            st.miners.push(Miner { id, writer, tag: (id % 256) as u8, sent_model: None });
        }
        println!("[miner {id}] connected");
        px.push_all();
        let px = px.clone();
        thread::spawn(move || serve_miner(&px, stream, id));
    }
}
