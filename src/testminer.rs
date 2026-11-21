//! neb-pool-testminer: a small CPU miner that speaks the pool protocol (spec/pool.md), to test a
//! pool end to end. It uses the node's reference code for the ticket, so it is slow but exact.
//!
//! neb-pool-testminer --pool 127.0.0.1:3460 --login neb1...addr.rig1 --dataset neb-datasets.bin [--threads 2]
//! Add --bad 5 to submit every 5th result with a wrong nonce (checks that the pool refuses it).

mod common;

use common::*;
use neb_block::Target;
use neb_pow::shares::{Model, ShareBuffers};
use neb_work::frames::{self, Job};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct Work {
    job_id: String,
    job: Job,
    slot: u8,
    model: Arc<Model>,
    target: [u8; 32],
}

struct State {
    en1: Vec<u8>,
    en2_size: usize,
    target: [u8; 32],
    models: HashMap<[u8; 32], (u8, Arc<Model>)>,
    work: Option<Arc<Work>>,
}

fn arg(name: &str, default: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1).cloned()).unwrap_or_else(|| default.to_string())
}

fn main() {
    let pool = arg("--pool", "127.0.0.1:3460");
    let login = arg("--login", "");
    let dataset = arg("--dataset", "neb-datasets.bin");
    let threads: usize = arg("--threads", "2").parse().unwrap();
    let bad_every: u64 = arg("--bad", "0").parse().unwrap();
    let password = arg("--password", "");
    let network = match arg("--network", "mainnet").as_str() {
        "testnet" => neb_node::params::Network::Testnet,
        _ => neb_node::params::Network::Mainnet,
    };
    let datasets = Arc::new(network.load_dataset(std::path::Path::new(&dataset)).expect("dataset"));
    let stream = TcpStream::connect(&pool).expect("connect to the pool");
    let out = Arc::new(Mutex::new(stream.try_clone().unwrap()));
    let send = {
        let out = out.clone();
        move |v: Value| {
            let mut l = v.to_string();
            l.push('\n');
            out.lock().unwrap().write_all(l.as_bytes()).expect("write to the pool");
        }
    };
    send(json!({"id": 1, "method": "mining.subscribe", "params": ["neb-pool-testminer/0.1", "neb-pool/1"]}));
    let st = Arc::new(Mutex::new(State { en1: vec![], en2_size: 0, target: [0; 32], models: HashMap::new(), work: None }));
    let generation = Arc::new(AtomicU64::new(0));
    let (attempts, found, next_id) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(10)));
    for t in 0..threads {
        let (st, generation, attempts, found, next_id, datasets) =
            (st.clone(), generation.clone(), attempts.clone(), found.clone(), next_id.clone(), datasets.clone());
        let send = send.clone();
        let login = login.clone();
        thread::spawn(move || {
            let mut buffers = ShareBuffers::new();
            let mut counter: u64 = 0;
            loop {
                let (gen0, work, en1, en2_size) = {
                    let s = st.lock().unwrap();
                    (generation.load(Ordering::SeqCst), s.work.clone(), s.en1.clone(), s.en2_size)
                };
                let Some(w) = work else {
                    thread::sleep(Duration::from_millis(200));
                    continue;
                };
                counter += 1;
                let mut en2 = vec![0u8; en2_size];
                en2[0] = t as u8;
                let c = counter.to_le_bytes();
                let n = (en2_size - 1).min(8);
                en2[1..1 + n].copy_from_slice(&c[..n]);
                let mut extranonce = en1.clone();
                extranonce.extend_from_slice(&en2);
                let time = w.job.time_start;
                let ds = &datasets[w.slot as usize];
                for nonce in 0u32..2000 {
                    if generation.load(Ordering::SeqCst) != gen0 {
                        break;
                    }
                    let header = w.job.header(&extranonce, time, nonce).encode();
                    let key = *blake3::hash(&header).as_bytes();
                    let ticket = buffers.ticket(&w.model, ds.as_ref(), &key);
                    attempts.fetch_add(1, Ordering::Relaxed);
                    if Target(w.target).admits(&ticket) {
                        let k = found.fetch_add(1, Ordering::Relaxed) + 1;
                        let sent_nonce = if bad_every > 0 && k % bad_every == 0 { nonce ^ 0x5a5a } else { nonce };
                        let id = next_id.fetch_add(1, Ordering::Relaxed);
                        let block = Target(w.job.target).admits(&ticket);
                        if block {
                            println!("[t{t}] block-level ticket at height {} (job {})", w.job.height, w.job_id);
                        }
                        send(json!({"id": id, "method": "mining.submit",
                                    "params": [login, w.job_id, hex(&en2), time, sent_nonce]}));
                    }
                }
            }
        });
    }
    {
        let attempts = attempts.clone();
        thread::spawn(move || {
            let start = Instant::now();
            loop {
                thread::sleep(Duration::from_secs(30));
                let a = attempts.load(Ordering::Relaxed);
                println!("[stats] {:.0} attempts/s", a as f64 / start.elapsed().as_secs_f64());
            }
        });
    }
    let mut accepted = 0u64;
    let mut rejected = 0u64;
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        match v.get("method").and_then(|m| m.as_str()) {
            Some("mining.set_target") => {
                let t = unhex32(v["params"][0].as_str().unwrap_or("")).expect("target");
                st.lock().unwrap().target = t;
                println!("[pool] target set (work per share {:.0})", work_of(&t));
            }
            Some("mining.set_model") => {
                let p = &v["params"];
                let slot = p[0].as_u64().unwrap() as u8;
                let wh = unhex32(p[1].as_str().unwrap()).unwrap();
                let mut payload = vec![slot];
                payload.extend_from_slice(&wh);
                payload.extend_from_slice(&unhex(p[2].as_str().unwrap()).unwrap());
                let (s2, model) = frames::decode_model(&payload).expect("model");
                assert_eq!(model.weights_hash(), wh, "model hash");
                st.lock().unwrap().models.insert(wh, (s2, Arc::new(model)));
            }
            Some("mining.notify") => {
                let j = &v["params"][0];
                let h = |k: &str| unhex(j[k].as_str().unwrap()).unwrap();
                let h32 = |k: &str| unhex32(j[k].as_str().unwrap()).unwrap();
                let u = |k: &str| j[k].as_u64().unwrap() as u32;
                let mut s = st.lock().unwrap();
                let wh = h32("weights_hash");
                let Some((slot, model)) = s.models.get(&wh).cloned() else {
                    println!("[pool] job for a model I don't hold; skipped");
                    continue;
                };
                let job = Job {
                    id: 0,
                    clean: j["clean"].as_bool().unwrap(),
                    version: u("version"),
                    parent: h32("parent"),
                    time_start: u("time_start"),
                    time_window: u("time_window"),
                    bits: u("bits"),
                    target: h32("target"),
                    share_target: h32("share_target"),
                    coinbase_prefix: h("coinbase_prefix"),
                    coinbase_suffix: h("coinbase_suffix"),
                    extranonce_size: (s.en1.len() + s.en2_size) as u8,
                    branch: j["branch"].as_array().unwrap().iter().map(|b| unhex32(b.as_str().unwrap()).unwrap()).collect(),
                    tx_count: u("tx_count"),
                    shares_root: h32("shares_root"),
                    height: u("height"),
                    expiry: u("expiry"),
                };
                assert_eq!(slot as u64, j["slot"].as_u64().unwrap(), "slot");
                let target = s.target;
                println!("[pool] job {} height {} clean {}", j["job_id"], job.height, job.clean);
                s.work = Some(Arc::new(Work { job_id: j["job_id"].as_str().unwrap().into(), job, slot, model, target }));
                generation.fetch_add(1, Ordering::SeqCst);
            }
            _ => {
                let id = v["id"].as_u64().unwrap_or(0);
                if id == 1 {
                    let r = &v["result"];
                    let mut s = st.lock().unwrap();
                    s.en1 = unhex(r["extranonce1"].as_str().expect("subscribed")).unwrap();
                    s.en2_size = r["extranonce2_size"].as_u64().unwrap() as usize;
                    println!("[pool] subscribed: extranonce1 {} size {} chain {}", hex(&s.en1), s.en2_size, r["chain_id"]);
                    drop(s);
                    send(json!({"id": 2, "method": "mining.authorize", "params": [login, password]}));
                } else if id == 2 {
                    println!("[pool] authorize: {}", v);
                } else if v["error"].is_null() {
                    accepted += 1;
                    if accepted % 10 == 1 {
                        println!("[pool] accepted {accepted}, rejected {rejected}");
                    }
                } else {
                    rejected += 1;
                    println!("[pool] REJECTED: {} (accepted {accepted}, rejected {rejected})", v["error"]);
                }
            }
        }
    }
    println!("pool closed the connection");
}
