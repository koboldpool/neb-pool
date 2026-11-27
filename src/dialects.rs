//! The two other pool dialects NEB miners speak, so every common miner works on the same ports without options:
//!
//! - **Suprnova-style stratum** (suprminer; the dialect of neb.suprnova.cc and 2Miners): `mining.subscribe` without
//!   "neb-pool/1" answers `[[subscriptions], extranonce1, 8]`; `mining.set_difficulty [attempts per share]` instead of
//!   `set_target`; `mining.set_model [weights_hash, slot, base64 weights]`; `mining.notify` as a positional array;
//!   `mining.submit [login, job_id, extranonce2, time hex, nonce hex]` (time and nonce big-endian hex).
//! - **NEB work protocol** (INVminer; Aria Energy in "aria" mode): the node's binary frames of spec/node.md 7.1. The
//!   first Hello names the login (`neb1....worker`, then optional words such as "ack"). Each Job carries the pool's
//!   share target in `share_target` and a coinbase prefix that ends with the session's extranonce1 and a job counter,
//!   so the miner's own extranonce bytes complete a header that is unique to the session. Shares come as Share frames
//!   (reference header: zero extranonce, `time_start`) or as Submit frames (rolled extranonce); every Submit gets a
//!   Result, and a miner that said "ack" gets a ShareAck frame (type 8, InnovLab's extension) for each Share.
//!
//! Both feed the same checks as neb-pool/1 (`attempt` in main.rs): one share code path for every miner.

use super::*;
use neb_work::frames::{MSG_HELLO, MSG_SHARE, MSG_SUBMIT, ShareMsg};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Proto {
    #[default]
    NebPool,
    Stratum,
    Work,
}

impl Proto {
    pub fn name(self) -> &'static str {
        match self {
            Proto::NebPool => "neb-pool/1",
            Proto::Stratum => "Suprnova-style stratum",
            Proto::Work => "NEB work protocol",
        }
    }
}

/// Standard base64 with padding (the Suprnova dialect sends the model weights this way).
pub fn base64(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                s.push(T[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}

// ------------------------------------------------------------------------------------- Suprnova-style stratum

/// Bytes of extranonce2 a stratum miner rolls: 8, as on Suprnova.
const STRATUM_EN2: usize = 8;

/// The stratum session's extranonce1 (our 4-byte en1, padded with zeros to the node's extranonce size minus 8) and
/// its extranonce2 size.
pub fn stratum_extranonce(en1: &[u8; 4], node_en: usize) -> (Vec<u8>, usize) {
    let en2 = STRATUM_EN2.min(node_en.saturating_sub(4));
    let mut x = en1.to_vec();
    x.resize(node_en - en2, 0);
    (x, en2)
}

pub fn stratum_subscribe_result(session: u64, en1x: &[u8], en2: usize) -> Value {
    let sid = format!("{session:016x}");
    json!([[["mining.set_difficulty", sid], ["mining.notify", sid]], hex(en1x), en2])
}

/// The difficulty of a pool target: expected attempts per share (Suprnova: one unit = one training attempt), raised by
/// a hair so the miner's own target, 2^256 / difficulty, never lies above the pool target.
pub fn difficulty(target: &[u8; 32]) -> f64 {
    work_of(target) * (1.0 + 1e-9)
}

fn be32(v: u32) -> String {
    format!("{v:08x}")
}

/// `mining.notify` params as Suprnova sends them: job_id, parent, coinbase prefix, coinbase suffix, branch, version,
/// bits, time (big-endian hex), clean, height, weights_hash, slot, tx_count (hex), shares_root, latest time (hex).
pub fn stratum_notify(n: &Job, job_id: &str, slot: u8, weights_hash: &[u8; 32], clean: bool) -> Value {
    json!([
        job_id,
        hex(&n.parent),
        hex(&n.coinbase_prefix),
        hex(&n.coinbase_suffix),
        n.branch.iter().map(|b| hex(b)).collect::<Vec<_>>(),
        be32(n.version),
        be32(n.bits),
        be32(n.time_start),
        clean,
        n.height,
        hex(weights_hash),
        slot,
        be32(n.tx_count),
        hex(&n.shares_root),
        be32(n.time_start.saturating_add(n.time_window))
    ])
}

// ------------------------------------------------------------------------------------- NEB work protocol

/// InnovLab's acknowledgement of a Share frame: accepted (u8) || code (u8) || job id (u64) || nonce (u32).
const MSG_SHARE_ACK: u8 = 8;
/// Jobs a work-protocol session can still submit to.
const KEEP_WORK_JOBS: usize = 64;

/// One job as sent to a work-protocol miner.
pub struct WorkJob {
    id: u64,
    job: Arc<PoolJob>,
    /// Bytes the pool put after the node's coinbase prefix (extranonce1 || job counter).
    extra: Vec<u8>,
    /// Bytes of extranonce the miner adds.
    miner_en: usize,
    /// The vardiff k of the share target the job carried.
    k: u32,
}

#[derive(Default)]
pub struct WorkState {
    jobs: VecDeque<WorkJob>,
    next: u64,
    /// The miner asked for ShareAck frames ("ack" in its Hello).
    ack: bool,
}

/// True when the first 5 bytes are a Hello frame header: length 3..=4096, type 1. A JSON line starts with '{'.
pub fn looks_like_hello(b: &[u8]) -> bool {
    b.len() >= 5 && b[4] == MSG_HELLO && (3..=4096).contains(&u32::from_le_bytes(b[..4].try_into().unwrap()))
}

/// Peeks at the first bytes of a connection (without consuming them) to tell a work-protocol miner from a JSON one.
pub fn is_work_hello(stream: &TcpStream) -> bool {
    let mut b = [0u8; 5];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match stream.peek(&mut b) {
            Ok(0) | Err(_) => return false,
            Ok(n) if n >= 5 => return looks_like_hello(&b),
            Ok(_) if b[0] == b'{' || b[0].is_ascii_whitespace() => return false,
            Ok(_) if Instant::now() > deadline => return false,
            Ok(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// The job a work-protocol miner gets: the node job with the pool target as `share_target`, and `extra`
/// (extranonce1 || the low bytes of the job counter) appended to the coinbase prefix; the miner fills the rest of the
/// node's extranonce. Returns the job and `extra`.
pub fn work_job(node: &Job, id: u64, clean: bool, share_target: [u8; 32], en1: &[u8]) -> (Job, Vec<u8>) {
    let node_en = node.extranonce_size as usize;
    let mut extra = en1.to_vec();
    // Leave the miner at least 1 byte; up to 4 bytes of counter make every refresh a new header space.
    let ctr = node_en.saturating_sub(extra.len() + 1).min(4);
    extra.extend_from_slice(&(id as u32).to_le_bytes()[..ctr]);
    let mut j = node.clone();
    j.id = id;
    j.clean = clean;
    j.share_target = share_target;
    j.coinbase_prefix.extend_from_slice(&extra);
    j.extranonce_size = (node_en - extra.len()) as u8;
    (j, extra)
}

fn send_bytes(sess: &Session, b: &[u8]) {
    let ok = sess.out.lock().map(|mut s| s.write_all(b).is_ok()).unwrap_or(false);
    if !ok {
        sess.alive.store(false, Ordering::Relaxed);
    }
}

/// Sends a work-protocol miner a fresh job (its Model frame first when the model is new to it).
pub fn push_work_job(sess: &Session, job: &Arc<PoolJob>, clean: bool) {
    let mut out = Vec::new();
    {
        let mut st = sess.st.lock().unwrap();
        if st.logins.is_empty() {
            return;
        }
        let target = shifted(&job.node.share_target, st.k);
        st.sent_target = Some(target);
        if !st.sent_models.contains(&job.weights_hash) {
            st.sent_models.push_back(job.weights_hash);
            if st.sent_models.len() > 8 {
                st.sent_models.pop_front();
            }
            let mut p = vec![job.slot];
            p.extend_from_slice(&job.weights_hash);
            p.extend_from_slice(&job.weights.raw);
            out.extend_from_slice(&frames::frame(MSG_MODEL, &p));
        }
        st.work.next += 1;
        let id = st.work.next;
        let (j, extra) = work_job(&job.node, id, clean, target, &st.en1);
        out.extend_from_slice(&j.encode());
        let miner_en = j.extranonce_size as usize;
        let k = st.k;
        st.work.jobs.push_back(WorkJob { id, job: job.clone(), extra, miner_en, k });
        while st.work.jobs.len() > KEEP_WORK_JOBS {
            st.work.jobs.pop_front();
        }
    }
    send_bytes(sess, &out);
}

/// Every 10 s (stats loop): a fresh job for every work-protocol miner, so one that only rolls the nonce of the
/// reference header (2^32 attempts per job) never runs dry between blocks.
pub fn refresh_work_sessions(sh: &Arc<Shared>) {
    let Some(job) = sh.latest_job() else { return };
    let sessions: Vec<Arc<Session>> = sh.sessions.lock().unwrap().clone();
    for s in sessions {
        if s.alive.load(Ordering::Relaxed) && s.st.lock().unwrap().proto == Proto::Work {
            push_work_job(&s, &job, false);
        }
    }
}

/// Serves a work-protocol miner until it disconnects.
pub fn serve_work(sh: &Arc<Shared>, sess: &Arc<Session>, mut stream: TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = vec![0u8; 16384];
    let mut buffers = ShareBuffers::new();
    loop {
        match stream.read(&mut tmp) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
        // Miners send small frames only (Hello, Share, Submit).
        if buf.len() > 65536 {
            return;
        }
        loop {
            match frames::parse_frame(&buf) {
                Ok(FrameOut::Need(_)) => break,
                Ok(FrameOut::Message { msg_type, payload, consumed }) => {
                    let payload = payload.to_vec();
                    buf.drain(..consumed);
                    let go_on = match msg_type {
                        MSG_HELLO => work_hello(sh, sess, &payload),
                        MSG_SHARE => work_share(sh, sess, &payload, &mut buffers),
                        MSG_SUBMIT => work_submit(sh, sess, &payload, &mut buffers),
                        _ => true,
                    };
                    if !go_on || !sess.alive.load(Ordering::Relaxed) {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    }
}

/// The first Hello logs the miner in; later ones (INVminer sends its settings in a second Hello) are ignored.
fn work_hello(sh: &Arc<Shared>, sess: &Session, payload: &[u8]) -> bool {
    if !sess.st.lock().unwrap().logins.is_empty() {
        return true;
    }
    let Ok(h) = Hello::decode(payload) else { return false };
    let login = h.name.split_whitespace().next().unwrap_or("").to_string();
    if valid_login(sh.address_prefix, &login).is_none() {
        let shown: String = h.name.chars().take(64).collect();
        sh.log(&format!("session {} {} work-protocol hello without a neb1 login ({shown:?}); closed", sess.id, sess.peer));
        return false;
    }
    {
        let mut st = sess.st.lock().unwrap();
        st.subscribed = true;
        st.proto = Proto::Work;
        st.en1x = st.en1.to_vec();
        st.work.ack = h.name.split_whitespace().any(|w| w == "ack");
        st.logins.insert(login.clone());
        if sess.solo_port {
            st.solo.insert(login.clone());
        }
    }
    sh.log(&format!(
        "session {} {} authorized {login}{} ({})",
        sess.id,
        sess.peer,
        if sess.solo_port { " (solo)" } else { "" },
        Proto::Work.name()
    ));
    if let Some(job) = sh.latest_job() {
        push_work_job(sess, &job, true);
    }
    true
}

/// The login and the job a Share or Submit names; None for an unknown or expired job.
fn work_lookup(sess: &Session, id: u64) -> Option<(String, Arc<PoolJob>, Vec<u8>, usize, u32)> {
    let st = sess.st.lock().unwrap();
    let login = st.logins.iter().next()?.clone();
    let w = st.work.jobs.iter().rev().find(|w| w.id == id)?;
    Some((login, w.job.clone(), w.extra.clone(), w.miner_en, w.k))
}

/// Rate limit, job lookup and expiry; then the common checks of main.rs.
fn work_attempt(
    sh: &Shared,
    sess: &Session,
    id: u64,
    miner_extranonce: Option<&[u8]>,
    time: Option<u32>,
    nonce: u32,
    buffers: &mut ShareBuffers,
) -> (String, Result<(), (u32, &'static str)>) {
    if !rate_ok(sess, &mut sess.st.lock().unwrap()) {
        return (String::new(), Err((20, "too many submits")));
    }
    let Some((login, job, extra, miner_en, k)) = work_lookup(sess, id) else {
        let login = sess.st.lock().unwrap().logins.iter().next().cloned().unwrap_or_default();
        return (login, Err((21, "job not found or past its expiry")));
    };
    if (now() as u32) > job.node.expiry {
        return (login, Err((21, "job not found or past its expiry")));
    }
    let mut extranonce = extra;
    match miner_extranonce {
        Some(e) if e.len() == miner_en => extranonce.extend_from_slice(e),
        Some(_) => return (login, Err((27, "extranonce of the wrong length"))),
        None => extranonce.resize(extranonce.len() + miner_en, 0),
    }
    let time = time.unwrap_or(job.node.time_start);
    let r = attempt(sh, sess, &login, &job, &extranonce, time, nonce, buffers, Some(k));
    (login, r)
}

fn log_work_reject(sh: &Shared, sess: &Session, login: &str, r: &Result<(), (u32, &'static str)>) {
    if let Err((code, msg)) = r {
        log_reject(sh, sess, &[json!(login)], &json!({"error": [code, msg]}));
    }
}

/// Node result code for a refused attempt: 4 (unknown job) for a stale one, else 1 (bad submission).
fn result_code(r: &Result<(), (u32, &'static str)>) -> u8 {
    match r {
        Ok(()) => 0,
        Err((21, _)) => 4,
        Err(_) => 1,
    }
}

fn work_share(sh: &Shared, sess: &Session, payload: &[u8], buffers: &mut ShareBuffers) -> bool {
    let Ok(s) = ShareMsg::decode(payload) else { return false };
    let (login, r) = work_attempt(sh, sess, s.job_id, None, None, s.nonce, buffers);
    log_work_reject(sh, sess, &login, &r);
    if sess.st.lock().unwrap().work.ack {
        let mut p = vec![r.is_ok() as u8, result_code(&r)];
        p.extend_from_slice(&s.job_id.to_le_bytes());
        p.extend_from_slice(&s.nonce.to_le_bytes());
        send_bytes(sess, &frames::frame(MSG_SHARE_ACK, &p));
    }
    true
}

fn work_submit(sh: &Shared, sess: &Session, payload: &[u8], buffers: &mut ShareBuffers) -> bool {
    let Ok(s) = Submit::decode(payload) else { return false };
    let (login, r) = work_attempt(sh, sess, s.id, Some(&s.extranonce), Some(s.time), s.nonce, buffers);
    log_work_reject(sh, sess, &login, &r);
    send_bytes(sess, &ResultMsg { accepted: r.is_ok(), code: result_code(&r) }.encode());
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_job() -> Job {
        Job {
            id: 77,
            clean: true,
            version: 1,
            parent: [3; 32],
            time_start: 1_795_740_222,
            time_window: 3600,
            bits: 0xb993_331a,
            target: [0x11; 32],
            share_target: [0x22; 32],
            coinbase_prefix: vec![1, 1, 0, 0, 0xff],
            coinbase_suffix: vec![2, 0, 0xb3],
            extranonce_size: 16,
            branch: vec![[5; 32], [6; 32]],
            tx_count: 3,
            shares_root: [9; 32],
            height: 12306,
            expiry: 1_795_740_522,
        }
    }

    #[test]
    fn base64_matches_rfc4648() {
        for (raw, b64) in [("", ""), ("f", "Zg=="), ("fo", "Zm8="), ("foo", "Zm9v"), ("foob", "Zm9vYg=="), ("fooba", "Zm9vYmE="), ("foobar", "Zm9vYmFy")] {
            assert_eq!(base64(raw.as_bytes()), b64);
        }
        assert_eq!(base64(&[0xcc, 0xcc, 0x2a, 0x54]), "zMwqVA==");
    }

    #[test]
    fn hello_detection() {
        let hello = frames::frame(MSG_HELLO, &Hello { version: 1, name: "neb1abc.rig1 invminer-neb/2.2.0 ack".into() }.encode());
        assert!(looks_like_hello(&hello));
        assert!(!looks_like_hello(br#"{"id":1,"method":"mining.subscribe","params":[]}"#));
        assert!(!looks_like_hello(b"\n{\"id\""));
        // A long name keeps the length below 4096; a JSON line never has a 1 at byte 4 after a small length.
        let long = frames::frame(MSG_HELLO, &Hello { version: 1, name: "x".repeat(300) }.encode());
        assert!(looks_like_hello(&long));
    }

    #[test]
    fn stratum_extranonce_is_suprnova_shaped() {
        let (x, en2) = stratum_extranonce(&[0xaa, 0xbb, 0xcc, 0xdd], 16);
        assert_eq!((x, en2), (vec![0xaa, 0xbb, 0xcc, 0xdd, 0, 0, 0, 0], 8));
        let r = stratum_subscribe_result(0x2072, &[0xaa, 0xbb, 0xcc, 0xdd, 0, 0, 0, 0], 8);
        assert_eq!(r, json!([[["mining.set_difficulty", "0000000000002072"], ["mining.notify", "0000000000002072"]], "aabbccdd00000000", 8]));
    }

    #[test]
    fn stratum_notify_layout() {
        let n = node_job();
        let v = stratum_notify(&n, "fde0", 0, &[7; 32], true);
        let a = v.as_array().unwrap();
        assert_eq!(a.len(), 15);
        assert_eq!(a[0], "fde0");
        assert_eq!(a[2], "01010000ff");
        assert_eq!(a[5], "00000001");
        assert_eq!(a[6], "b993331a");
        assert_eq!(a[7], "6b08d23e"); // the Suprnova capture's time for 1795740222
        assert_eq!(a[8], true);
        assert_eq!(a[9], 12306);
        assert_eq!(a[11], 0);
        assert_eq!(a[12], "00000003");
        assert_eq!(a[14], "6b08e04e"); // time_start + 3600, as in the capture
    }

    #[test]
    fn stratum_params_parse() {
        assert_eq!(u32_param(&json!("6b08d23e")), Some(0x6b08_d23e));
        assert_eq!(u32_param(&json!("af1a98ea")), Some(0xaf1a_98ea));
        assert_eq!(u32_param(&json!(1_795_738_652u64)), Some(1_795_738_652));
        assert_eq!(u32_param(&json!(5_000_000_000u64)), None);
        assert_eq!(u32_param(&json!("")), None);
        assert_eq!(u32_param(&json!("123456789")), None);
    }

    #[test]
    fn difficulty_never_loosens_the_target() {
        let mut t = [0u8; 32];
        t[24..28].copy_from_slice(&[0xe2, 0xba, 0xbc, 0x00]);
        let d = difficulty(&t);
        // The miner's target 2^256 / d must not be above ours.
        assert!(2f64.powi(256) / d <= le_to_f64(&t) + 1.0);
        assert!((d / work_of(&t) - 1.0).abs() < 1e-8);
    }

    #[test]
    fn work_job_reconstructs_the_pool_header() {
        let n = node_job();
        let en1 = [0x43, 0x12, 0x1e, 0xf5];
        let (j, extra) = work_job(&n, 5, false, [0x44; 32], &en1);
        assert_eq!(extra, vec![0x43, 0x12, 0x1e, 0xf5, 5, 0, 0, 0]);
        assert_eq!(j.extranonce_size, 8);
        assert_eq!(j.share_target, [0x44; 32]);
        assert_eq!((j.id, j.clean, j.target), (5, false, n.target));
        // The miner's reference header (zero extranonce) is the pool's header for extranonce extra || zeros.
        let mut full = extra.clone();
        full.resize(16, 0);
        assert_eq!(j.header(&[0; 8], n.time_start, 42).encode(), n.header(&full, n.time_start, 42).encode());
        // A rolled extranonce too.
        let mine = [9, 8, 7, 6, 5, 4, 3, 2];
        let mut full2 = extra;
        full2.extend_from_slice(&mine);
        assert_eq!(j.header(&mine, n.time_start + 1, 43).encode(), n.header(&full2, n.time_start + 1, 43).encode());
        // The frame decodes as a valid job.
        let f = j.encode();
        let back = Job::decode(&f[5..]).unwrap();
        assert_eq!(back, j);
    }

    #[test]
    fn work_job_with_a_small_node_extranonce() {
        let mut n = node_job();
        n.extranonce_size = 8;
        let (j, extra) = work_job(&n, 1, true, [0; 32], &[1, 2, 3, 4]);
        assert_eq!((extra.len(), j.extranonce_size), (7, 1));
    }
}
