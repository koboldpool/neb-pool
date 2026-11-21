//! neb-pool-txids: reads blocks as hex (one per line, as the node's `getblock` returns them) on
//! stdin and prints the transaction ids of each block on one line, space separated. payout.py
//! uses it to check that a payment really is in a block (the node's RPC has no tx lookup).

mod common;

use common::{hex, unhex};
use std::io::{BufRead, Write};

fn main() {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.expect("read stdin");
        let ids = unhex(line.trim())
            .ok_or_else(|| "not hex".to_string())
            .and_then(|b| neb_chain::Block::decode(&b))
            .map(|b| b.txs.iter().map(|t| hex(&t.txid())).collect::<Vec<_>>().join(" "));
        match ids {
            Ok(s) => writeln!(out, "{s}").unwrap(),
            Err(e) => writeln!(out, "ERROR {e}").unwrap(),
        }
    }
}
