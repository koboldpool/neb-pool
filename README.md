# neb-pool

The mining pool software behind [Kobold Pool](https://koboldpool.com/neb/) for **NEB** (Neural Engine Byte), published so
miners can check exactly how their shares are counted and paid.

It implements NEB's own pool protocol, `neb-pool/1` ([spec/pool.md](https://github.com/SilasEigen/neb/blob/main/spec/pool.md)),
on top of the official NEB node's crates, fetched from the [NEB repository](https://github.com/SilasEigen/neb) at a release
tag (see `Cargo.toml`; `Cargo.lock` pins the exact commit).

| Program | What it does |
|---|---|
| `neb-pool` | the pool server: stratum for miners (PPLNS port and solo port), jobs from the node, every share checked with the node's own code |
| `payout.py` | PPLNS payouts: credits mature pool blocks, pays balances with `neb-wallet`, tracks every payment until it is deep in the chain |
| `neb-pool-txids` | helper for `payout.py`: the transaction ids of blocks, decoded with the node's code |
| `kobold-neb-proxy` | lets NEB's official, unmodified `neb-miner-gpu` / `neb-miner-cpu` mine on any `neb-pool/1` pool |
| `neb-pool-testminer` | a small CPU miner for testing a pool end to end |

## How shares and payouts work

- **Share check.** The pool recomputes the ticket of every submitted share with `neb-pow`, the node's own proof-of-work
  code, and accepts it only if it meets the session's pool target. Duplicates, stale jobs and out-of-window times are
  refused. Each accepted share is logged with its work (`<data>/shares.jsonl`).
- **Difficulty.** Per-connection vardiff aims at 3–12 shares a minute. Shares count by their **work**, not their number, so
  a miner on a harder target loses nothing.
- **Blocks.** A share that meets the network target is sent to the node as a block; the node's answer is logged
  (`<data>/blocks.jsonl`) together with the fee in force when the block was found.
- **PPLNS.** When a block is mature (100 confirmations) and still on the best chain, `payout.py` splits its reward, minus
  that block's fee, over the pool shares in the last 2 × (block work) before it, in proportion to their work.
  Solo-port blocks go whole to their finder, minus the solo fee.
- **Payments.** Balances at or above the minimum are paid with `neb-wallet send`. Each payment is written to the ledger
  (`<data>/payouts.json`) before the next one is sent, waits for the previous one to confirm, and is checked until it is
  6 blocks deep. A payment that disappears (for example a node restart that drops its mempool) is marked failed and its
  amount goes back to the miner's balance, to be paid again. A block is never credited twice.
- **Fees.** Read from `<data>/pool-config.json`, `{"fee_percent": 0.0, "solo_fee_percent": 0.0}`, re-read every 10 s and
  limited to 0–10 %. Changing them never touches blocks already found.

## Build

Rust 1.85 or later (edition 2024). The NEB crates are downloaded from GitHub by Cargo.

```
cargo build --release --locked
```

The binaries are in `target/release/`. Tested with Rust 1.96 on Debian 12 (`docker run rust:1.96-bookworm`).

GitHub Actions (`.github/workflows/build.yml`) builds and tests every push on GitHub's servers. Each release (tag `v*`)
carries the Linux binaries built that way, with `SHA256SUMS` and a link to the build log, so you can check that a
download was built from this source.

## Run

1. Run a NEB node (official release) with `--miner-lock` set to your pool wallet's lock
   (`neb-wallet --wallet pool-wallet.txt lock`), so block rewards go to the pool wallet. Its work service (3441) and
   peer port (3440) must be reachable from the pool, normally on `127.0.0.1`.
2. Start the pool with the network's dataset file:

   ```
   neb-pool --network mainnet --dataset neb-datasets.bin --data pool-data \
            [--listen 0.0.0.0:3460] [--listen-solo 0.0.0.0:3470] [--node-work 127.0.0.1:3441] [--node-p2p 127.0.0.1:3440]
   ```

   It writes `stats.json` every 10 s (hashrate, miners and workers, height, effort), plus the share and block logs.
3. Run the payouts every 30 minutes or so (a cron job or systemd timer). Without `--pay` it only reports:

   ```
   NEB_WALLET_PASSWORD=... python3 payout.py --data pool-data --wallet pool-wallet.txt \
       --wallet-bin neb-wallet --txids-bin neb-pool-txids --min 1 --pay
   ```

   Options: `--fee`, `--window` (PPLNS window in block works, default 2), `--min` (minimum payout), `--reserve` (NEB
   always left in the wallet for transaction fees), `--rpc` (the node's JSON-RPC, default `http://127.0.0.1:8432`).

Miners connect with any `neb-pool/1` miner, login `neb1ADDRESS.worker`; a password containing `solo` on the PPLNS port
also selects solo mode.

## Official miner through the proxy

```
kobold-neb-proxy --pool HOST:3460 --user neb1ADDRESS.rig1 [--listen 127.0.0.1:3451] [--refresh 30]
neb-miner-gpu --node 127.0.0.1:3451 --dataset neb-datasets.bin
```

The proxy looks like a node's work service to the miner and turns each of its shares into one `mining.submit`.

## Tests

```
python3 -m unittest test_payout        # PPLNS crediting, fees per block, maturity
```

The pool has also been run end to end on a private NEB chain (blocks found, payouts sent and received, a lost payment
re-credited and paid again), and it runs Kobold Pool on the NEB main network.

## License

Apache License 2.0 (`LICENSE`). Built on the NEB node's crates, also Apache-2.0, © the NEB authors.
