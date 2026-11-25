#!/usr/bin/env python3
"""PPLNS payouts for neb-pool: credits mature pool blocks (once, only while on the best chain) over the last WINDOW x
block work of shares, and with --pay sends balances above --min with neb-wallet (password from NEB_WALLET_PASSWORD),
never more than the confirmed balance minus --reserve. Ledger: <data>/payouts.json.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# Shared ledger helper (lock, dated copy, fsync'd saves, pending-before-send), loaded by path so it also works
# under python3 -I (which leaves the script's folder off sys.path).
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location("kobold_ledger", Path(__file__).resolve().parent / "kobold_ledger.py")
kl = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(kl)

COINBASE_MATURITY = 100
COIN = 100_000_000           # base units per NEB
BASE_REWARD = 20 * COIN      # per block, before the project share
HALVING = 450_000
PROJECT_SHARE = 0.05         # 5 % of the reward stays with the project (in the coinbase)


def rpc(url: str, method: str, *params):
    body = json.dumps({"jsonrpc": "1.0", "id": 1, "method": method, "params": list(params)}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read())
    if out.get("error"):
        raise RuntimeError(f"{method}: {out['error']}")
    return out["result"]


def pool_reward(height: int) -> int:
    """Base units the pool's lock receives for a block (fees are left in the pool as margin)."""
    base = BASE_REWARD >> (height // HALVING)
    return base - int(base * PROJECT_SHARE)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # a line cut by a crash
    return out


def wallet_confirmed(args) -> int | None:
    """Confirmed (spendable) base units in the pool wallet, from `neb-wallet balance`, which prints
    one line per address (`neb1...  133 NEB`, confirmed coins), then `immature N` and `total N NEB`."""
    p = subprocess.run([args.wallet_bin, "--rpc", args.rpc, "--wallet", args.wallet, "balance"],
                       capture_output=True, text=True, timeout=300)
    amounts = re.findall(r"^\s*neb1\S+\s+([\d.]+)\s+NEB\s*$", p.stdout, re.M)
    if p.returncode != 0 or not amounts:
        return None
    return sum(int(round(float(a) * COIN)) for a in amounts)


PAYMENT_CONFIRMATIONS = 6     # a payment counts as done this deep in the chain
PAYMENT_LOST_AFTER = 10       # blocks after sending with the tx neither mined nor in the mempool


def block_txids(args, heights: list[int]) -> dict[int, set[str]]:
    """Transaction ids of the blocks at `heights` on the best chain, decoded by neb-pool-txids."""
    if not heights:
        return {}
    hexes = [rpc(args.rpc, "getblock", rpc(args.rpc, "getblockhash", h)) for h in heights]
    p = subprocess.run([args.txids_bin], input="\n".join(hexes) + "\n", capture_output=True, text=True, timeout=300)
    lines = p.stdout.splitlines()
    if p.returncode != 0 or len(lines) != len(heights) or any(l.startswith("ERROR") for l in lines):
        raise RuntimeError(f"neb-pool-txids failed: {p.stderr[-300:]} {lines[:1]}")
    return {h: set(l.split()) for h, l in zip(heights, lines)}


def reconcile(args, ledger: dict, tip: int) -> None:
    """Checks every unconfirmed payment against the chain. A payment that was neither mined nor
    is waiting in the mempool PAYMENT_LOST_AFTER blocks after it was sent (e.g. a node restart
    dropped its mempool) is marked failed and its amount goes back to the miner's balance."""
    pending = [p for p in ledger["payments"] if p.get("status", "pending") == "pending"]
    if not pending:
        return
    mempool = set(rpc(args.rpc, "getrawmempool") or [])
    lo = min(p.get("height_sent", max(0, tip - 200)) for p in pending)
    found: dict[str, int] = {}
    for h, ids in block_txids(args, list(range(max(1, lo), tip + 1))).items():
        for t in ids:
            found.setdefault(t, h)
    for p in pending:
        sent = p.setdefault("height_sent", max(0, tip - 200))
        h = found.get(p["txid"])
        if h is not None:
            p["mined_height"] = h
            if tip - h + 1 >= PAYMENT_CONFIRMATIONS:
                p["status"] = "confirmed"
                print(f"payment {p['txid'][:16]} to {p['addr'][:16]}... confirmed at height {h}")
        elif p["txid"] in mempool:
            continue
        elif tip - sent >= PAYMENT_LOST_AFTER:
            p["status"] = "failed"
            ledger["balances"][p["addr"]] = ledger["balances"].get(p["addr"], 0) + p["amount"]
            ledger["paid"][p["addr"]] = ledger["paid"].get(p["addr"], 0) - p["amount"]
            print(f"PAYMENT LOST: {p['amount'] / COIN:.8f} NEB to {p['addr']} ({p['txid']}) was never mined; "
                  f"credited back to the balance")


def wait_confirmed(args, txid: str, timeout_s: int = 900) -> bool:
    """True once `txid` is no longer in the node's mempool (mined), polling every 10 s."""
    end = time.time() + timeout_s
    while time.time() < end:
        if txid not in (rpc(args.rpc, "getrawmempool") or []):
            return True
        time.sleep(10)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="pool-data")
    ap.add_argument("--rpc", default="http://127.0.0.1:8432")
    ap.add_argument("--wallet-bin", default="neb-wallet")
    ap.add_argument("--wallet", default="pool-wallet.txt")
    ap.add_argument("--txids-bin", default="neb-pool-txids")
    ap.add_argument("--fee", type=float, default=0.0,
                    help="fee in percent for old block records that don't carry their own fee_bp")
    ap.add_argument("--window", type=float, default=2.0, help="PPLNS window in block works")
    ap.add_argument("--min", type=float, default=1.0, help="minimum payout in NEB")
    ap.add_argument("--reserve", type=float, default=0.01, help="NEB always left in the wallet for fees")
    ap.add_argument("--pay", action="store_true", help="actually send payments")
    args = ap.parse_args()

    data = Path(args.data)
    ledger_path = data / "payouts.json"
    led = kl.Ledger(ledger_path)  # lock + dated copy before the run
    ledger = led.open({
        "credited": {}, "orphaned": {}, "balances": {}, "paid": {}, "payments": [], "pool_fees": 0})

    def save():
        led.save(ledger)  # temp file, fsync, rename, fsync the folder

    tip = int(rpc(args.rpc, "getblockcount"))
    reconcile(args, ledger, tip)
    save()
    events = read_jsonl(data / "blocks.jsonl")
    found = {e["id"]: e for e in events if e.get("event") == "found"}
    accepted = {e["id"] for e in events if e.get("event") == "result" and e.get("accepted")}
    shares = read_jsonl(data / "shares.jsonl")

    for bid, blk in sorted(found.items(), key=lambda kv: kv[1]["height"]):
        if bid in ledger["credited"] or bid in ledger["orphaned"] or bid not in accepted:
            continue
        h = blk["height"]
        if tip - h < COINBASE_MATURITY:
            continue  # not mature yet
        if rpc(args.rpc, "getblockhash", h) != bid:
            ledger["orphaned"][bid] = {"height": h, "ts": blk["ts"]}
            print(f"block {h} {bid[:16]} is not on the best chain: orphaned, nothing credited")
            continue
        reward = pool_reward(h)
        # The fee in force when the block was found (recorded by the pool); --fee only for old
        # records without it. So a fee change never touches blocks found before it.
        fee_bp = int(blk["fee_bp"]) if "fee_bp" in blk else int(round(args.fee * 100))
        fee = reward * fee_bp // 10_000
        to_split = reward - fee
        mode = blk.get("mode", "pplns")
        work_by: dict[str, float] = {}
        total = 0.0
        if mode == "solo":
            # Solo: the whole block (less the solo fee) goes to the address that found it.
            work_by[blk["finder"].split(".", 1)[0]] = 1.0
            total = 1.0
        else:
            # PPLNS: walk back from the block over pool (non-solo) shares until the window is filled.
            window = args.window * float(blk.get("block_work") or 0)
            for s in reversed(shares):
                if s["ts"] > blk["ts"] or s.get("mode", "pplns") == "solo":
                    continue
                if window and total >= window:
                    break
                work_by[s["addr"]] = work_by.get(s["addr"], 0.0) + s["work"]
                total += s["work"]
        if total <= 0:
            print(f"block {h}: no shares in the window, kept as pool income")
            ledger["pool_fees"] += reward
        else:
            given = 0
            for addr, w in work_by.items():
                part = int(to_split * w / total)
                ledger["balances"][addr] = ledger["balances"].get(addr, 0) + part
                given += part
            ledger["pool_fees"] += reward - given
        ledger["credited"][bid] = {"height": h, "reward": reward, "fee": fee, "fee_bp": fee_bp, "mode": mode,
                                   "miners": len(work_by), "ts": blk["ts"]}
        print(f"block {h} credited ({mode}, fee {fee_bp / 100:.2f} %): {reward / COIN:.8f} NEB over "
              f"{len(work_by)} miners (fee {fee / COIN:.8f})")
        save()

    due = {a: b for a, b in ledger["balances"].items() if b >= args.min * COIN}
    print(f"tip {tip}; {len(ledger['credited'])} blocks credited; "
          f"{len(due)} balances due (>= {args.min} NEB); owed in total "
          f"{sum(ledger['balances'].values()) / COIN:.8f} NEB")
    for a, b in sorted(ledger["balances"].items(), key=lambda kv: -kv[1]):
        print(f"  {a}  {b / COIN:.8f} NEB{'  (due)' if a in due else ''}")
    if args.pay and kl.hold(ledger, ledger_path):
        save()
        return kl.HOLD_EXIT
    if not args.pay or not due:
        save()
        return 0

    available = wallet_confirmed(args)
    if available is None:
        print("could not read the wallet balance; nothing paid")
        return 1
    available -= int(args.reserve * COIN)
    last_txid = None
    for addr, amount in sorted(due.items(), key=lambda kv: -kv[1]):
        # neb-wallet picks coins from the confirmed chain only: wait until the previous payment
        # left the mempool, or it would pick the same coins again and the node would refuse it.
        if last_txid and not wait_confirmed(args, last_txid):
            print("previous payment still unconfirmed after the wait; the rest is paid next run")
            break
        if amount > available:
            print(f"wallet has {available / COIN:.8f} NEB spendable, not enough for {addr}; stopping")
            break
        text = f"{amount / COIN:.8f}"
        # Pending before send: recorded (balance taken off) and saved first, so a crash after the send cannot pay twice.
        intent = kl.new_intent()
        kl.add_intent(ledger, intent, addr, amount, height_sent=tip)
        save()
        p = subprocess.run([args.wallet_bin, "--rpc", args.rpc, "--wallet", args.wallet, "send", addr, text],
                           capture_output=True, text=True, timeout=300)
        out = (p.stdout + p.stderr).strip()
        if p.returncode != 0:
            kl.drop_intent(ledger, intent)
            save()
            print(f"payment to {addr} failed: {out[-300:]}")
            break
        txid = (re.findall(r"\b[0-9a-f]{64}\b", out) or ["?"])[-1]
        kl.mark_sent(ledger, intent, txid, height_sent=int(rpc(args.rpc, "getblockcount")))
        available -= amount + COIN // 10_000  # the wallet's fixed fee
        last_txid = txid
        save()
        print(f"paid {text} NEB to {addr}: {txid}")
    save()
    return 0


if __name__ == "__main__":
    sys.exit(main())
