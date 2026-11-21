"""Exact checks of payout.py's crediting: per-block fees, solo blocks, PPLNS without solo shares,
orphans and refused blocks. A fake node answers the RPC calls; nothing is paid (no --pay).

    python -m unittest test_payout
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import payout

REWARD = 1_900_000_000  # 19 NEB: 20 NEB less the 5 % project share, heights below the first halving


class FakeNode:
    def __init__(self, tip, chain):
        self.tip, self.chain = tip, chain  # chain: height -> block id on the best chain

    def rpc(self, url, method, *params):
        if method == "getblockcount":
            return self.tip
        if method == "getblockhash":
            return self.chain.get(params[0], "f" * 64)
        if method == "getrawmempool":
            return []
        raise AssertionError(f"unexpected RPC {method}")


def run_payout(data: Path, *extra):
    argv = ["payout.py", "--data", str(data), "--window", "2", *extra]
    with mock.patch.object(sys, "argv", argv):
        assert payout.main() == 0
    return json.loads((data / "payouts.json").read_text())


class PayoutTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.data = Path(self.tmp.name)
        found = lambda bid, h, ts, finder, **kw: {"event": "found", "id": bid, "height": h, "ts": ts,
                                                  "finder": finder, "block_work": 1000.0, **kw}
        result = lambda bid, ok=True: {"event": "result", "id": bid, "accepted": ok}
        blocks = [
            found("a" * 64, 10, 1000, "alice.rig", mode="pplns", fee_bp=0), result("a" * 64),
            found("b" * 64, 20, 2000, "carol.gpu", mode="pplns", fee_bp=200), result("b" * 64),
            found("c" * 64, 25, 2500, "bob.solo1", mode="solo", fee_bp=150), result("c" * 64),
            found("d" * 64, 30, 3000, "alice.rig"), result("d" * 64),  # old record: no mode/fee_bp
            found("e" * 64, 40, 3500, "alice.rig", mode="pplns", fee_bp=200), result("e" * 64),  # orphaned
            found("9" * 64, 45, 3600, "carol.gpu", mode="pplns", fee_bp=200), result("9" * 64, False),  # refused
        ]
        (d / "blocks.jsonl").write_text("\n".join(json.dumps(b) for b in blocks) + "\n")
        sh = lambda ts, addr, work, mode="pplns": {"ts": ts, "addr": addr, "worker": "w", "work": work, "mode": mode}
        shares = [sh(900, "alice", 300), sh(950, "carol", 100), sh(1900, "bob", 1000, "solo"),
                  sh(1900, "alice", 100), sh(1950, "carol", 300)]
        (d / "shares.jsonl").write_text("\n".join(json.dumps(s) for s in shares) + "\n")
        chain = {10: "a" * 64, 20: "b" * 64, 25: "c" * 64, 30: "d" * 64, 40: "0" * 64, 45: "9" * 64}
        self.node = FakeNode(200, chain)
        self.patch = mock.patch.object(payout, "rpc", self.node.rpc)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_per_block_fees_solo_and_pplns(self):
        led = run_payout(self.data, "--fee", "1.0")
        cr = led["credited"]
        self.assertEqual(cr["a" * 64]["fee"], 0)                      # 0 % when found
        self.assertEqual(cr["b" * 64]["fee"], REWARD * 200 // 10_000)  # 2 % when found
        self.assertEqual(cr["c" * 64]["fee"], REWARD * 150 // 10_000)  # solo 1.5 %
        self.assertEqual(cr["d" * 64]["fee"], REWARD * 100 // 10_000)  # old record: --fee 1.0
        self.assertEqual(cr["c" * 64]["mode"], "solo")
        self.assertIn("e" * 64, led["orphaned"])
        self.assertNotIn("9" * 64, cr)                                 # refused by the node
        b = led["balances"]
        # a: 0 % over alice 300 / carol 100. b and d: alice 400 / carol 400 (bob's solo share excluded).
        self.assertEqual(b["alice"], 1_425_000_000 + 931_000_000 + 940_500_000)
        self.assertEqual(b["carol"], 475_000_000 + 931_000_000 + 940_500_000)
        self.assertEqual(b["bob"], REWARD - REWARD * 150 // 10_000)    # the whole solo block, less 1.5 %
        self.assertEqual(led["pool_fees"], 0 + 38_000_000 + 28_500_000 + 19_000_000)
        # Every credited base unit is accounted for: balances + fees = rewards.
        self.assertEqual(sum(b.values()) + led["pool_fees"], 4 * REWARD)

    def test_fee_change_later_does_not_touch_credited_blocks(self):
        first = run_payout(self.data, "--fee", "1.0")
        second = run_payout(self.data, "--fee", "5.0")  # a later run with another fallback fee
        self.assertEqual(first["balances"], second["balances"])
        self.assertEqual(first["credited"], second["credited"])

    def test_immature_blocks_wait(self):
        self.node.tip = 115  # 100 confirmations reached for heights <= 15 only
        led = run_payout(self.data)
        self.assertEqual(set(led["credited"]), {"a" * 64})


if __name__ == "__main__":
    unittest.main()
