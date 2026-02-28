#!/usr/bin/env python3
"""
codex 5.3 concocted script for running fsx-linux to try and find a minimal
series of operations that produce a bug

Find the fsx-linux seed that triggers failure in the fewest operations.

Strategy:
- Iterate seeds from --seed-start upward.
- Run fsx-linux with current -N cap (initially user-provided).
- If failure occurs, parse "LOG DUMP (X total operations)" to get ops-to-fail.
- Track best (fewest ops). When a new best is found, tighten -N to that value
  (optionally minus a small margin) to reduce work on subsequent runs.
- Stop when --max-seeds checked, or if --stop-at-ops reached, etc.

Notes:
- Assumes fsx-linux prints either:
    "All operations completed A-OK!"
  or failure output containing:
    "LOG DUMP (NNN total operations):"
- Uses a fresh testfile per run (default behavior: deletes it first).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional, Tuple

OK_RE = re.compile(r"All operations completed A-OK!", re.IGNORECASE)
FAIL_OPS_RE = re.compile(r"LOG DUMP \((\d+)\s+total operations\):", re.IGNORECASE)
SEED_RE = re.compile(r"Seed set to\s+(\d+)", re.IGNORECASE)

@dataclass
class RunResult:
    seed: int
    ok: bool
    ops: Optional[int]          # ops-to-fail if failed, else None
    stdout: str                 # combined output
    timed_out: bool = False

def run_once(
    fsx_path: str,
    testfile: str,
    seed: int,
    length: int,
    maxop: int,
    nops: int,
    extra_args: list[str],
    timeout_s: Optional[float],
    delete_testfile: bool,
) -> RunResult:
    if delete_testfile:
        try:
            os.remove(testfile)
        except FileNotFoundError:
            pass

    cmd = [
        fsx_path,
        "-l", str(length),
        "-o", str(maxop),
        "-N", str(nops),
        "-S", str(seed),
    ]
    cmd += extra_args
    cmd.append(testfile)

    try:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        out = p.stdout or ""
        ok = bool(OK_RE.search(out))
        if ok:
            return RunResult(seed=seed, ok=True, ops=None, stdout=out, timed_out=False)

        m = FAIL_OPS_RE.search(out)
        ops = int(m.group(1)) if m else None
        return RunResult(seed=seed, ok=False, ops=ops, stdout=out, timed_out=False)

    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") if isinstance(e.stdout, str) else ""
        return RunResult(seed=seed, ok=False, ops=None, stdout=out, timed_out=True)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fsx", default="./fsx-linux", help="Path to fsx-linux binary")
    ap.add_argument("--testfile", default="testfile", help="Test file path")
    ap.add_argument("--seed-start", type=int, default=0)
    ap.add_argument("--max-seeds", type=int, default=10_000)
    ap.add_argument("--timeout", type=float, default=None, help="Per-run timeout seconds")
    ap.add_argument("--delete-testfile", action="store_true", default=True,
                    help="Delete testfile before each run (default: true)")
    ap.add_argument("--no-delete-testfile", dest="delete_testfile", action="store_false")

    # fsx knobs (matching your example)
    ap.add_argument("-l", "--length", type=int, default=131072)
    ap.add_argument("-o", "--maxop", type=int, default=16384)
    ap.add_argument("-N", "--nops", type=int, default=512)

    # tighten behavior
    ap.add_argument("--shrink-to-best", action="store_true", default=True,
                    help="Reduce -N after finding a best seed (default: true)")
    ap.add_argument("--no-shrink-to-best", dest="shrink_to_best", action="store_false")
    ap.add_argument("--shrink-margin", type=int, default=0,
                    help="Subtract this many ops from best when setting new -N (default: 0).")
    ap.add_argument("--stop-if-ops-le", type=int, default=None,
                    help="Stop once a failure is found with ops <= this value")

    # pass-through fsx flags: your common set: -W -R (and optionally -m to enable mmap, etc.)
    ap.add_argument("--extra", nargs=argparse.REMAINDER, default=[],
                    help="Extra args passed to fsx-linux (e.g. --extra -W -R)")

    args = ap.parse_args()

    fsx_path = shutil.which(args.fsx) or args.fsx
    if not os.path.exists(fsx_path):
        print(f"error: fsx-linux not found: {fsx_path}", file=sys.stderr)
        return 2

    best_seed: Optional[int] = None
    best_ops: Optional[int] = None
    current_nops = args.nops

    print(f"fsx: {fsx_path}")
    print(f"testfile: {args.testfile}")
    print(f"seed range: [{args.seed_start}, {args.seed_start + args.max_seeds})")
    print(f"initial: -l {args.length} -o {args.maxop} -N {current_nops}")
    if args.extra:
        print(f"extra fsx args: {' '.join(args.extra)}")
    print()

    for i in range(args.max_seeds):
        seed = args.seed_start + i
        rr = run_once(
            fsx_path=fsx_path,
            testfile=args.testfile,
            seed=seed,
            length=args.length,
            maxop=args.maxop,
            nops=current_nops,
            extra_args=args.extra,
            timeout_s=args.timeout,
            delete_testfile=args.delete_testfile,
        )

        if rr.timed_out:
            print(f"[seed {seed}] TIMEOUT (N={current_nops})")
            continue

        if rr.ok:
            print(f"[seed {seed}] OK (N={current_nops})")
            continue

        # failure
        ops_str = str(rr.ops) if rr.ops is not None else "?"
        print(f"[seed {seed}] FAIL at ops={ops_str} (N={current_nops})")

        if rr.ops is None:
            # If we can't parse ops, keep going but save output to inspect
            out_path = f"fail.seed{seed}.log"
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(rr.stdout)
            print(f"  could not parse ops; wrote full output to {out_path}")
            continue

        if best_ops is None or rr.ops < best_ops:
            best_ops = rr.ops
            best_seed = seed

            # Save output for the best failure
            out_path = f"best.seed{seed}.ops{best_ops}.log"
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(rr.stdout)

            print(f"  NEW BEST: seed={best_seed}, ops={best_ops} (saved {out_path})")

            if args.shrink_to_best:
                new_nops = max(1, best_ops - args.shrink_margin)
                if new_nops < current_nops:
                    current_nops = new_nops
                    print(f"  tightening: now using -N {current_nops}")

            if args.stop_if_ops_le is not None and best_ops <= args.stop_if_ops_le:
                print(f"\nStopping: best ops {best_ops} <= stop threshold {args.stop_if_ops_le}")
                break

    print("\nBest result:")
    if best_seed is None:
        print("  no failures found (unexpected based on your description)")
        return 1
    print(f"  seed = {best_seed}")
    print(f"  ops  = {best_ops}")
    print(f"  suggested repro command:")
    extra = " ".join(args.extra) + " " if args.extra else ""
    print(f"    {fsx_path} -l {args.length} -o {args.maxop} -N {best_ops} -S {best_seed} {extra}{args.testfile}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
