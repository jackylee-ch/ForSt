#!/usr/bin/env python3
"""Deterministic compare of two print-sink row captures (forst-rs vs rocksdb).

Each input file holds lines like `+I[a, b, c]` (optional `N> ` subtask prefix,
kinds +I/+U/-U/-D). Rows are MATERIALIZED by changelog semantics (+I/+U add,
-U/-D subtract) into multiset counters, then compared exactly; the verdict
hash is the sha256 of the sorted (count, row) stream.

q12 is PROCTIME-windowed (wall-clock nondeterministic by design): instead of
row equality it checks the invariant sum(bid_count) == EXPECTED_BIDS on BOTH
sides plus identical bidder sets.

Usage: compare-print.py <query> <frs_file> <rdb_file> [expected_bids=920000]
Exit 0 = EQUAL/PASS, 1 = DIFF/FAIL. Emits COMPARE_TSV line + diff samples.
"""
import hashlib
import re
import sys
from collections import Counter

KINDS = ("+I", "+U", "-U", "-D")
PREFIX = re.compile(r"^\d+> ")


def read(path):
    c = Counter()
    raw = 0
    neg = 0
    with open(path, errors="replace") as f:
        for line in f:
            line = PREFIX.sub("", line.rstrip("\n"))
            if len(line) < 3 or line[:2] not in KINDS:
                continue
            kind, row = line[:2], line[2:]
            raw += 1
            if kind in ("+I", "+U"):
                c[row] += 1
            else:
                c[row] -= 1
                if c[row] < 0:
                    neg += 1
    return Counter({k: v for k, v in c.items() if v}), raw, neg


def sha(c):
    h = hashlib.sha256()
    for row in sorted(c):
        h.update(f"{c[row]}\t{row}\n".encode())
    return h.hexdigest()


def q12_stats(c, label):
    total = 0
    bidders = set()
    malformed = 0
    for row, mul in c.items():
        m = re.match(r"^\[([^,]+),\s*([^,]+),\s*([^,]+),\s*([^\]]+)\]$", row)
        if not m:
            malformed += mul
            continue
        try:
            total += int(m.group(2).strip()) * mul
        except ValueError:
            malformed += mul
            continue
        bidders.add(m.group(1).strip())
    print(f"q12[{label}]: sum_bid_count={total} unique_bidders={len(bidders)} malformed={malformed}")
    return total, bidders, malformed


def main():
    query, frs_path, rdb_path = sys.argv[1:4]
    expected_bids = int(sys.argv[4]) if len(sys.argv) > 4 else 920000
    fc, fraw, fneg = read(frs_path)
    rc, rraw, rneg = read(rdb_path)
    fh, rh = sha(fc), sha(rc)
    diff_rows = sum(abs(fc.get(k, 0) - rc.get(k, 0)) for k in set(fc) | set(rc))

    if query == "q12":
        ft, fb, fm = q12_stats(fc, "frs")
        rt, rb, rm = q12_stats(rc, "rdb")
        ok = ft == expected_bids and rt == expected_bids and fb == rb and fm == 0 and rm == 0
        verdict = "PASS_INVARIANT" if ok else "FAIL_INVARIANT"
        note = f"proctime_invariant_sum={expected_bids}"
    else:
        ok = diff_rows == 0 and fneg == 0 and rneg == 0
        verdict = "EQUAL" if ok else "DIFF"
        note = "materialized_hash"

    print(
        f"COMPARE_TSV\t{query}\t{verdict}\tfrs_rows={sum(fc.values())}\trdb_rows={sum(rc.values())}"
        f"\tdiff_rows={diff_rows}\tfrs_sha={fh[:16]}\trdb_sha={rh[:16]}"
        f"\tfrs_raw={fraw}\trdb_raw={rraw}\tfrs_neg={fneg}\trdb_neg={rneg}\t{note}"
    )
    if not ok and query != "q12":
        only_f = [(k, fc[k] - rc.get(k, 0)) for k in fc if fc[k] != rc.get(k, 0)]
        only_r = [(k, rc[k] - fc.get(k, 0)) for k in rc if rc[k] != fc.get(k, 0)]
        print(f"--- rows over-represented in forst-rs ({len(only_f)} distinct, sample 20) ---")
        for k, d in sorted(only_f)[:20]:
            print(f"  frs+{d}: {k}")
        print(f"--- rows over-represented in rocksdb ({len(only_r)} distinct, sample 20) ---")
        for k, d in sorted(only_r)[:20]:
            print(f"  rdb+{d}: {k}")
    if not ok and query == "q12":
        fb_only = sorted(fb - rb)[:10]
        rb_only = sorted(rb - fb)[:10]
        print(f"bidders only in frs (sample): {fb_only}")
        print(f"bidders only in rdb (sample): {rb_only}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
