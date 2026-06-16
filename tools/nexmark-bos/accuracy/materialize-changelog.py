#!/usr/bin/env python3
"""Materialize and compare NexMark print-sink changelog captures.

Input lines are produced from Flink's print connector and look like:

    +I[1, foo, 2026-01-01T00:00]
    3> -U[1, foo, 2026-01-01T00:00]

The script writes:
  - raw-changelog.csv: row_kind,payload
  - final-materialized.csv: multiplicity,payload

For comparisons, +I/+U increment a row counter and -U/-D decrement it. The final
materialized multiset is compared exactly, except q12 where processing-time
windowing is expected to be wall-clock dependent.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

KINDS = {"+I", "+U", "-U", "-D"}
PREFIX = re.compile(r"^\d+> ")


def parse_print_line(line: str) -> tuple[str, str] | None:
    line = PREFIX.sub("", line.rstrip("\n"))
    if len(line) < 3:
        return None
    kind = line[:2]
    if kind not in KINDS:
        return None
    return kind, line[2:]


def load_print(path: Path) -> tuple[list[tuple[str, str]], Counter[str], int]:
    raw: list[tuple[str, str]] = []
    materialized: Counter[str] = Counter()
    negative_seen = 0

    with path.open(errors="replace") as f:
        for line in f:
            parsed = parse_print_line(line)
            if parsed is None:
                continue
            kind, payload = parsed
            raw.append((kind, payload))
            if kind in {"+I", "+U"}:
                materialized[payload] += 1
            else:
                materialized[payload] -= 1
                if materialized[payload] < 0:
                    negative_seen += 1

    return raw, Counter({k: v for k, v in materialized.items() if v}), negative_seen


def write_raw(rows: list[tuple[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["row_kind", "payload"])
        for kind, payload in rows:
            w.writerow([kind, payload])


def write_final(rows: Counter[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["multiplicity", "payload"])
        for payload in sorted(rows):
            w.writerow([rows[payload], payload])


def digest(rows: Counter[str]) -> str:
    h = hashlib.sha256()
    for payload in sorted(rows):
        h.update(f"{rows[payload]}\t{payload}\n".encode())
    return h.hexdigest()


def q12_stats(rows: Counter[str]) -> tuple[int, set[str], int]:
    total = 0
    bidders: set[str] = set()
    malformed = 0
    for payload, multiplicity in rows.items():
        m = re.match(r"^\[([^,]+),\s*([^,]+),\s*([^,]+),\s*([^\]]+)\]$", payload)
        if not m:
            malformed += multiplicity
            continue
        try:
            total += int(m.group(2).strip()) * multiplicity
        except ValueError:
            malformed += multiplicity
            continue
        bidders.add(m.group(1).strip())
    return total, bidders, malformed


def normalize_ts(value: str) -> str:
    value = value.strip().strip('"').replace("T", " ")
    if "." not in value:
        return value
    head, frac = value.split(".", 1)
    frac = (frac + "000")[:3].rstrip("0")
    return head if not frac else f"{head}.{frac}"


def normalize_field(value: str) -> str:
    return value.strip().strip('"')


def iter_csv_dir(path: Path):
    for file in sorted(path.iterdir()):
        if not file.is_file():
            continue
        with file.open(newline="", errors="replace") as f:
            yield from csv.reader(f)


def q9_expected_winners(csv_dir: Path) -> tuple[dict[str, set[tuple[str, str, str, str]]], int]:
    auctions: dict[str, list[str]] = {}
    for row in iter_csv_dir(csv_dir / "auction"):
        if len(row) != 10:
            raise ValueError(f"bad q9 auction row width: {len(row)}")
        auctions[row[0]] = row

    bids_by_auction: dict[str, list[list[str]]] = defaultdict(list)
    for row in iter_csv_dir(csv_dir / "bid"):
        if len(row) != 7:
            raise ValueError(f"bad q9 bid row width: {len(row)}")
        bids_by_auction[row[0]].append(row)

    expected: dict[str, set[tuple[str, str, str, str]]] = {}
    ambiguous = 0
    for auction_id, auction in auctions.items():
        candidates = [
            bid
            for bid in bids_by_auction.get(auction_id, [])
            if normalize_ts(auction[5]) <= normalize_ts(bid[5]) <= normalize_ts(auction[6])
        ]
        if not candidates:
            continue
        best_price = max(int(bid[2]) for bid in candidates)
        best_time = min(normalize_ts(bid[5]) for bid in candidates if int(bid[2]) == best_price)
        winners = [
            bid
            for bid in candidates
            if int(bid[2]) == best_price and normalize_ts(bid[5]) == best_time
        ]
        if len(winners) > 1:
            ambiguous += 1
        expected[auction_id] = {
            (normalize_field(bid[1]), normalize_field(bid[2]), normalize_ts(bid[5]), normalize_field(bid[6]))
            for bid in winners
        }
    return expected, ambiguous


def q9_output_winners(path: Path) -> tuple[dict[str, tuple[str, str, str, str]], Counter[str], int]:
    raw, _, negative_seen = load_print(path)
    by_auction: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    kinds: Counter[str] = Counter()
    for kind, payload in raw:
        kinds[kind] += 1
        if payload.startswith("[") and payload.endswith("]"):
            payload = payload[1:-1]
        fields = payload.split(", ")
        if len(fields) != 15:
            raise ValueError(f"bad q9 output row width: {len(fields)}")
        by_auction[fields[0]].append((kind, fields))

    winners: dict[str, tuple[str, str, str, str]] = {}
    for auction_id, rows in by_auction.items():
        latest = rows[-1][1]
        winners[auction_id] = (
            normalize_field(latest[11]),
            normalize_field(latest[12]),
            normalize_ts(latest[13]),
            normalize_field(latest[14]),
        )
    return winners, kinds, negative_seen


def q9_compare(args: argparse.Namespace) -> int:
    if not args.csv_dir:
        raise ValueError("--csv-dir is required for q9 comparison")
    expected, ambiguous = q9_expected_winners(Path(args.csv_dir))
    left_winners, left_kinds, left_negative = q9_output_winners(Path(args.left))
    right_winners, right_kinds, right_negative = q9_output_winners(Path(args.right))

    def validate(actual: dict[str, tuple[str, str, str, str]]) -> tuple[list[str], list[str], list[str]]:
        missing = [auction_id for auction_id in expected if auction_id not in actual]
        extra = [auction_id for auction_id in actual if auction_id not in expected]
        wrong = [
            auction_id
            for auction_id, winner in actual.items()
            if auction_id in expected and winner not in expected[auction_id]
        ]
        return missing, extra, wrong

    left_missing, left_extra, left_wrong = validate(left_winners)
    right_missing, right_extra, right_wrong = validate(right_winners)
    cross_diff = [
        auction_id
        for auction_id in sorted(set(left_winners) | set(right_winners))
        if left_winners.get(auction_id) != right_winners.get(auction_id)
    ]
    ok = (
        not left_missing
        and not left_extra
        and not left_wrong
        and not right_missing
        and not right_extra
        and not right_wrong
        and left_negative == 0
        and right_negative == 0
        and (
            not cross_diff
            or all(
                left_winners.get(auction_id) in expected.get(auction_id, set())
                and right_winners.get(auction_id) in expected.get(auction_id, set())
                for auction_id in cross_diff
            )
        )
    )
    verdict = "PASS_FINAL_WINNER" if ok else "FAIL_FINAL_WINNER"
    print(
        "COMPARE_TSV\t"
        f"{args.query}\t{verdict}\texpected_auctions={len(expected)}"
        f"\tambiguous_order_keys={ambiguous}\tleft_auctions={len(left_winners)}"
        f"\tright_auctions={len(right_winners)}\tcross_diff={len(cross_diff)}"
        f"\tleft_missing={len(left_missing)}\tleft_extra={len(left_extra)}"
        f"\tleft_wrong={len(left_wrong)}\tright_missing={len(right_missing)}"
        f"\tright_extra={len(right_extra)}\tright_wrong={len(right_wrong)}"
        f"\tleft_neg={left_negative}\tright_neg={right_negative}"
        f"\tleft_raw={sum(left_kinds.values())}\tright_raw={sum(right_kinds.values())}"
    )
    if not ok:
        print(f"left_wrong_sample={left_wrong[:10]}")
        print(f"right_wrong_sample={right_wrong[:10]}")
        print(f"cross_diff_sample={cross_diff[:10]}")
    return 0 if ok else 1


def materialize(args: argparse.Namespace) -> int:
    raw, final, negative_seen = load_print(Path(args.input))
    write_raw(raw, Path(args.raw_csv))
    write_final(final, Path(args.final_csv))
    print(
        "MATERIALIZE_TSV\t"
        f"input={args.input}\traw_rows={len(raw)}\tfinal_rows={sum(final.values())}"
        f"\tdistinct={len(final)}\tnegative_seen={negative_seen}\tsha256={digest(final)}"
    )
    return 0


def compare(args: argparse.Namespace) -> int:
    if args.query == "q9":
        return q9_compare(args)

    _, left, left_negative = load_print(Path(args.left))
    _, right, right_negative = load_print(Path(args.right))
    left_hash = digest(left)
    right_hash = digest(right)
    all_keys = set(left) | set(right)
    diff_rows = sum(abs(left.get(k, 0) - right.get(k, 0)) for k in all_keys)

    if args.query == "q12":
        left_total, left_bidders, left_malformed = q12_stats(left)
        right_total, right_bidders, right_malformed = q12_stats(right)
        ok = (
            left_total == args.expected_bids
            and right_total == args.expected_bids
            and left_bidders == right_bidders
            and left_malformed == 0
            and right_malformed == 0
        )
        verdict = "PASS_INVARIANT" if ok else "FAIL_INVARIANT"
        print(
            "COMPARE_TSV\t"
            f"{args.query}\t{verdict}\tleft_rows={sum(left.values())}"
            f"\tright_rows={sum(right.values())}\tdiff_rows={diff_rows}"
            f"\tleft_sha={left_hash[:16]}\tright_sha={right_hash[:16]}"
            f"\tleft_neg={left_negative}\tright_neg={right_negative}"
            f"\texpected_bids={args.expected_bids}\tleft_bid_sum={left_total}"
            f"\tright_bid_sum={right_total}\tleft_bidders={len(left_bidders)}"
            f"\tright_bidders={len(right_bidders)}"
        )
        return 0 if ok else 1

    ok = diff_rows == 0 and left_negative == 0 and right_negative == 0
    verdict = "EQUAL" if ok else "DIFF"
    print(
        "COMPARE_TSV\t"
        f"{args.query}\t{verdict}\tleft_rows={sum(left.values())}"
        f"\tright_rows={sum(right.values())}\tdiff_rows={diff_rows}"
        f"\tleft_sha={left_hash[:16]}\tright_sha={right_hash[:16]}"
        f"\tleft_neg={left_negative}\tright_neg={right_negative}"
    )

    if not ok:
        left_only = [(k, left[k] - right.get(k, 0)) for k in left if left[k] != right.get(k, 0)]
        right_only = [(k, right[k] - left.get(k, 0)) for k in right if right[k] != left.get(k, 0)]
        print(f"--- over-represented in left ({len(left_only)} distinct, sample 20) ---")
        for payload, delta in sorted(left_only)[:20]:
            print(f"left+{delta}: {payload}")
        print(f"--- over-represented in right ({len(right_only)} distinct, sample 20) ---")
        for payload, delta in sorted(right_only)[:20]:
            print(f"right+{delta}: {payload}")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_mat = sub.add_parser("materialize")
    p_mat.add_argument("--input", required=True)
    p_mat.add_argument("--raw-csv", required=True)
    p_mat.add_argument("--final-csv", required=True)
    p_mat.set_defaults(func=materialize)

    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("--query", required=True)
    p_cmp.add_argument("--left", required=True)
    p_cmp.add_argument("--right", required=True)
    p_cmp.add_argument("--csv-dir")
    p_cmp.add_argument("--expected-bids", type=int, default=92000)
    p_cmp.set_defaults(func=compare)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
