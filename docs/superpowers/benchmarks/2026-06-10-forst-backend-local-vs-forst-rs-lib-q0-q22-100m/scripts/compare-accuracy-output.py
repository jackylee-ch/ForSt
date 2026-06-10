#!/usr/bin/env python3
import argparse
import csv
import hashlib
import re
from collections import Counter
from pathlib import Path

KINDS = {'+I', '+U', '-U', '-D'}


def normalize_record(line: str):
    line = line.rstrip('\n')
    if not line:
        return None
    if '\t' in line:
        kind, row = line.split('\t', 1)
    else:
        kind, row = '+I', line
    if len(row) >= 3 and row[:2] in KINDS and row[2] == '[':
        row = row[2:]
    return kind, row


def digest_counter(counter: Counter) -> str:
    h = hashlib.sha256()
    for row in sorted(counter):
        cnt = counter[row]
        if cnt:
            h.update(str(cnt).encode())
            h.update(b'\t')
            h.update(row.encode())
            h.update(b'\n')
    return h.hexdigest()


def digest_raw(lines):
    h = hashlib.sha256()
    for line in sorted(lines):
        h.update(line.encode())
        h.update(b'\n')
    return h.hexdigest()


def read_output(root: Path, run_id: str, query: str):
    out_dir = root / 'results' / 'accuracy-output' / run_id / query
    lines = []
    files = sorted(out_dir.glob('*')) if out_dir.exists() else []
    for path in files:
        if not path.is_file():
            continue
        with path.open('r', errors='replace') as fh:
            for raw in fh:
                rec = normalize_record(raw)
                if rec is None:
                    continue
                kind, row = rec
                lines.append(f'{kind}\t{row}')
    mat = Counter()
    negative_events = 0
    for line in lines:
        kind, row = line.split('\t', 1)
        if kind in ('+I', '+U'):
            mat[row] += 1
        elif kind in ('-U', '-D'):
            mat[row] -= 1
        else:
            mat[row] += 1
        if mat[row] < 0:
            negative_events += 1
    mat = Counter({k: v for k, v in mat.items() if v != 0})
    return {
        'dir': str(out_dir),
        'files': len([p for p in files if p.is_file()]),
        'raw_count': len(lines),
        'raw_sha256': digest_raw(lines),
        'materialized_count': sum(mat.values()),
        'materialized_keys': len(mat),
        'materialized_sha256': digest_counter(mat),
        'negative_events': negative_events,
        'counter': mat,
    }




def q12_stats(counter: Counter):
    rows = 0
    sum_bid_count = 0
    bidders = set()
    windows = set()
    malformed = 0
    for row, multiplicity in counter.items():
        m = re.match(r"^\[([^,]+),\s*([^,]+),\s*([^,]+),\s*([^\]]+)\]$", row)
        if not m:
            malformed += multiplicity
            continue
        try:
            bid_count = int(m.group(2).strip())
        except ValueError:
            malformed += multiplicity
            continue
        rows += multiplicity
        sum_bid_count += bid_count * multiplicity
        bidders.add(m.group(1).strip())
        windows.add((m.group(3).strip(), m.group(4).strip()))
    bidder_digest = hashlib.sha256("\n".join(sorted(bidders)).encode()).hexdigest()
    return {
        'q12_rows': rows,
        'q12_sum_bid_count': sum_bid_count,
        'q12_unique_bidders': len(bidders),
        'q12_windows': len(windows),
        'q12_bidder_sha256': bidder_digest,
        'q12_malformed': malformed,
    }


def read_matrix(root: Path, label: str):
    matrix = root / 'results' / f'{label}.tsv'
    rows = {}
    if not matrix.exists():
        return rows
    with matrix.open(newline='') as fh:
        reader = csv.DictReader(fh, delimiter='\t')
        for row in reader:
            rows[(row.get('variant'), row.get('query'))] = row
    return rows


def diff_size(a: Counter, b: Counter) -> int:
    total = 0
    for key in set(a) | set(b):
        total += abs(a.get(key, 0) - b.get(key, 0))
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-root', default='/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610')
    ap.add_argument('--run-label', required=True)
    ap.add_argument('--queries', required=True, help='space separated query list')
    ap.add_argument('--variants', default='forst-local forst-rs-lib')
    ap.add_argument('--out')
    args = ap.parse_args()
    root = Path(args.work_root)
    queries = args.queries.split()
    variants = args.variants.split()
    if len(variants) != 2:
        raise SystemExit('compare expects exactly two variants')
    matrix = read_matrix(root, args.run_label)
    out = Path(args.out) if args.out else root / 'results' / f'{args.run_label}.accuracy-compare.tsv'
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        'run_label','query','status','diff_materialized_rows',
        f'{variants[0]}_mode', f'{variants[1]}_mode',
        f'{variants[0]}_raw_count', f'{variants[1]}_raw_count',
        f'{variants[0]}_materialized_count', f'{variants[1]}_materialized_count',
        f'{variants[0]}_materialized_keys', f'{variants[1]}_materialized_keys',
        f'{variants[0]}_materialized_sha256', f'{variants[1]}_materialized_sha256',
        f'{variants[0]}_negative_events', f'{variants[1]}_negative_events',
        f'{variants[0]}_native', f'{variants[1]}_native',
        f'{variants[0]}_q12_sum_bid_count', f'{variants[1]}_q12_sum_bid_count',
        f'{variants[0]}_q12_unique_bidders', f'{variants[1]}_q12_unique_bidders',
        f'{variants[0]}_q12_windows', f'{variants[1]}_q12_windows',
        f'{variants[0]}_q12_bidder_sha256', f'{variants[1]}_q12_bidder_sha256',
        'compare_note',
        f'{variants[0]}_dir', f'{variants[1]}_dir',
    ]
    with out.open('w', newline='') as fh:
        writer = csv.DictWriter(fh, delimiter='\t', fieldnames=fields)
        writer.writeheader()
        for q in queries:
            vals = {v: read_output(root, f'{args.run_label}-{v}-{q}', q) for v in variants}
            d = diff_size(vals[variants[0]]['counter'], vals[variants[1]]['counter'])
            modes = {v: matrix.get((v, q), {}).get('mode', 'NO_MATRIX') for v in variants}
            nats = {v: matrix.get((v, q), {}).get('native_loaded', 'NO_MATRIX') for v in variants}
            success_modes = {'SOURCE_PLATEAU', 'SOURCE_DONE', 'FINISHED'}
            compare_note = 'materialized_hash'
            q12 = {v: q12_stats(vals[v]['counter']) for v in variants} if q == 'q12' else {}
            if q == 'q12':
                expected_values = []
                for v in variants:
                    try:
                        expected_values.append(int(matrix.get((v, q), {}).get('expected_src', '0')))
                    except ValueError:
                        expected_values.append(0)
                expected = expected_values[0] if expected_values and all(x == expected_values[0] for x in expected_values) else 0
                status = 'PASS' if (
                    expected > 0
                    and all(modes[v] in success_modes for v in variants)
                    and all(vals[v]['negative_events'] == 0 for v in variants)
                    and all(q12[v]['q12_malformed'] == 0 for v in variants)
                    and all(q12[v]['q12_sum_bid_count'] == expected for v in variants)
                    and q12[variants[0]]['q12_bidder_sha256'] == q12[variants[1]]['q12_bidder_sha256']
                    and all(q12[v]['q12_rows'] > 0 for v in variants)
                ) else 'FAIL'
                compare_note = f'q12_proctime_invariant_expected_sum={expected}'
            else:
                status = 'PASS' if d == 0 and all(modes[v] in success_modes for v in variants) else 'FAIL'
            row = {
                'run_label': args.run_label,
                'query': q,
                'status': status,
                'diff_materialized_rows': d,
            }
            for v in variants:
                row[f'{v}_mode'] = modes[v]
                row[f'{v}_raw_count'] = vals[v]['raw_count']
                row[f'{v}_materialized_count'] = vals[v]['materialized_count']
                row[f'{v}_materialized_keys'] = vals[v]['materialized_keys']
                row[f'{v}_materialized_sha256'] = vals[v]['materialized_sha256']
                row[f'{v}_negative_events'] = vals[v]['negative_events']
                row[f'{v}_native'] = nats[v]
                if q == 'q12':
                    row[f'{v}_q12_sum_bid_count'] = q12[v]['q12_sum_bid_count']
                    row[f'{v}_q12_unique_bidders'] = q12[v]['q12_unique_bidders']
                    row[f'{v}_q12_windows'] = q12[v]['q12_windows']
                    row[f'{v}_q12_bidder_sha256'] = q12[v]['q12_bidder_sha256']
                else:
                    row[f'{v}_q12_sum_bid_count'] = ''
                    row[f'{v}_q12_unique_bidders'] = ''
                    row[f'{v}_q12_windows'] = ''
                    row[f'{v}_q12_bidder_sha256'] = ''
                row['compare_note'] = compare_note
                row[f'{v}_dir'] = vals[v]['dir']
            writer.writerow(row)
    print(out)

if __name__ == '__main__':
    main()
