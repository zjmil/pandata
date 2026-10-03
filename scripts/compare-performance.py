#!/usr/bin/env python3
"""Validate measurements and apply a strict, confirmed 15% regression policy."""
import argparse
import json
import math
import statistics
from decimal import Decimal
from pathlib import Path

CASES = tuple(f'{a}-to-{b}' for a in ('csv', 'json', 'parquet')
              for b in ('csv', 'json', 'parquet') if a != b)


def load(path, cases):
    data = json.loads(Path(path).read_text())
    if data['rows'] != 100_000 or set(data['cases']) != set(cases):
        raise ValueError(f'{path}: unexpected rows or cases')
    for case, result in data['cases'].items():
        samples = result['samples_seconds']
        values = [*samples, result['median_seconds'], result['rows_per_second']]
        if len(samples) != 15 or any(type(v) not in (int, float) or
                                    not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError(f'{path}: invalid measurements for {case}')
        if not math.isclose(statistics.median(samples), result['median_seconds'], rel_tol=1e-12):
            raise ValueError(f'{path}: inconsistent median for {case}')
        if not math.isclose(100_000 / result['median_seconds'], result['rows_per_second'], rel_tol=1e-12):
            raise ValueError(f'{path}: inconsistent throughput for {case}')
    return data['cases']


def slow(base, head):
    return Decimal(str(head)) > Decimal(str(base)) * Decimal('1.15')


def compare(base, head, cases):
    print(f'{"Case":20} {"Base (s)":>12} {"Head (s)":>12} {"Change":>10}')
    failures = []
    for case in cases:
        before, after = base[case]['median_seconds'], head[case]['median_seconds']
        print(f'{case:20} {before:12.6f} {after:12.6f} {(after / before - 1):+10.2%}')
        if slow(before, after):
            failures.append(case)
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base')
    parser.add_argument('head')
    parser.add_argument('--candidates', required=True)
    parser.add_argument('--confirmation-base')
    parser.add_argument('--confirmation-head')
    args = parser.parse_args()
    failures = compare(load(args.base, CASES), load(args.head, CASES), CASES)
    Path(args.candidates).write_text(','.join(failures))
    if bool(args.confirmation_base) != bool(args.confirmation_head):
        raise ValueError('Both confirmation files are required')
    if args.confirmation_base:
        confirmed = compare(load(args.confirmation_base, failures),
                            load(args.confirmation_head, failures), failures)
        if confirmed:
            print('Persistent regressions: ' + ', '.join(confirmed))
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
