#!/usr/bin/env python3
"""Run a command and sample memory for the whole Linux process tree.

The primary metric is total PSS (proportional set size) across the process tree.
Unlike summing RSS, this does not fully double-count pages shared by Mori workers.
Uses only Python's standard library and Linux /proc.
"""

import argparse
import os
import subprocess
import sys
import time


def process_table():
    table = {}
    for name in os.listdir('/proc'):
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            with open(f'/proc/{pid}/status', 'r', encoding='utf-8', errors='replace') as f:
                ppid = None
                for line in f:
                    if line.startswith('PPid:'):
                        ppid = int(line.split()[1])
                        break
            if ppid is not None:
                table[pid] = ppid
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            pass
    return table


def descendants(root_pid, table):
    children = {}
    for pid, ppid in table.items():
        children.setdefault(ppid, []).append(pid)

    out = {root_pid}
    stack = [root_pid]
    while stack:
        p = stack.pop()
        for child in children.get(p, []):
            if child not in out:
                out.add(child)
                stack.append(child)
    return out


def read_rollup_kb(pid):
    pss = None
    rss = None
    try:
        with open(f'/proc/{pid}/smaps_rollup', 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if line.startswith('Pss:'):
                    pss = int(line.split()[1])
                elif line.startswith('Rss:'):
                    rss = int(line.split()[1])
                if pss is not None and rss is not None:
                    break
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
        return 0, 0
    return pss or 0, rss or 0


def sample_tree(root_pid):
    table = process_table()
    pids = descendants(root_pid, table)
    total_pss = 0
    total_rss = 0
    alive = 0
    for pid in pids:
        pss, rss = read_rollup_kb(pid)
        if pss or rss:
            alive += 1
            total_pss += pss
            total_rss += rss
    return total_pss, total_rss, alive


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics', required=True)
    parser.add_argument('--interval', type=float, default=0.20)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    ns = parser.parse_args()

    cmd = ns.command
    if cmd and cmd[0] == '--':
        cmd = cmd[1:]
    if not cmd:
        parser.error('a command is required after --')

    start = time.monotonic()
    proc = subprocess.Popen(cmd)

    peak_pss = 0
    peak_rss_sum = 0
    peak_processes = 0

    while True:
        pss, rss, nproc = sample_tree(proc.pid)
        peak_pss = max(peak_pss, pss)
        peak_rss_sum = max(peak_rss_sum, rss)
        peak_processes = max(peak_processes, nproc)

        rc = proc.poll()
        if rc is not None:
            break
        time.sleep(ns.interval)

    wall = time.monotonic() - start

    os.makedirs(os.path.dirname(os.path.abspath(ns.metrics)), exist_ok=True)
    with open(ns.metrics, 'w', encoding='utf-8') as f:
        f.write(f'wall_sec={wall:.6f}\n')
        f.write(f'peak_tree_pss_kb={peak_pss}\n')
        f.write(f'peak_tree_rss_sum_kb={peak_rss_sum}\n')
        f.write(f'peak_processes={peak_processes}\n')
        f.write(f'exit_status={rc}\n')
        f.write(f'sample_interval_sec={ns.interval}\n')

    return rc


if __name__ == '__main__':
    sys.exit(main())
