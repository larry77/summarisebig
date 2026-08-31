#!/usr/bin/env python3
"""Run a command and record wall time only.

Mirai daemon processes can detach from the ordinary parent process tree.
Therefore this extended benchmark deliberately does not claim complete
multi-process memory accounting.  It records end-to-end elapsed time and
exit status only.
"""

import argparse
import os
import subprocess
import sys
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    ns = parser.parse_args()

    cmd = ns.command
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        parser.error("a command is required after --")

    start = time.monotonic()
    rc = subprocess.call(cmd)
    wall = time.monotonic() - start

    os.makedirs(os.path.dirname(os.path.abspath(ns.metrics)), exist_ok=True)

    with open(ns.metrics, "w", encoding="utf-8") as f:
        f.write(f"wall_sec={wall:.6f}\n")
        f.write(f"exit_status={rc}\n")

    return rc

if __name__ == "__main__":
    sys.exit(main())
