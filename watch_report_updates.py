#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path


BASE_DIR = Path("/Users/nguyencan/Library/CloudStorage/OneDrive-TARA/Order Haravan")
MAPPING_FILE = Path("/Users/nguyencan/Downloads/Copy of list-sp-hien-website.xlsx")
GENERATE_SCRIPT = BASE_DIR / "generate_report.py"
LOG_FILE = BASE_DIR / "report_watcher.log"
POLL_SECONDS = 5


def watched_files():
    files = sorted(BASE_DIR.glob("Orders_T*_20*.xlsx"))
    if MAPPING_FILE.exists():
        files.append(MAPPING_FILE)
    files.append(GENERATE_SCRIPT)
    return files


def snapshot(paths):
    state = {}
    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError:
            state[str(path)] = None
            continue
        state[str(path)] = (stat.st_mtime_ns, stat.st_size)
    return state


def log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}\n"
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line)
    print(line, end="")


def generate():
    log("Change detected, regenerating report...")
    result = subprocess.run(
        [sys.executable, str(GENERATE_SCRIPT)],
        cwd=str(BASE_DIR),
        text=True,
        capture_output=True,
    )
    if result.stdout.strip():
        log(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr.strip():
            log(result.stderr.strip())
        return False
    return True


def main():
    log("Watcher started.")
    paths = watched_files()
    previous = snapshot(paths)
    while True:
        time.sleep(POLL_SECONDS)
        paths = watched_files()
        current = snapshot(paths)
        if current != previous:
            generate()
            previous = current


if __name__ == "__main__":
    main()
