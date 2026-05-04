#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path


BASE_DIR = Path("/Users/nguyencan/Library/CloudStorage/OneDrive-TARA/Order Haravan")
MAPPING_FILE = Path("/Users/nguyencan/Downloads/Copy of list-sp-hien-website.xlsx")
LOCAL_DIR = Path("/Users/nguyencan/Library/Application Support/OrderHaravanReport")
GENERATE_SCRIPTS = [
    LOCAL_DIR / "generate_report.py",
    LOCAL_DIR / "generate_hoang_anh_request.py",
]
LOG_FILE = BASE_DIR / "report_watcher.log"
PUBLISH_FILES = [
    BASE_DIR / "index.html",
    BASE_DIR / "order_report.html",
    BASE_DIR / "Hoang Anh Request.html",
    BASE_DIR / "hoang-anh-request" / "index.html",
    BASE_DIR / "generate_report.py",
    BASE_DIR / "generate_hoang_anh_request.py",
    BASE_DIR / "watch_report_updates.py",
]


def run_command(args):
    return subprocess.run(
        args,
        cwd=str(BASE_DIR),
        text=True,
        capture_output=True,
    )

def log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}\n"
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line)
    print(line, end="")


def generate():
    log("Scheduled run, regenerating reports...")
    ok = True
    for script in GENERATE_SCRIPTS:
        if not script.exists():
            continue
        result = run_command([sys.executable, str(script)])
        if result.stdout.strip():
            log(result.stdout.strip())
        if result.returncode != 0:
            ok = False
            if result.stderr.strip():
                log(result.stderr.strip())
    return ok


def publish():
    tracked = [str(path.relative_to(BASE_DIR)) for path in PUBLISH_FILES if path.exists()]
    if not tracked:
        return True

    status = run_command(["git", "status", "--short", "--"] + tracked)
    if status.returncode != 0:
        if status.stderr.strip():
            log(status.stderr.strip())
        return False
    if not status.stdout.strip():
        log("No publishable changes detected.")
        return True

    log("Publishable changes detected, committing and pushing...")
    add = run_command(["git", "add", "--"] + tracked)
    if add.returncode != 0:
        if add.stderr.strip():
            log(add.stderr.strip())
        return False

    commit_message = f"Auto update reports {time.strftime('%Y-%m-%d %H:%M:%S')}"
    commit = run_command(["git", "commit", "-m", commit_message])
    if commit.returncode != 0:
        combined = "\n".join(part for part in [commit.stdout.strip(), commit.stderr.strip()] if part).strip()
        if "nothing to commit" in combined.lower():
            log("Nothing to commit after staging.")
        elif combined:
            log(combined)
            return False

    push = run_command(["git", "push", "origin", "HEAD:main"])
    if push.returncode != 0:
        combined = "\n".join(part for part in [push.stdout.strip(), push.stderr.strip()] if part).strip()
        if combined:
            log(combined)
        return False

    if push.stdout.strip():
        log(push.stdout.strip())
    if push.stderr.strip():
        log(push.stderr.strip())
    return True


def main():
    log("Scheduled watcher started.")
    if generate():
        publish()


if __name__ == "__main__":
    main()
