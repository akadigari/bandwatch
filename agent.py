"""Local daily runner for the bandwatch archiver.

launchd (com.bandwatch.archive) runs this once a day instead of the old
GitHub Actions cron. The CI path died two different ways in one week
(40-minute timeout on heavy days, then GitHub's 100 MB file block on the
push step), and both failures were invisible until someone went looking.
Running on the Mac that owns the data removes the wall clock cap and makes
the cadence observable from local file mtimes.

What one run does:
  1. Refuses to start if another archive/repair run holds the lock.
  2. Runs archiver.py (catch-up + backfill + metadata snapshot).
  3. Commits whatever changed under data/.
  4. Pushes, best effort. A push failure is printed loudly but does not
     kill the run: the data is already safe in the local commit, and the
     next run pushes the backlog. This matters because the machine's
     GitHub token can die independently of the archive (it did on
     2026-07-28), and losing data over a dead token would be absurd.
  5. Exits nonzero if the ARCHIVER failed, so `launchctl list` shows a
     real error code instead of a green 0 over a broken sensor.

The git env mirrors knaves/paddock/collect.py, the one push path already
proven to work from launchd on this machine.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOCK = HERE / ".agent.lock"
PYTHON = HERE / "venv" / "bin" / "python"

GIT_ENV = {
    "PATH": "/usr/bin:/bin:/usr/local/bin",
    "GIT_TERMINAL_PROMPT": "0",
    "HOME": str(Path.home()),
}


def say(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}Z] {msg}", flush=True)


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=HERE, env=GIT_ENV,
                          capture_output=True, text=True)


def main() -> int:
    if LOCK.exists():
        say(f"SKIP: {LOCK.name} exists (another archive or repair run is active). "
            "Delete it if that is wrong.")
        return 0
    LOCK.write_text(datetime.now(timezone.utc).isoformat())
    try:
        say("archive run starting")
        result = subprocess.run([str(PYTHON), str(HERE / "archiver.py")],
                                cwd=HERE)
        if result.returncode != 0:
            say(f"ARCHIVER FAILED with exit {result.returncode}; nothing committed")
            return result.returncode

        git("add", "data")
        diff = git("diff", "--cached", "--quiet")
        if diff.returncode == 0:
            say("nothing new this run")
            return 0

        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        commit = git("commit", "-q", "-m", f"bandwatch: archive {stamp} (local)")
        if commit.returncode != 0:
            say(f"COMMIT FAILED: {commit.stderr.strip()[:200]}")
            return 1
        say("committed")

        pull = git("pull", "--rebase", "--autostash", "-q", "origin", "main")
        if pull.returncode != 0:
            say(f"pull --rebase failed ({pull.stderr.strip()[:160]}); "
                "pushing without it may be rejected, next run retries")
        push = git("push", "-q")
        if push.returncode != 0:
            say(f"PUSH FAILED (data is safe in the local commit): "
                f"{push.stderr.strip()[:200]}")
        else:
            say("pushed")
        return 0
    finally:
        LOCK.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
