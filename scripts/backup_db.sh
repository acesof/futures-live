#!/bin/zsh
# Daily restic backup of futures-live's NOT-in-git runtime data to BAZE.
# Code lives in git (github.com:acesof/futures-live); this covers everything
# .gitignore'd under data/:
#   data/audit.db            — the live trade ledger (run_log + executions +
#                              late-fill/off-session reconciler rows)
#   data/executor_state.json — active contract months per symbol (roll state)
#   data/reference_equity.json — daily-loss circuit reference
#   data/close_prices_*.json, data/targets_*.json — per-cycle snapshots
#
# WHY restic, not git: the .db is a binary that mutates every cron; we do NOT
# pollute git with it (forex-live's git-committed audit.db is the legacy
# exception, not the pattern). This mirrors futures-instra/scripts/backup_db.sh.
#
# Scheduled via crontab (0 2 * * *). Password is read from a chmod-600 FILE,
# not the keychain: cron's non-GUI security session cannot read the login
# keychain (restic then exits 44). This is the r-factory 2026-06-02 lesson.
#
# Restore (any path or the whole tree):
#   export RESTIC_REPOSITORY=/Volumes/BAZE/futures-live-restic
#   export RESTIC_PASSWORD_FILE=/Users/acess/.config/restic/futures-live.pw
#   restic snapshots
#   restic restore <snapshot-id> --target /tmp/recover \
#       --include /Users/acess/projects/futures-live/data/audit.db   # just the ledger
#   restic restore <snapshot-id> --target /tmp/recover               # everything
#   # then rsync the recovered subtree back to its canonical path
set -e

REPO_DIR="/Users/acess/projects/futures-live"
LOG_DIR="$REPO_DIR/logs"
LOG="$LOG_DIR/backup_db.log"
mkdir -p "$LOG_DIR"

export RESTIC_REPOSITORY="/Volumes/BAZE/futures-live-restic"
export RESTIC_PASSWORD_FILE="/Users/acess/.config/restic/futures-live.pw"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

{
  echo "[$(ts)] backup_db starting"

  # Refuse if BAZE isn't mounted — fail loudly rather than silently skip a day.
  if [ ! -d "$RESTIC_REPOSITORY" ]; then
    echo "[$(ts)] ERROR: $RESTIC_REPOSITORY not present (BAZE unmounted?). Aborting."
    exit 2
  fi

  cd "$REPO_DIR"
  /opt/homebrew/bin/restic backup \
      data/ \
      --tag daily --quiet
  echo "[$(ts)] backup snapshot complete"

  # Retention: keep 7 daily, 4 weekly, 6 monthly, 2 yearly.
  /opt/homebrew/bin/restic forget --tag daily \
      --keep-daily 7 --keep-weekly 4 --keep-monthly 6 --keep-yearly 2 \
      --prune --quiet
  echo "[$(ts)] forget+prune complete"

  echo "[$(ts)] backup_db ok"
} >> "$LOG" 2>&1
