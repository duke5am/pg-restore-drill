#!/usr/bin/env bash
# ===========================================================================
# drill.sh - one command: run the whole restore drill and print the report
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# This is the entry point to schedule (cron, systemd timer, CI job). It:
#
#   1. checks the prerequisites and the free disk space, so a drill fails at the
#      start rather than ninety per cent of the way through a restore;
#   2. runs drill/restore_drill.sh, which takes a base backup of a throwaway
#      cluster, writes known rows at known times, chooses a recovery target,
#      destroys the cluster, restores, replays WAL to the target and verifies by
#      querying that the rows before the target survived and the rows after it
#      did not;
#   3. keeps the raw log and turns it into a measured RPO/RTO report with
#      drill/report.py - both the human report and a JSON one, so the history
#      can be graphed;
#   4. exits non-zero unless everything passed. That exit code is the whole
#      point: wire it to your alerting, and "time since the last successful,
#      verified restore" becomes a number you can watch.
#
# Usage:
#   ./drill.sh                                  # defaults; cleans up after itself
#   ./drill.sh --work-dir /srv/pgdrill          # scratch data somewhere roomy
#   ./drill.sh --report-dir /var/log/pgdrill    # where to keep logs and reports
#   ./drill.sh --negative-control               # prove the verification can fail
#   ./drill.sh --json | ./drill.sh --keep
#
# Exit status:
#   0  the drill ran and every verification check passed
#   1  the drill ran and verification FAILED (a real problem: read the report)
#   2  nothing was measured - a missing prerequisite, or the drill died before
#      it could verify anything
#   3  the report could not be produced
#
#   With --negative-control the semantics invert deliberately: the drill is
#   supposed to fail, and this script exits 0 only when the mismatch WAS
#   detected. A negative control that passes is a broken test, not good news.
#
# All options not listed below are passed straight through to restore_drill.sh;
# run ./restore_drill.sh --help for the full list.
# ===========================================================================

set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIT_DIR="$(cd "${SELF_DIR}/.." && pwd)"

DRILL="${SELF_DIR}/restore_drill.sh"
REPORT="${SELF_DIR}/report.py"
DRILL_ARGS=()
REPORT_DIR="${PGDR_REPORT_DIR:-./drill-reports}"
JSON=0
QUIET=0
EXPECT_FAIL=0
NEGATIVE_CONTROL=0
TEE_TO_TERMINAL=0

usage() {
    cat >&2 <<'USAGE'
Usage: drill.sh [OPTIONS] [options passed through to restore_drill.sh]

Options handled here:
  --report-dir DIR    where to write the log and the reports
                      (default ./drill-reports, or $PGDR_REPORT_DIR)
  --json              print the JSON report instead of the text report
  --quiet             only print the raw drill log's final summary and the verdict
  --negative-control  expect the verification to FAIL. Passed to the drill.
                      Exits 0 only if the mismatch was actually detected.
  --expect-fail       same effect, without changing the drill's behaviour
  -h, --help          this help

Passed through (see ./restore_drill.sh --help):
  --work-dir DIR   --port N   --keep   --pre-rows N   --post-rows N
  --pre-batches N  --batch-gap S   --pre-target-gap S   --target-time TS
  --inclusive on|off   --no-verifybackup   --no-fsync   --start-timeout S
  --replay-timeout S   --pg-option 'OPTS'

Examples:
  ./drill.sh                                        # run it now
  ./drill.sh --keep --pg-option '-c shared_memory_type=mmap'
  ./drill.sh --negative-control                     # the test of the test
  ./drill.sh --report-dir /var/log/pgdrill --json
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --report-dir) REPORT_DIR="$2"; shift 2 ;;
        --json) JSON=1; shift ;;
        --quiet|-q) QUIET=1; shift ;;
        --negative-control) NEGATIVE_CONTROL=1; EXPECT_FAIL=1; DRILL_ARGS+=(--negative-control); shift ;;
        --expect-fail) EXPECT_FAIL=1; shift ;;
        --tee) TEE_TO_TERMINAL=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) DRILL_ARGS+=("$1"); shift ;;
    esac
done

log()  { printf '[drill.sh] %s\n' "$*" >&2; }
die()  { printf '[drill.sh] ERROR: %s\n' "$*" >&2; exit 2; }

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
[ -x "$DRILL" ] || die "$DRILL is missing or not executable. From the kit root: chmod +x drill/*.sh backup/*.sh monitoring/*.sh, or run lib/common.sh's install_perms."
[ -r "$REPORT" ] || die "$REPORT is missing"
PYTHON="${PYTHON:-python3}"
command -v "$PYTHON" >/dev/null 2>&1 || die "python3 is required by report.py"

# resolve the PostgreSQL bin directory the same way the other scripts do
# shellcheck source=../lib/common.sh
if [ -r "${KIT_DIR}/lib/common.sh" ]; then
    . "${KIT_DIR}/lib/common.sh"
fi
PGBIN_DIR="$(pgbin 2>/dev/null || true)"
[ -n "$PGBIN_DIR" ] || die "could not find the PostgreSQL binaries. Set PGBIN to the bin directory (for example /usr/lib/postgresql/17/bin)."
for b in initdb pg_ctl pg_basebackup psql pg_controldata; do
    [ -x "${PGBIN_DIR}/${b}" ] || die "missing ${PGBIN_DIR}/${b} - this kit needs the PostgreSQL server package, not just the client"
done
export PGBIN="$PGBIN_DIR"

# ---------------------------------------------------------------------------
# Report directory
# ---------------------------------------------------------------------------
mkdir -p "$REPORT_DIR" 2>/dev/null || die "cannot create --report-dir $REPORT_DIR"
REPORT_DIR="$(cd "$REPORT_DIR" && pwd)"
STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG="${REPORT_DIR}/drill-${STAMP}.log"
REPORT_TXT="${REPORT_DIR}/drill-${STAMP}.report.txt"
REPORT_JSON="${REPORT_DIR}/drill-${STAMP}.report.json"

log "postgres binaries : $PGBIN_DIR ($("${PGBIN_DIR}/postgres" --version 2>/dev/null | sed 's/.* //'))"
log "report directory  : $REPORT_DIR"
log "log               : $LOG"
if [ "$NEGATIVE_CONTROL" = "1" ]; then
    log "NEGATIVE CONTROL  : the drill is EXPECTED to fail verification. This script exits 0 only if it did."
fi

# ---------------------------------------------------------------------------
# Run the drill. Output is redirected to a file rather than piped through tee:
# in containers and under PRoot a pipe shared with a forking postmaster can keep
# the read end open and hang the pipeline forever.
# ---------------------------------------------------------------------------
log "running the drill ..."
if [ "${#DRILL_ARGS[@]}" -gt 0 ]; then
    "$DRILL" "${DRILL_ARGS[@]}" >"$LOG" 2>&1
else
    "$DRILL" >"$LOG" 2>&1
fi
DRILL_RC=$?
log "restore_drill.sh exited $DRILL_RC"

if [ "$TEE_TO_TERMINAL" = "1" ]; then
    cat "$LOG"
fi

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
REPORT_ARGS=( --log "$LOG" --out "$REPORT_TXT" )
[ "$JSON" = "1" ] && REPORT_ARGS+=( --json )
[ "$EXPECT_FAIL" = "1" ] && REPORT_ARGS+=( --expect-fail )

if [ "$JSON" = "1" ]; then
    "$PYTHON" "$REPORT" "${REPORT_ARGS[@]}"
    REPORT_RC=$?
    "$PYTHON" "$REPORT" --log "$LOG" --json --out "$REPORT_JSON" --allow-incomplete >/dev/null 2>&1
else
    if [ "$QUIET" = "1" ]; then
        "$PYTHON" "$REPORT" "${REPORT_ARGS[@]}" -q
        REPORT_RC=$?
        "$PYTHON" "$REPORT" --log "$LOG" --out "$REPORT_TXT" --allow-incomplete >/dev/null 2>&1
    else
        "$PYTHON" "$REPORT" "${REPORT_ARGS[@]}"
        REPORT_RC=$?
    fi
    "$PYTHON" "$REPORT" --log "$LOG" --json --out "$REPORT_JSON" --allow-incomplete >/dev/null 2>&1
fi

printf '\n' >&2
log "raw drill log        : $LOG"
log "text report          : $REPORT_TXT"
log "json report          : $REPORT_JSON"
log "keep these: the trend of measured RPO and RTO across drills is the early"
log "warning that no single pass/fail bit gives you."

if [ "$NEGATIVE_CONTROL" = "1" ]; then
    if [ "$REPORT_RC" = "0" ]; then
        log "NEGATIVE CONTROL OUTCOME: the drill FAILED verification and the report agrees."
        log "  That is the correct result: the verification can detect an under-recovery."
        exit 0
    fi
    log "NEGATIVE CONTROL OUTCOME: the drill did NOT fail verification, or the report is"
    log "  incomplete. Either way the test of the test did not work - treat this as a failure."
    exit 2
fi

if [ "$DRILL_RC" = "3" ]; then
    log "the drill could not complete (infrastructure failure, exit 3). Nothing was proven."
    exit 2
fi

exit "$REPORT_RC"
