#!/usr/bin/env bash
# ===========================================================================
# basebackup.sh - physical base backup with verification, retry and retention
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# WHAT IT DOES
#   1. Runs pg_basebackup with a defensible set of flags (streamed WAL so the
#      backup is self-contained, fast checkpoint, SHA256 manifest where the
#      server supports it).
#   2. Publishes the result ATOMICALLY: it is built in a hidden .partial
#      directory and renamed into place only after it succeeds, so a cron job
#      or a monitoring scraper never sees a half-finished base backup and
#      mistakes it for a good one.
#   3. Records a machine-readable metadata file next to the backup containing
#      the server identity, the version, the LSN range the backup can serve,
#      its size and its checksum algorithm. The drill and the runbook use this
#      to prove a recovery target is reachable BEFORE anyone starts a restore.
#   4. Retries on failure with backoff, keeping the failed attempt aside for
#      diagnosis.
#   5. Prunes old backups under a retention policy that will NOT delete the
#      newest backup, will not delete so much that you are left with fewer
#      than MIN_KEEP, and only ever deletes directories it created itself.
#
# VERSION NOTES (important, and handled at runtime)
#   * backup manifests and pg_verifybackup require PostgreSQL 13 or newer.
#     Older servers get no manifest, and the script says so in the metadata.
#   * pg_basebackup --resume does NOT exist in PostgreSQL 17 (it was never
#     merged). Real resume-on-failure in 17 is done with INCREMENTAL backups:
#     pg_basebackup --incremental=OLDMANIFEST, then pg_combinebackup to
#     materialise a full. This script supports both --incremental-of and a
#     plain retry loop, and tells you which one it used.
#
# USAGE
#   ./basebackup.sh --backup-root /var/backups/pgdrill --label nightly
#   ./basebackup.sh --host db1 --port 5432 --user postgres --verify
#   ./basebackup.sh --prune-only --prune-dry-run
# ===========================================================================

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SELF_DIR}/../lib/common.sh"

# --- defaults ---------------------------------------------------------------
PGHOST="${PGHOST:-/var/run/postgresql}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/pgdrill}"
LABEL="${LABEL:-base}"
RETAIN_COUNT="${RETAIN_COUNT:-3}"      # keep at least the newest N base backups
RETAIN_DAYS="${RETAIN_DAYS:-30}"       # ...and anything younger than this
MIN_KEEP="${MIN_KEEP:-2}"              # never prune below this many
RETRIES="${RETRIES:-3}"
RETRY_BACKOFF="${RETRY_BACKOFF:-15}"   # seconds, doubled each attempt
VERIFY=0
PRUNE_ONLY=0
PRUNE_DRY_RUN=0
COMPRESS=""                            # e.g. client-gzip:6, server-zstd:3
INCREMENTAL_OF=""
SLOT=""
CREATE_SLOT=0
TARGET=""                              # -t, for a standby/replication target
EXTRA_ARGS=()

usage() {
    cat >&2 <<'USAGE'
Usage: basebackup.sh [OPTIONS]

Connection:
  --host HOST            server host or socket dir        (default $PGHOST)
  --port PORT            server port                      (default 5432)
  --user USER            role to connect as               (default postgres)

Destination:
  --backup-root DIR      root of the backup tree          (default /var/backups/pgdrill)
  --label TEXT           label recorded in the backup name and metadata

Backup options:
  --compress SPEC        passed to pg_basebackup -Z, e.g. client-gzip:6
  --slot NAME            use an existing replication slot
  --create-slot          create the slot named by --slot (or a temp one)
  --target SPEC          pg_basebackup -t (e.g. 'standby:db2' or 'blackhole')
  --incremental-of DIR   take an INCREMENTAL backup against DIR's manifest and
                         then pg_combinebackup it into a full. (PostgreSQL 17+)
  --retries N            attempts on failure             (default 3)
  --retry-backoff S      initial backoff in seconds      (default 15)
  --verify               run verify_backup.sh on the new backup when done

Retention:
  --retain-count N       keep the newest N backups       (default 3)
  --retain-days N        keep anything younger than N days (default 30)
  --min-keep N           never reduce below N backups    (default 2)
  --prune-only           only run retention, take no backup
  --prune-dry-run        report what retention would delete, delete nothing
  --no-prune             take the backup, skip retention

  -h, --help             this help

Exit status: 0 on success, non-zero on failure. With --verify, a failed
verification fails the whole run - a backup that does not verify is not a
backup.
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host) PGHOST="$2"; shift 2 ;;
        --port) PGPORT="$2"; shift 2 ;;
        --user) PGUSER="$2"; shift 2 ;;
        --backup-root) BACKUP_ROOT="$2"; shift 2 ;;
        --label) LABEL="$2"; shift 2 ;;
        --compress) COMPRESS="$2"; shift 2 ;;
        --slot) SLOT="$2"; shift 2 ;;
        --create-slot) CREATE_SLOT=1; shift ;;
        --target) TARGET="$2"; shift 2 ;;
        --incremental-of) INCREMENTAL_OF="$2"; shift 2 ;;
        --retries) RETRIES="$2"; shift 2 ;;
        --retry-backoff) RETRY_BACKOFF="$2"; shift 2 ;;
        --verify) VERIFY=1; shift ;;
        --retain-count) RETAIN_COUNT="$2"; shift 2 ;;
        --retain-days) RETAIN_DAYS="$2"; shift 2 ;;
        --min-keep) MIN_KEEP="$2"; shift 2 ;;
        --prune-only) PRUNE_ONLY=1; shift ;;
        --prune-dry-run) PRUNE_DRY_RUN=1; shift ;;
        --no-prune) RETAIN_COUNT=0; RETAIN_DAYS=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "basebackup.sh: unknown option '$1' (try --help)" ;;
    esac
done

PGBIN_DIR="$(pgbin)" || die "could not find the PostgreSQL binaries; set PGBIN"
need_cmd wc
need_cmd mv
need_cmd cp
need_cmd awk
need_cmd sort

BASE_DIR="${BACKUP_ROOT}/base"
mkdir -p "$BASE_DIR"
pg_owner_give "$BACKUP_ROOT"

# ===========================================================================
# RETENTION
# ===========================================================================
# The policy here is deliberately conservative, because over-pruning backups is
# how a "we have backups" team becomes a "we had backups" team.
#
#   * Published backups are named  <UTC timestamp>_<label>  and live in
#     $BACKUP_ROOT/base/. Only directories matching that pattern are ever
#     considered for deletion. Hidden .partial*/.failed* directories are
#     handled separately and are never touched by count-based pruning.
#   * The NEWEST backup is never deleted, ever. Neither is the one we just
#     created. If a retention policy can delete your newest backup, one bad
#     clock or one empty directory listing turns into data loss.
#   * We never go below MIN_KEEP backups.
#   * Anything younger than RETAIN_DAYS is kept regardless of the count.
#
# WHAT THIS POLICY DOES NOT DO - and you must decide it yourself:
#   Retention of BASE backups and the recovery window are different things.
#   Deleting an old base backup removes your ability to recover to any time
#   before your oldest remaining base backup, NO MATTER HOW MUCH WAL YOU STILL
#   HAVE. WAL without a base backup to start from is useless. So RETAIN_DAYS
#   is effectively a statement about how far back in time you can go. If a
#   slow-burn corruption is discovered three weeks later, a 7-day retention
#   window means the damage is unrecoverable. See docs/BACKUP-STRATEGY.md.
# ===========================================================================
prune_base() {
    local dry="$1"
    local -a all=()
    local d base

    # Deliberately avoid `find` for the listing: several container and FUSE
    # filesystems under-report with find, and a retention script that cannot
    # see a backup will happily delete the ones it can see. A plain glob is
    # exact.
    shopt -s nullglob
    for d in "$BASE_DIR"/*/; do
        base="$(basename "$d")"
        case "$base" in
            .*) continue ;;                       # .partial*, .failed*
            [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z_*) ;;
            *) continue ;;                        # not one of ours
        esac
        [ -f "$d/PG_VERSION" ] || continue        # not a real cluster dir
        all+=("$base")
    done
    shopt -u nullglob

    local total="${#all[@]}"
    if [ "$total" -eq 0 ]; then
        log "retention: no published base backups found under $BASE_DIR"
        return 0
    fi

    # Name sort == chronological sort, because the name starts with UTC time.
    # NOTE: we sort via a temporary file rather than process substitution
    # (`done < <(...)`). Process substitution depends on /dev/fd, which is
    # absent or broken in several container and emulated environments, and it
    # fails with "/dev/fd/63: No such file or directory" - inside a retention
    # function, that is a silent no-prune or, worse, an over-prune. A temp file
    # works everywhere.
    SORT_TMP="$(mktemp 2>/dev/null || echo "${BACKUP_ROOT}/.sort.$$")"
    printf '%s\n' "${all[@]}" | sort > "$SORT_TMP"
    local -a sorted=()
    while IFS= read -r line; do
        [ -n "$line" ] && sorted+=("$line")
    done < "$SORT_TMP"
    rm -f -- "$SORT_TMP"

    local newest="${sorted[$((total - 1))]}"
    log "retention: $total published base backup(s); newest is $newest"

    local -a keep=() del=()
    local i idx from_end age_days mt
    for (( i = 0; i < total; i++ )); do
        base="${sorted[$i]}"
        from_end=$(( total - 1 - i ))          # 0 == newest
        local reason=""
        if [ "$from_end" -eq 0 ]; then
            reason="newest backup - never pruned"
        elif [ "$from_end" -lt "$RETAIN_COUNT" ]; then
            reason="within newest $RETAIN_COUNT"
        else
            mt="$(stat -c %Y "$BASE_DIR/$base" 2>/dev/null || echo 0)"
            age_days="$(awk -v n="$(date +%s)" -v m="$mt" 'BEGIN{printf "%.2f", (n-m)/86400}')"
            if awk -v a="$age_days" -v r="$RETAIN_DAYS" 'BEGIN{exit !(a < r)}'; then
                reason="younger than ${RETAIN_DAYS}d (${age_days}d)"
            fi
        fi
        if [ -n "$reason" ]; then
            keep+=("$base")
        else
            del+=("$base")
        fi
    done

    # Never prune below MIN_KEEP, and never prune the newest.
    while [ "${#del[@]}" -gt 0 ] && [ $(( ${#keep[@]} )) -lt "$MIN_KEEP" ]; do
        local last="${del[${#del[@]}-1]}"
        warn "retention: keeping $last to stay at MIN_KEEP=$MIN_KEEP"
        keep+=("$last")
        unset 'del[${#del[@]}-1]'
    done

    if [ "${#del[@]}" -eq 0 ]; then
        log "retention: nothing to prune"
        return 0
    fi

    local freed=0 sz
    for base in "${del[@]}"; do
        if [ "$base" = "$newest" ]; then      # paranoia: see comment above
            warn "retention: refusing to delete the newest backup $base"
            continue
        fi
        sz="$(dir_bytes "$BASE_DIR/$base" 2>/dev/null || echo 0)"
        if [ "$dry" = "1" ]; then
            log "retention: WOULD delete $base ($(fmt_bytes "$sz"))"
        else
            log "retention: deleting $base ($(fmt_bytes "$sz"))"
            rm -rf -- "$BASE_DIR/$base"
            freed=$(( freed + sz ))
        fi
    done
    if [ "$dry" != "1" ]; then
        log "retention: freed $(fmt_bytes "$freed")"
        # WARNING: pruning base backups does NOT prune the WAL archive. If you
        # delete the oldest base backup, the WAL older than it can never be
        # replayed into anything, and is now dead weight - but deleting it is
        # only safe once you are certain no base backup needs it. Never prune
        # the archive automatically on the same schedule as the base backups.
    fi
}

# Sweep abandoned .partial_* directories older than a day. These are only ever
# created by this script, and only while a backup is in flight.
sweep_partials() {
    local d base
    shopt -s nullglob
    for d in "$BASE_DIR"/.partial_* "$BASE_DIR"/.failed_*; do
        base="$(basename "$d")"
        local mt age
        mt="$(stat -c %Y "$d" 2>/dev/null || echo 0)"
        age="$(awk -v n="$(date +%s)" -v m="$mt" 'BEGIN{printf "%.0f", (n-m)/3600}')"
        if [ "$age" -ge 24 ]; then
            log "sweep: removing stale in-progress directory $base (${age}h old)"
            rm -rf -- "$d"
        fi
    done
    shopt -u nullglob
    return 0
}

if [ "$PRUNE_ONLY" = "1" ]; then
    prune_base "$PRUNE_DRY_RUN"
    exit 0
fi

# ===========================================================================
# VERSION / CAPABILITY DETECTION
# ===========================================================================
SRV_NUM="$(server_version_num "$PGHOST" "$PGPORT" "$PGUSER" || true)"
if [ -z "$SRV_NUM" ]; then
    die "cannot connect to PostgreSQL at ${PGHOST}:${PGPORT} as ${PGUSER}. Check pg_isready and pg_hba.conf."
fi
log "server reports server_version_num=$SRV_NUM"

# Extract just the version number. `--version` prints e.g.
#   pg_basebackup (PostgreSQL) 17.11 (Debian 17.11-0+deb13u1)
# so $NF would give "17.11-0+deb13u1)" - a trailing paren that then ends up in
# the metadata. Take the token right after "(PostgreSQL)".
BASEBB_VER="$("$PGBIN_DIR/pg_basebackup" --version 2>/dev/null | sed -nE 's/.*PostgreSQL\)[[:space:]]+([^[:space:]]+).*/\1/p')"
[ -n "$BASEBB_VER" ] || BASEBB_VER="unknown"
log "client pg_basebackup is $BASEBB_VER"

# Manifests + pg_verifybackup: PostgreSQL 13+.
SUPPORTS_MANIFEST=0
if [ "$SRV_NUM" -ge 130000 ]; then SUPPORTS_MANIFEST=1; fi
# pg_combinebackup / incremental: PostgreSQL 17+.
SUPPORTS_INCREMENTAL=0
if [ "$SRV_NUM" -ge 170000 ]; then SUPPORTS_INCREMENTAL=1; fi

# ===========================================================================
# BUILD THE BACKUP
# ===========================================================================
STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
SAFE_LABEL="$(printf '%s' "$LABEL" | tr -c 'A-Za-z0-9._-' '_')"
FINAL_NAME="${STAMP}_${SAFE_LABEL}"
FINAL_DIR="${BASE_DIR}/${FINAL_NAME}"
WORK_DIR="${BASE_DIR}/.partial_${SAFE_LABEL}"
COMBINED_DIR="${BASE_DIR}/.partial_${SAFE_LABEL}_combined"

[ -e "$FINAL_DIR" ] && die "$FINAL_DIR already exists; wait a second or change --label"

ARGS=( -D "$WORK_DIR" -X stream -c fast -v )
[ "${SUPPORTS_MANIFEST}" = "1" ] && ARGS+=( --manifest-checksums=SHA256 )
[ "${SUPPORTS_MANIFEST}" != "1" ] && ARGS+=( --no-manifest )
[ -n "$COMPRESS" ] && ARGS+=( -Z "$COMPRESS" )
[ -n "$SLOT" ] && ARGS+=( -S "$SLOT" )
[ "${CREATE_SLOT}" = "1" ] && ARGS+=( -C )
[ -n "$TARGET" ] && ARGS+=( -t "$TARGET" )

INCR_BASE=""
if [ -n "$INCREMENTAL_OF" ]; then
    if [ "$SUPPORTS_INCREMENTAL" != "1" ]; then
        die "--incremental-of needs PostgreSQL 17 or newer (server is $SRV_NUM). Use --retries instead."
    fi
    if [ ! -f "$INCREMENTAL_OF/backup_manifest" ]; then
        die "$INCREMENTAL_OF/backup_manifest not found. An incremental backup needs the manifest of the parent backup."
    fi
    INCR_BASE="$INCREMENTAL_OF"
    ARGS+=( -i "$INCR_BASE/backup_manifest" )
    log "incremental backup against $INCR_BASE"
fi

log "starting base backup: label=$LABEL dir=$FINAL_DIR"
STARTED_EPOCH="$(now_epoch)"
STARTED_AT="$(now_iso)"

attempt=1
backoff="$RETRY_BACKOFF"
backup_ok=0
while [ "$attempt" -le "$RETRIES" ]; do
    log "pg_basebackup attempt $attempt/$RETRIES"
    rm -rf -- "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    pg_owner_give "$WORK_DIR"

    if "$PGBIN_DIR/pg_basebackup" "${ARGS[@]}" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -l "$SAFE_LABEL" -n; then
        backup_ok=1
        break
    fi

    rc=$?
    err "pg_basebackup attempt $attempt failed (exit $rc)"
    if [ "$attempt" -lt "$RETRIES" ]; then
        # Keep the failed attempt for diagnosis, then start clean.
        # NOTE: pg_basebackup --resume does not exist in PostgreSQL 17, so a
        # retry is a fresh copy. The real "resume" mechanism in 17 is
        # incremental backups (--incremental-of), which copies only changed
        # blocks instead of restarting the whole transfer.
        if [ -d "$WORK_DIR" ]; then
            mv -- "$WORK_DIR" "${BASE_DIR}/.failed_${STAMP}_${SAFE_LABEL}_a${attempt}" 2>/dev/null || rm -rf -- "$WORK_DIR"
        fi
        log "retrying in ${backoff}s"
        sleep "$backoff"
        backoff=$(( backoff * 2 ))
    fi
    attempt=$(( attempt + 1 ))
done

if [ "$backup_ok" != "1" ]; then
    err "base backup FAILED after $RETRIES attempt(s). No backup was published."
    err "The failed attempt (if any) was kept under $BASE_DIR/.failed_* for diagnosis."
    exit 1
fi

# --- incremental -> full -----------------------------------------------------
if [ -n "$INCR_BASE" ]; then
    log "combining incremental with $INCR_BASE using pg_combinebackup"
    rm -rf -- "$COMBINED_DIR"
    # pg_combinebackup takes the chain oldest-first and the incremental last.
    if ! "$PGBIN_DIR/pg_combinebackup" -o "$COMBINED_DIR" "$INCR_BASE" "$WORK_DIR" >&2; then
        err "pg_combinebackup failed. The incremental backup was NOT published."
        err "An incremental chain is only as good as its weakest link - do not"
        err "ignore this. See docs/COMMON-FAILURES.md."
        exit 1
    fi
    rm -rf -- "$WORK_DIR"
    mv -- "$COMBINED_DIR" "$WORK_DIR"
    log "pg_combinebackup produced a full backup at $WORK_DIR"
fi

FINISHED_EPOCH="$(now_epoch)"
FINISHED_AT="$(now_iso)"
DURATION="$(fsub "$FINISHED_EPOCH" "$STARTED_EPOCH")"

# ===========================================================================
# METADATA
# ===========================================================================
# Read the identity of the cluster and the LSN range this backup can serve.
# This matters more than it looks: a recovery target BEFORE the backup's start
# LSN is UNREACHABLE from this backup, and you want to know that at 3am before
# you start, not after replay stops. See docs/RESTORE-RUNBOOK.md.
SYSID=""; CKPT_LSN=""; CKPT_TLI=""; CTRL_STATE=""
if CTRL_OUT="$("$PGBIN_DIR/pg_controldata" "$WORK_DIR" 2>/dev/null)"; then
    SYSID="$(printf '%s\n' "$CTRL_OUT" | sed -n 's/^Database system identifier: *//p' | head -1)"
    CKPT_LSN="$(printf '%s\n' "$CTRL_OUT" | sed -n 's/^Latest checkpoint location: *//p' | head -1)"
    CKPT_TLI="$(printf '%s\n' "$CTRL_OUT" | sed -n "s/^Latest checkpoint's TimeLineID: *//p" | head -1)"
    CTRL_STATE="$(printf '%s\n' "$CTRL_OUT" | sed -n 's/^Database cluster state: *//p' | head -1)"
fi

START_LSN=""; START_TLI=""; BK_LABEL=""
if [ -f "$WORK_DIR/backup_label" ]; then
    START_LSN="$(sed -n 's/^START WAL LOCATION: *\([0-9A-Fa-f]*\/[0-9A-Fa-f]*\).*/\1/p' "$WORK_DIR/backup_label" | head -1)"
    START_TLI="$(sed -n 's/^START TIMELINE: *//p' "$WORK_DIR/backup_label" | head -1)"
    BK_LABEL="$(sed -n 's/^LABEL: *//p' "$WORK_DIR/backup_label" | head -1)"
fi

# WAL-Ranges from the manifest give the exact LSN span the backup covers.
# The manifest is a JSON document; we shell out to python3 for a correct parse
# but fall back to a sed scrape and then to UNKNOWN, so the script still works
# on a host with no python3.
MANIFEST_START=""; MANIFEST_END=""
if [ -f "$WORK_DIR/backup_manifest" ]; then
    MF_TMP="$(mktemp 2>/dev/null || echo "${BACKUP_ROOT}/.manifest.$$")"
    python3 -c '
import json, sys
try:
    with open(sys.argv[1], "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    r = doc.get("WAL-Ranges") or []
    if r:
        sys.stdout.write("%s %s\n" % (r[0].get("Start-LSN", ""), r[-1].get("End-LSN", "")))
except Exception:
    pass
' "$WORK_DIR/backup_manifest" > "$MF_TMP" 2>/dev/null || true
    if [ -s "$MF_TMP" ]; then
        read -r MANIFEST_START MANIFEST_END < "$MF_TMP" || true
    else
        warn "could not parse WAL-Ranges from backup_manifest; falling back to a text scrape"
        MANIFEST_START="$(tr ',' '\n' < "$WORK_DIR/backup_manifest" | sed -n 's/.*"Start-LSN" *: *"\([0-9A-Fa-f]*\/[0-9A-Fa-f]*\)".*/\1/p' | head -1)"
        MANIFEST_END="$(tr ',' '\n' < "$WORK_DIR/backup_manifest" | sed -n 's/.*"End-LSN" *: *"\([0-9A-Fa-f]*\/[0-9A-Fa-f]*\)".*/\1/p' | tail -1)"
        [ -n "$MANIFEST_END" ] || warn "backup end LSN UNKNOWN - record it by hand from the server log"
    fi
    rm -f -- "$MF_TMP"
fi

SIZE_BYTES="$(dir_bytes "$WORK_DIR" 2>/dev/null || echo 0)"
FILE_COUNT="$(python3 -c 'import os,sys;print(sum(len(f) for _,_,f in os.walk(sys.argv[1])))' "$WORK_DIR" 2>/dev/null || echo "unknown")"

MANIFEST_STATE="absent"
[ -f "$WORK_DIR/backup_manifest" ] && MANIFEST_STATE="present"
# Set this as a SHELL variable before the heredoc below. Writing the
# substitution inline inside the heredoc would record the right value in the
# file but leave the variable itself unset, so the log line would lie.
if [ "$SUPPORTS_MANIFEST" = "1" ] && [ "$MANIFEST_STATE" = "present" ]; then
    MANIFEST_CHECKSUMS="SHA256"
else
    MANIFEST_CHECKSUMS="none"
fi

# Write the metadata INSIDE the backup before publishing, so it travels with
# the backup. It is written as shell key=value so both bash and report.py can
# read it without a JSON parser.
#
# NOTE FOR pg_verifybackup: this file is created AFTER pg_basebackup has
# finished, so it is NOT listed in backup_manifest, and pg_verifybackup reports
# "present on disk but not in the manifest" as an error. Any hand-run check must
# therefore pass  -i .pgdrill-meta  (verify_backup.sh does). Verified against
# PostgreSQL 17.11: -i accepts a path that is not in the manifest, and a backup
# with no metadata file still verifies clean.
cat > "$WORK_DIR/.pgdrill-meta" <<META
PGDR_META_VERSION=1
KIND=base
LABEL=$SAFE_LABEL
BACKUP_NAME=$FINAL_NAME
BACKUP_DIR=$FINAL_DIR
STARTED_AT=$STARTED_AT
FINISHED_AT=$FINISHED_AT
STARTED_EPOCH=$STARTED_EPOCH
FINISHED_EPOCH=$FINISHED_EPOCH
DURATION_S=$DURATION
SERVER_VERSION_NUM=$SRV_NUM
CLIENT_PG_BASEBACKUP=$BASEBB_VER
SYSTEM_IDENTIFIER=${SYSID:-unknown}
START_LSN=${START_LSN:-unknown}
BACKUP_END_LSN=${MANIFEST_END:-unknown}
MANIFEST_START_LSN=${MANIFEST_START:-unknown}
CHECKPOINT_LSN=${CKPT_LSN:-unknown}
TIMELINE=${START_TLI:-${CKPT_TLI:-unknown}}
BACKUP_LABEL=${BK_LABEL:-unknown}
CONTROL_STATE=${CTRL_STATE:-unknown}
WAL_METHOD=stream
MANIFEST=$MANIFEST_STATE
MANIFEST_CHECKSUMS=$MANIFEST_CHECKSUMS
INCREMENTAL_OF=${INCR_BASE:-none}
SIZE_BYTES=$SIZE_BYTES
FILE_COUNT=$FILE_COUNT
HOST=$PGHOST
PORT=$PGPORT
USER=$PGUSER
TOOL=pg_basebackup
META

# ===========================================================================
# PUBLISH (atomic rename) and apply retention
# ===========================================================================
mv -- "$WORK_DIR" "$FINAL_DIR"
pg_owner_give "$FINAL_DIR"

log "base backup published: $FINAL_DIR"
log "  size              : $(fmt_bytes "$SIZE_BYTES")"
log "  duration          : $(fmt_dur "$DURATION")"
log "  system identifier : ${SYSID:-unknown}"
log "  backup start LSN  : ${START_LSN:-unknown}   (recovery targets must be >= this)"
log "  backup end LSN    : ${MANIFEST_END:-unknown}"
log "  manifest          : $MANIFEST_STATE (checksums ${MANIFEST_CHECKSUMS:-none})"

event "KIND=base" "BACKUP_NAME=$FINAL_NAME" "BACKUP_DIR=$FINAL_DIR" \
      "DURATION_S=$DURATION" "SIZE_BYTES=$SIZE_BYTES" \
      "START_LSN=${START_LSN:-unknown}" "END_LSN=${MANIFEST_END:-unknown}" \
      "SYSID=${SYSID:-unknown}"

prune_base "$PRUNE_DRY_RUN"
sweep_partials

# ===========================================================================
# OPTIONAL VERIFICATION - the step that turns a backup into a backup
# ===========================================================================
if [ "$VERIFY" = "1" ]; then
    log "verifying the new backup"
    if "${SELF_DIR}/verify_backup.sh" "$FINAL_DIR"; then
        log "verification PASSED for $FINAL_NAME"
    else
        err "verification FAILED for $FINAL_NAME - treat this backup as unusable"
        exit 1
    fi
fi

exit 0
