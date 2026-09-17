#!/usr/bin/env bash
# ===========================================================================
# logical_backup.sh - custom-format pg_dump with retention and verification
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# WHY CUSTOM FORMAT (-Fc) IS THE DEFAULT HERE
#   The whole point of a logical backup in a kit like this is the ability to
#   put back ONE THING: one table, one schema, one function, one sequence
#   value. Only the custom (-Fc) and directory (-Fd) formats support that,
#   because pg_restore can select objects out of them. A plain-SQL dump (-Fp)
#   is a script, and a script can only be run or grep-ed; you cannot ask it for
#   just the orders table without hand-editing SQL at 3am. Custom format can
#   also be compressed, can be restored in parallel with -j, and can be
#   re-listed with `pg_restore --list` to prove it is not truncated.
#
#   Directory format (-Fd) is the better choice if you want parallel dump
#   (-j) and per-table files on disk. Use --format=directory for that.
#
# WHEN LOGICAL BEATS PHYSICAL
#   * You need one table back, not the whole cluster.
#   * You are moving or upgrading across PostgreSQL major versions, or across
#     architectures/endianness. A physical backup does not cross a major
#     version without pg_upgrade.
#   * You need to inspect or edit the data before restoring it.
#   * You want a second, independent copy whose corruption modes are not
#     shared with the physical one. A corrupt page is corrupt in every
#     physical backup derived from it; a logical dump re-reads the data
#     through SQL and will either succeed or fail loudly.
#   * You need something a human can restore with only psql and no cluster.
#
# WHEN PHYSICAL BEATS LOGICAL (i.e. why you still need basebackup.sh)
#   * Size and speed. A logical dump of a 1TB database can take hours to dump
#     and much longer to restore (every index is rebuilt, every constraint
#     re-validated, every foreign key rechecked). A physical restore is a file
#     copy plus WAL replay.
#   * Point-in-time recovery. A logical dump is one consistent point in time
#     and NOTHING ELSE. It cannot be replayed forward. Only physical backups
#     plus a WAL archive give you PITR.
#   * Bloat, physical layout, statistics, and unlogged-table state are not
#     preserved.
#
# IMPORTANT LIMITATION - READ THIS
#   `pg_dump` of a single database does NOT include roles, role membership,
#   tablespace definitions, or per-database GRANTs. Restore a dump onto a
#   fresh cluster without the globals and every object ends up owned by the
#   restoring superuser with the wrong privileges, and application logins may
#   not exist at all. Use --globals (default ON) to also capture
#   `pg_dumpall --globals-only`. See docs/COMMON-FAILURES.md.
#
#   Also: pg_dump takes a consistent snapshot but does NOT block DDL. A
#   concurrent ALTER/DROP during the dump can produce a dump that fails to
#   restore. Run logical backups in a quiet window, or against a replica.
# ===========================================================================

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SELF_DIR}/../lib/common.sh"

PGHOST="${PGHOST:-/var/run/postgresql}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/pgdrill}"
RETAIN_COUNT="${RETAIN_COUNT:-7}"
RETAIN_DAYS="${RETAIN_DAYS:-30}"
MIN_KEEP="${MIN_KEEP:-2}"
FORMAT="${FORMAT:-custom}"          # custom | directory | plain
COMPRESS_LEVEL="${COMPRESS_LEVEL:-6}"
JOBS="${JOBS:-1}"
DATABASES=()
GLOBALS=1
EXCLUDE_TABLE_DATA=""
PRUNE_ONLY=0
PRUNE_DRY_RUN=0
LIST_DUMPS=0
RESTORE_OBJECT=""
RESTORE_INTO=""

usage() {
    cat >&2 <<'USAGE'
Usage: logical_backup.sh [OPTIONS] [DBNAME...]

  With no DBNAME, every non-template database is dumped.

Connection:
  --host HOST --port PORT --user USER

Destination:
  --backup-root DIR      root of the backup tree (default /var/backups/pgdrill)

Dump options:
  --format custom|directory|plain      (default custom)
  --compress-level N     compression level, 0-9      (default 6)
  --jobs N               parallel dump; needs --format=directory (default 1)
  --no-globals           skip the pg_dumpall --globals-only capture
  --exclude-table-data PATTERN         e.g. 'audit_*' for big append-only tables

Retention:
  --retain-count N  --retain-days N  --min-keep N
  --prune-only  --prune-dry-run

Inspection / selective restore:
  --list                 list the dumps currently on disk and exit
  --restore-object DUMP  print (and with --into, run) the pg_restore command
                         that pulls a single object out of DUMP
  --into DBNAME          target database for --restore-object
  --table NAME           the object to restore with --restore-object
  --schema NAME          schema of the object

Examples:
  # nightly logical backup of every database, keeping 7
  ./logical_backup.sh --backup-root /var/backups/pgdrill

  # restore ONE table from a dump into a scratch database
  ./logical_backup.sh --restore-object \
      /var/backups/pgdrill/logical/appdb_20260917T043000Z.dump \
      --table orders --into appdb_scratch

  # look inside a dump without restoring anything
  pg_restore --list /var/backups/pgdrill/logical/appdb_*.dump
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host) PGHOST="$2"; shift 2 ;;
        --port) PGPORT="$2"; shift 2 ;;
        --user) PGUSER="$2"; shift 2 ;;
        --backup-root) BACKUP_ROOT="$2"; shift 2 ;;
        --format) FORMAT="$2"; shift 2 ;;
        --compress-level) COMPRESS_LEVEL="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --no-globals) GLOBALS=0; shift ;;
        --exclude-table-data) EXCLUDE_TABLE_DATA="$2"; shift 2 ;;
        --retain-count) RETAIN_COUNT="$2"; shift 2 ;;
        --retain-days) RETAIN_DAYS="$2"; shift 2 ;;
        --min-keep) MIN_KEEP="$2"; shift 2 ;;
        --prune-only) PRUNE_ONLY=1; shift ;;
        --prune-dry-run) PRUNE_DRY_RUN=1; shift ;;
        --list) LIST_DUMPS=1; shift ;;
        --restore-object) RESTORE_OBJECT="$2"; shift 2 ;;
        --into) RESTORE_INTO="$2"; shift 2 ;;
        --table) TABLE_NAME="$2"; shift 2 ;;
        --schema) SCHEMA_NAME="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        -*) die "logical_backup.sh: unknown option '$1' (try --help)" ;;
        *) DATABASES+=("$1"); shift ;;
    esac
done

PGBIN_DIR="$(pgbin)" || die "could not find the PostgreSQL binaries; set PGBIN"
need_cmd wc

LOGICAL_DIR="${BACKUP_ROOT}/logical"
mkdir -p "$LOGICAL_DIR"
pg_owner_give "$BACKUP_ROOT"

# ===========================================================================
# --list : what do we actually have?
# ===========================================================================
if [ "$LIST_DUMPS" = "1" ]; then
    shopt -s nullglob
    found=0
    for f in "$LOGICAL_DIR"/*; do
        [ -f "$f" ] || continue
        case "$f" in *.meta) continue ;; esac
        found=1
        printf '%s  %12s  %s\n' \
            "$(date -u -r "$f" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo '?')" \
            "$(fmt_bytes "$(wc -c < "$f" | tr -d ' ')")" \
            "$(basename "$f")"
    done
    shopt -u nullglob
    [ "$found" = "1" ] || log "no logical dumps under $LOGICAL_DIR"
    exit 0
fi

# ===========================================================================
# --restore-object : single-object restore (the reason for custom format)
# ===========================================================================
if [ -n "$RESTORE_OBJECT" ]; then
    [ -f "$RESTORE_OBJECT" ] || die "no such dump: $RESTORE_OBJECT"
    [ -n "${TABLE_NAME:-}" ] || die "--restore-object needs --table NAME"
    local_sel=()
    [ -n "${SCHEMA_NAME:-}" ] && local_sel=( -n "$SCHEMA_NAME" )
    local_sel+=( -t "$TABLE_NAME" )

    log "objects in $RESTORE_OBJECT matching --table $TABLE_NAME:"
    "$PGBIN_DIR/pg_restore" --list "$RESTORE_OBJECT" | grep -F "$TABLE_NAME" >&2 || true

    if [ -z "$RESTORE_INTO" ]; then
        cat >&2 <<EOF

Dry run only (no --into given). The command you would run is:

  createdb -h $PGHOST -p $PGPORT -U $PGUSER <SCRATCH_DB>
  $PGBIN_DIR/pg_restore -h $PGHOST -p $PGPORT -U $PGUSER \\
      -d <SCRATCH_DB> --no-owner --no-privileges \\
      ${local_sel[*]} $RESTORE_OBJECT

Restoring into a SCRATCH database and then copying the data across with
INSERT ... SELECT is almost always safer than restoring a single table into
the live database, because pg_restore --table does not restore the table's
dependent objects (indexes, constraints, sequences, triggers) and will
happily collide with the existing table.
EOF
        exit 0
    fi

    log "restoring $TABLE_NAME from $(basename "$RESTORE_OBJECT") into $RESTORE_INTO"
    "$PGBIN_DIR/pg_restore" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
        -d "$RESTORE_INTO" --no-owner --no-privileges "${local_sel[@]}" "$RESTORE_OBJECT" >&2
    log "done. Verify with a row count before you trust it."
    exit 0
fi

# ===========================================================================
# RETENTION
# ===========================================================================
# Same philosophy as basebackup.sh: never delete the newest, never drop below
# MIN_KEEP, only touch files this script created. Per-database counts are kept
# separately so one wide database cannot cause another's only dump to be
# pruned. A dump is keyed  <db>_<UTC timestamp>.<ext>.
# ===========================================================================
prune_logical() {
    local dry="$1" base db ts
    shopt -s nullglob
    local -a files=( "$LOGICAL_DIR"/*.dump "$LOGICAL_DIR"/*.dir "$LOGICAL_DIR"/*.sql )
    shopt -u nullglob
    [ "${#files[@]}" -eq 0 ] && { log "retention: no logical dumps to prune"; return 0; }

    local -A groups=()
    local f
    for f in "${files[@]}"; do
        base="$(basename "$f")"
        case "$base" in
            *_20[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.*) ;;
            *) continue ;;
        esac
        db="${base%_20*}"
        groups["$db"]+="$f"$'\n'
    done

    local freed=0 sz mt age_days
    local -a arr sorted del
    for db in "${!groups[@]}"; do
        arr=(); sorted=(); del=()
        while IFS= read -r line; do [ -n "$line" ] && arr+=("$line"); done <<< "${groups[$db]}"
        # Sort via a temporary file, not process substitution: /dev/fd is
        # missing in several container/emulated environments and `done < <(...)`
        # then fails, which inside a retention loop means wrong pruning.
        SORT_TMP="$(mktemp 2>/dev/null || echo "${BACKUP_ROOT}/.sort.$$")"
        printf '%s\n' "${arr[@]}" | sort > "$SORT_TMP"
        while IFS= read -r line; do
            [ -n "$line" ] && sorted+=("$line")
        done < "$SORT_TMP"
        rm -f -- "$SORT_TMP"
        local total="${#sorted[@]}" i from_end reason
        for (( i = 0; i < total; i++ )); do
            f="${sorted[$i]}"
            from_end=$(( total - 1 - i ))
            reason=""
            if [ "$from_end" -eq 0 ]; then
                reason="newest for $db - never pruned"
            elif [ "$from_end" -lt "$RETAIN_COUNT" ]; then
                reason="within newest $RETAIN_COUNT"
            else
                mt="$(stat -c %Y "$f" 2>/dev/null || echo 0)"
                age_days="$(awk -v n="$(date +%s)" -v m="$mt" 'BEGIN{printf "%.2f", (n-m)/86400}')"
                awk -v a="$age_days" -v r="$RETAIN_DAYS" 'BEGIN{exit !(a < r)}' && reason="younger than ${RETAIN_DAYS}d"
            fi
            if [ -n "$reason" ]; then continue; fi
            del+=("$f")
        done
        while [ "${#del[@]}" -gt 0 ] && [ $(( total - ${#del[@]} )) -lt "$MIN_KEEP" ]; do
            unset 'del[${#del[@]}-1]'
        done
        for f in "${del[@]}"; do
            sz="$(wc -c < "$f" | tr -d ' ')"
            if [ "$dry" = "1" ]; then
                log "retention: WOULD delete $(basename "$f") ($(fmt_bytes "$sz"))"
            else
                log "retention: deleting $(basename "$f") ($(fmt_bytes "$sz"))"
                rm -rf -- "$f" "$f.meta"
                freed=$(( freed + sz ))
            fi
        done
    done
    [ "$dry" != "1" ] && log "retention: freed $(fmt_bytes "$freed")"
    return 0
}

if [ "$PRUNE_ONLY" = "1" ]; then
    prune_logical "$PRUNE_DRY_RUN"
    exit 0
fi

# ===========================================================================
# DUMP
# ===========================================================================
SRV_NUM="$(server_version_num "$PGHOST" "$PGPORT" "$PGUSER" || true)"
[ -n "$SRV_NUM" ] || die "cannot connect to PostgreSQL at ${PGHOST}:${PGPORT} as ${PGUSER}"
DUMP_VER="$("$PGBIN_DIR/pg_dump" --version 2>/dev/null | awk '{print $NF}')"
log "pg_dump $DUMP_VER against server_version_num=$SRV_NUM"

# Version-skew warning: pg_dump can dump an OLDER server, never a NEWER one.
SRV_MAJOR=$(( SRV_NUM / 10000 ))
CLI_MAJOR="$(printf '%s' "$DUMP_VER" | cut -d. -f1)"
if [ "$CLI_MAJOR" -lt "$SRV_MAJOR" ] 2>/dev/null; then
    die "pg_dump $DUMP_VER cannot dump a PostgreSQL $SRV_MAJOR server. Use the matching or newer client."
fi
[ "$CLI_MAJOR" -gt "$SRV_MAJOR" ] 2>/dev/null && \
    warn "pg_dump $DUMP_VER is newer than the server ($SRV_MAJOR); this works but is unusual."

# --- globals first (roles, grants) ------------------------------------------
GLOBALS_FILE=""
if [ "$GLOBALS" = "1" ]; then
    GLOBALS_FILE="${LOGICAL_DIR}/globals_$(date -u '+%Y%m%dT%H%M%SZ').sql"
    log "capturing cluster globals (roles, tablespaces) -> $(basename "$GLOBALS_FILE")"
    if "$PGBIN_DIR/pg_dumpall" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" --globals-only \
            -f "$GLOBALS_FILE"; then
        pg_owner_give "$GLOBALS_FILE"
        log "globals captured ($(wc -l < "$GLOBALS_FILE" | tr -d ' ') lines)"
    else
        warn "pg_dumpall --globals-only failed; the dump will NOT contain roles"
        rm -f "$GLOBALS_FILE"; GLOBALS_FILE=""
    fi
fi

# --- which databases ---------------------------------------------------------
if [ "${#DATABASES[@]}" -eq 0 ]; then
    DB_TMP="$(mktemp 2>/dev/null || echo "${BACKUP_ROOT}/.dbs.$$")"
    "$PGBIN_DIR/psql" -X -q -A -t -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
        -d postgres -c \
        "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname" \
        > "$DB_TMP"
    while IFS= read -r line; do
        [ -n "$line" ] && DATABASES+=("$line")
    done < "$DB_TMP"
    rm -f -- "$DB_TMP"
fi
[ "${#DATABASES[@]}" -gt 0 ] || die "no databases to dump"

case "$FORMAT" in
    custom)    EXT="dump" ;;
    directory) EXT="dir" ;;
    plain)     EXT="sql" ;;
    *)         die "--format must be custom, directory or plain" ;;
esac

FAILED=()
for DB in "${DATABASES[@]}"; do
    STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
    OUT="${LOGICAL_DIR}/${DB}_${STAMP}.${EXT}"
    T0="$(now_epoch)"

    ARGS=( -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DB" )
    case "$FORMAT" in
        custom)    ARGS+=( -Fc -Z "$COMPRESS_LEVEL" ) ;;
        directory) ARGS+=( -Fd -Z "$COMPRESS_LEVEL" -j "$JOBS" ) ;;
        plain)     ARGS+=( -Fp ) ;;
    esac
    [ -n "$EXCLUDE_TABLE_DATA" ] && ARGS+=( --exclude-table-data "$EXCLUDE_TABLE_DATA" )
    ARGS+=( -f "$OUT" )

    log "dumping $DB -> $(basename "$OUT") (format=$FORMAT)"
    # Redirect pg_dump's stderr to a file rather than through a pipe or a
    # process substitution: under container/PRoot shells a child holding the
    # write end of a pipe can wedge the whole script.
    PGDUMP_ERR="${LOGICAL_DIR}/.${DB}.${STAMP}.err"
    if ! "$PGBIN_DIR/pg_dump" "${ARGS[@]}" 2>"$PGDUMP_ERR"; then
        err "pg_dump FAILED for $DB"
        sed 's/^/  pg_dump: /' "$PGDUMP_ERR" >&2 || true
        rm -rf -- "$OUT"                      # never leave a truncated dump behind
        FAILED+=("$DB")
        continue
    fi

    T1="$(now_epoch)"
    DUR="$(fsub "$T1" "$T0")"

    # --- verify the dump is readable, immediately ---------------------------
    # pg_restore --list reads the archive TOC. A truncated or corrupt archive
    # fails here. This is cheap and it means you find out now, not at 3am.
    TOC_ENTRIES="unknown"
    VERIFY="not-run"
    if [ "$FORMAT" != "plain" ]; then
        if TOC="$("$PGBIN_DIR/pg_restore" --list "$OUT" 2>/dev/null)"; then
            TOC_ENTRIES="$(printf '%s\n' "$TOC" | grep -c ';' || true)"
            VERIFY="ok (pg_restore --list read $TOC_ENTRIES TOC entries)"
        else
            VERIFY="FAILED - pg_restore cannot read this dump"
            err "dump $OUT is not readable by pg_restore. Deleting it."
            rm -rf -- "$OUT"
            FAILED+=("$DB")
            continue
        fi
    else
        VERIFY="plain SQL - not TOC-checked (grep it, or psql --set ON_ERROR_STOP=1)"
    fi

    SZ="$(if [ -d "$OUT" ]; then dir_bytes "$OUT"; else wc -c < "$OUT" | tr -d ' '; fi)"
    ROWS_EST="$("$PGBIN_DIR/psql" -X -q -A -t -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DB" \
        -c "SELECT COALESCE(sum(n_live_tup),0) FROM pg_stat_user_tables" 2>/dev/null || echo unknown)"

    cat > "${OUT}.meta" <<META
PGDR_META_VERSION=1
KIND=logical
DATABASE=$DB
FORMAT=$FORMAT
DUMP_FILE=$OUT
STARTED_EPOCH=$T0
FINISHED_EPOCH=$T1
DURATION_S=$DUR
SIZE_BYTES=$SZ
SERVER_VERSION_NUM=$SRV_NUM
CLIENT_PG_DUMP=$DUMP_VER
TOC_ENTRIES=$TOC_ENTRIES
READABLE=$VERIFY
EST_LIVE_TUPLES=$ROWS_EST
GLOBALS_FILE=${GLOBALS_FILE:-none}
HOST=$PGHOST
PORT=$PGPORT
USER=$PGUSER
META
    pg_owner_give "$OUT"
    [ -n "$GLOBALS_FILE" ] && pg_owner_give "$GLOBALS_FILE"

    log "  $DB: $(fmt_bytes "$SZ") in $(fmt_dur "$DUR"), $TOC_ENTRIES TOC entries, readable=$VERIFY"
    event "KIND=logical" "DATABASE=$DB" "DUMP_FILE=$OUT" "DURATION_S=$DUR" \
          "SIZE_BYTES=$SZ" "TOC_ENTRIES=$TOC_ENTRIES"
done

prune_logical "$PRUNE_DRY_RUN"

if [ "${#FAILED[@]}" -gt 0 ]; then
    err "these databases failed to dump: ${FAILED[*]}"
    exit 1
fi

log "logical backup complete: ${#DATABASES[@]} database(s)"
cat >&2 <<'NEXT'

To restore a SINGLE OBJECT from one of these dumps (the reason we used -Fc):

  # 1. See what is in the archive without restoring anything
  pg_restore --list appdb_20260917T043000Z.dump | less

  # 2. Restore just one table into a SCRATCH database (never the live one)
  createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" appdb_scratch
  pg_restore -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d appdb_scratch \
      --no-owner --no-privileges -t orders appdb_20260917T043000Z.dump

  # 3. Copy the rows across with INSERT ... SELECT, or pg_dump that one table
  #    back out and load it where you need it.
  #
  # Remember: -t restores the TABLE DATA AND ITS DEFINITION but NOT its
  # indexes, constraints, sequences, triggers or dependent views. Recreate
  # those, or restore the whole schema into the scratch database instead.

Globals (roles/grants) are in the paired globals_<timestamp>.sql file and are
NOT inside the per-database dump.
NEXT

exit 0
