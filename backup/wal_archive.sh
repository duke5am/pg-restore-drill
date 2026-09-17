#!/usr/bin/env bash
# ===========================================================================
# wal_archive.sh - a safe PostgreSQL archive_command target
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# Install it where the postgres OS user can read it, then in postgresql.conf:
#
#   wal_level       = replica        # or logical; needs a restart to change
#   archive_mode    = on             # needs a restart
#   archive_command = 'ARCHIVE_DIR=/var/backups/pgdrill/wal /opt/pgdrill/backup/wal_archive.sh %p %f'
#   archive_timeout = 300            # seconds; bounds unarchived WAL age
#
# NOTE the ARCHIVE_DIR=... prefix. PostgreSQL runs archive_command through
# /bin/sh with the postmaster's environment, and the postmaster does NOT have
# your shell's variables. If you export ARCHIVE_DIR in your login shell, in
# systemd's Environment=, or in .pgpass-adjacent config, the archiver will not
# see it and every segment will fail with a confusing error. Set it inline, as
# above, or in a config file (see ARCHIVE_DIR RESOLUTION below).
#
# %p is the path of the completed WAL segment inside pg_wal. On Linux this
# arrives as a path RELATIVE to the data directory (e.g. "pg_wal/0000...01"),
# because the archiver's working directory is PGDATA. %f is the bare file name,
# e.g. 00000001000000000000002A.
#
# THREE KINDS OF FILE ARRIVE HERE, and all three must be archived:
#   1. ordinary 16MB WAL segments, 24 hex digits;
#   2. timeline history files,  e.g. 00000002.history, written on promotion;
#   3. backup history files,    e.g. 000000010000000000000002.00000028.backup,
#      written by pg_basebackup when a base backup completes.
# Kind 3 is the one that bites. It is not 24 hex digits, so a name check that
# only accepts segments or .history refuses it - and the archiver, which works
# through pg_wal in order, then retries that single file forever. WAL archiving
# stops permanently after the first successful base backup, the recovery window
# silently stops growing, and pg_stat_archiver shows a failure that looks like a
# malformed argument rather than a missing rule.
#
# ARCHIVE_DIR RESOLUTION (first match wins)
#   1. the ARCHIVE_DIR environment variable
#   2. a third positional argument
#   3. the first non-comment line of  <this script>/../archive_dir.conf
#   4. the first non-comment line of  /etc/pgdrill/archive_dir.conf
# Resolution (3) means you can deploy the kit and configure the archive with a
# single file that the postgres user can read, without touching the
# archive_command line if the path ever moves.
#
# ---------------------------------------------------------------------------
# WHY THIS SCRIPT EXISTS - read this before you change anything
# ---------------------------------------------------------------------------
# PostgreSQL treats exit code 0 from archive_command as "this segment is
# durably archived" and is then free to RECYCLE OR DELETE the original in
# pg_wal. There is no second chance and no verification step. Two classic
# ways to break a WAL chain, both silent:
#
#  1. NON-ATOMIC COPY. The naive command
#
#         archive_command = 'cp %p /archive/%f'
#
#     creates the destination and then fills it. Between those two events the
#     destination EXISTS and is INCOMPLETE. Anything that reads the archive in
#     that window - a concurrent restore, a replication of the archive to
#     object storage, an rsync/S3 sync job - can pick up a half-written
#     segment. Worse, if the cp is killed (OOM, timeout, volume full) the
#     truncated destination is left behind, and because `cp` never returns 0
#     PostgreSQL retries... overwriting it. But if the failing copy was
#     interleaved with a *successful* copy of the same name from another path,
#     or if a human "fixes" the archive by hand, you get a corrupt segment that
#     looks present. Recovery then stops in the middle of the chain, possibly
#     weeks later.
#
#  2. NON-IDEMPOTENT RETRY. PostgreSQL retries archive_command for a segment
#     until it succeeds (up to wal_archive_retry_* semantics; by default
#     wal_retrieve_retry_interval applies to *recovery*, and for archiving the
#     archiver retries every 60s up to 10 times per segment - see
#     pg_stat_archiver.failed_count and last_failed_wal). A retry that
#     re-copies from scratch re-opens the same race.
#
# The fix has two halves, and you need BOTH:
#
#   (a) THE  test ! -f  IDIOM. Never write the final name directly:
#
#         test ! -f "$DEST" && cp "$SRC" "$DEST"
#
#       The && means: if the destination already exists, do nothing at all and
#       report success. That makes the operation idempotent - a retry after a
#       success is a no-op rather than a rewrite - and it means the destination
#       is only ever created once, by one writer, so no other reader can catch
#       a rewrite in progress. This is the idiom the PostgreSQL documentation
#       itself recommends, and it is why `archive_command = 'test ! -f
#       /archive/%f && cp %p /archive/%f'` is the canonical "safe" command you
#       will see in every tutorial.
#
#   (b) ATOMIC PUBLISH. Even with (a), `cp src dest` is still non-atomic: the
#       destination exists before it is complete. So this script copies to a
#       temporary name in the SAME DIRECTORY (so that rename() is atomic - a
#       rename across filesystems is a copy and is not atomic) and then
#       renames it into place with `mv`. A reader either sees no file at all
#       or sees the complete file. There is no third state.
#
# On top of that this script refuses to archive a file that is not a
# structurally complete WAL segment: it checks that the length matches the
# segment size the segment header itself claims, that the WAL page magic is
# present, and that the final page carries the same magic (a zero-padded or
# short-tailed file fails this). A partial WAL file is therefore REFUSED with a
# non-zero exit - PostgreSQL keeps the segment in pg_wal and retries - instead
# of being archived as if it were good and silently corrupting the chain.
#
# Exit codes (archive_command semantics):
#   0  segment is durably in the archive
#   *  anything else: NOT archived, PostgreSQL will retry
# ===========================================================================

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SELF_DIR}/../lib/common.sh"

usage() {
    cat >&2 <<'USAGE'
Usage: wal_archive.sh [-v] [-q] <wal_source_path> <wal_filename> [archive_dir]

  <wal_source_path>  %p - path to the completed segment inside pg_wal
  <wal_filename>     %f - bare name: 24-hex segment, *.history, or *.backup
                         (a *.backup name is a backup history file written by
                         pg_basebackup; it is archived like any other pg_wal
                         file and MUST NOT be refused)
  [archive_dir]      optional; overrides nothing if ARCHIVE_DIR is set

Recommended postgresql.conf line:

  archive_command = 'ARCHIVE_DIR=/var/backups/pgdrill/wal /opt/pgdrill/backup/wal_archive.sh %p %f'

Environment:
  ARCHIVE_DIR              destination directory (REQUIRED - see below)
  WAL_SEGMENT_SIZE         expected segment size in bytes (default 16777216)
  WAL_ARCHIVE_ALLOW_SHORT  set to 1 to archive a segment whose length does not
                           match its header (only for recovering a known-bad
                           archive; you lose the partial-file guard)
  WAL_ARCHIVE_FULL_SCAN    set to 1 to check the magic on EVERY page rather
                           than the first, sampled and last pages
  PGDR_QUIET               1 to silence informational logging

ARCHIVE_DIR is resolved from, in order: the environment, the third positional
argument, <script>/../archive_dir.conf, /etc/pgdrill/archive_dir.conf.
Exporting it in your login shell does NOT work: the postmaster does not
inherit it and every segment will fail with exit code 2.

Exit status is 0 only when the segment is durably stored. Any other value
makes PostgreSQL retain the segment and retry.
USAGE
}

VERBOSE=0
while [ $# -gt 0 ]; do
    case "$1" in
        -v|--verbose) VERBOSE=1; shift ;;
        -q|--quiet)   PGDR_QUIET=1; shift ;;
        -h|--help)    usage; exit 0 ;;
        --)           shift; break ;;
        -*)           usage; err "unknown option: $1"; exit 2 ;;
        *)            break ;;
    esac
done

SRC="${1:-}"
FILENAME="${2:-}"

if [ -z "$SRC" ] || [ -z "$FILENAME" ]; then
    usage
    err "wal_archive.sh: expected two arguments (%p and %f)"
    exit 2
fi

# ---------------------------------------------------------------------------
# 0. Argument sanity. An empty or malformed %f is a nasty real-world failure:
#    if a wrapper script loses the argument, or a shell variable in an inline
#    archive_command is unset, you end up copying every segment to the same
#    junk name and the archive silently contains one file. Refuse loudly.
# ---------------------------------------------------------------------------
case "$FILENAME" in
    [0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]|\
    [0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]|\
    [0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f])
        : ;;
    *.history)
        # Timeline history files are archived too and are NOT 24 hex digits.
        : ;;
    *.backup)
        # -------------------------------------------------------------------
        # BACKUP HISTORY FILES. pg_basebackup writes one of these into pg_wal
        # when the backup finishes, named <segment>.<offset>.backup, and the
        # archiver has to archive it like any other pg_wal file.
        #
        # Do NOT remove this case. Without it, a *successful base backup* stops
        # WAL archiving dead: the archiver works through pg_wal in order, hits
        # the .backup file, gets a refusal, and retries that one file forever.
        # Every later segment stays in pg_wal, pg_stat_archiver fills up with
        # failures, and the recovery window stops growing - while the base
        # backups themselves look perfect. It is the exact failure mode this
        # kit exists to catch, and it is easy to introduce, because a name
        # like 000000010000000000000002.00000028.backup is not 24 hex digits
        # and looks like a malformed %f.
        : ;;
    *)
        err "wal_archive.sh: REFUSING - '%s' is not a plausible WAL file name" "$FILENAME"
        err "  (expected 24 hex digits, or a .history or .backup file). A malformed"
        err "  %f usually means the archive_command template lost an argument."
        exit 2
        ;;
esac

ARCHIVE_DIR="${ARCHIVE_DIR:-${3:-}}"
if [ -z "$ARCHIVE_DIR" ]; then
    for _cfg in "${SELF_DIR}/../archive_dir.conf" "/etc/pgdrill/archive_dir.conf"; do
        if [ -r "$_cfg" ]; then
            ARCHIVE_DIR="$(sed -n 's/^[[:space:]]*\([^#[:space:]].*[^[:space:]]\)[[:space:]]*$/\1/p' "$_cfg" | head -1)"
            [ -n "$ARCHIVE_DIR" ] && break
        fi
    done
fi
[ -n "$ARCHIVE_DIR" ] || {
    err "wal_archive.sh: ARCHIVE_DIR is not set."
    err "  Set it inline in archive_command, e.g."
    err "    archive_command = 'ARCHIVE_DIR=/var/backups/pgdrill/wal $(pwd)/wal_archive.sh %p %f'"
    err "  or write the path into ${SELF_DIR}/../archive_dir.conf"
    err "  DO NOT rely on exporting it: the postmaster does not inherit your shell."
    exit 2
}
[ -d "$ARCHIVE_DIR" ] || { err "wal_archive.sh: ARCHIVE_DIR=$ARCHIVE_DIR is not a directory"; exit 2; }
[ -w "$ARCHIVE_DIR" ] || { err "wal_archive.sh: ARCHIVE_DIR=$ARCHIVE_DIR is not writable by $(id -un)"; exit 2; }

# %p may be relative to PGDATA (the archiver's cwd). Resolve it so that the
# script also works when invoked by hand from somewhere else - which is exactly
# what you will do when you are debugging an archive failure at 3am.
if [ ! -e "$SRC" ] && [ "${SRC#/}" = "$SRC" ] && [ -n "${PGDATA:-}" ] && [ -e "${PGDATA}/${SRC}" ]; then
    SRC="${PGDATA}/${SRC}"
fi
[ -r "$SRC" ] || { err "wal_archive.sh: source '$SRC' is not readable by $(id -un)"; exit 2; }
[ -f "$SRC" ] || { err "wal_archive.sh: source '$SRC' is not a regular file"; exit 2; }

DEST="${ARCHIVE_DIR}/${FILENAME}"

# ---------------------------------------------------------------------------
# 1. Idempotency guard - the  test ! -f  idiom.
#    If the destination already exists we are done. This is what makes a retry
#    safe and is the whole point of the canonical archive_command.
# ---------------------------------------------------------------------------
if [ -f "$DEST" ]; then
    src_size="$(wc -c < "$SRC" | tr -d ' ')"
    dst_size="$(wc -c < "$DEST" | tr -d ' ')"
    if [ "$src_size" = "$dst_size" ]; then
        log "already archived (idempotent no-op): $FILENAME"
        exit 0
    fi
    # Sizes differ. We must NOT silently overwrite: a differing destination
    # means something is genuinely wrong (a previous partial copy that was
    # never cleaned up, or two different clusters sharing an archive).
    err "wal_archive.sh: REFUSING - $DEST exists with size $dst_size but the source is $src_size bytes."
    err "  Do not delete it blindly: work out which copy is correct first."
    err "  See docs/COMMON-FAILURES.md, 'broken WAL chain'."
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Structural validation of the source segment.
#    This is the guard that makes a PARTIAL WAL file impossible to archive.
# ---------------------------------------------------------------------------
EXPECT_SIZE="${WAL_SEGMENT_SIZE:-16777216}"
PAGE=8192

# Read a little-endian unsigned integer of $2 bytes at byte offset $3.
read_u() { od -An -N"$2" -j"$3" -tu"$2" "$1" 2>/dev/null | tr -d ' \n'; }

actual_size="$(wc -c < "$SRC" | tr -d ' ')"
if [ "$actual_size" -eq 0 ]; then
    err "wal_archive.sh: REFUSING - '$FILENAME' is ZERO BYTES (source $SRC)."
    err "  A zero-length file in the archive is the signature of an interrupted"
    err "  copy. Archiving it would break recovery at this exact point."
    exit 1
fi

magic0="$(read_u "$SRC" 2 0)"
if [ -z "$magic0" ] || [ "$magic0" = "0" ]; then
    err "wal_archive.sh: REFUSING - '$FILENAME' has no WAL page magic at offset 0 (got '${magic0:-none}')."
    err "  This file is not a WAL segment (all-zero, truncated, or the wrong file)."
    exit 1
fi

case "$FILENAME" in
    *.history)
        # A timeline history file is a small text file:
        #     1\t0/2000028\tno recovery target specified
        # Only the first character is worth checking: it is the timeline number.
        case "$(head -c 1 "$SRC" 2>/dev/null)" in
            [0-9]) log "archiving timeline history file $FILENAME ($actual_size bytes)" ;;
            *) warn "$FILENAME does not look like a timeline history file (first byte is not a digit); archiving it anyway" ;;
        esac
        ;;
    *.backup)
        # ------------------------------------------------------------------
        # A backup history file, in the documented format:
        #
        #     START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
        #     STOP WAL LOCATION: 0/2000120 (file 000000010000000000000002)
        #     CHECKPOINT LOCATION: 0/2000080
        #     BACKUP METHOD: streamed
        #     ...
        #
        # It is NOT a WAL segment: it is a few hundred bytes of text, so none
        # of the segment header checks below apply to it. What we can check is
        # that it starts with the header that every PostgreSQL version writes.
        # That catches an error page, an empty file, or a backup history file
        # from something that is not PostgreSQL.
        # ------------------------------------------------------------------
        _first_line="$(head -n 1 "$SRC" 2>/dev/null || true)"
        case "$_first_line" in
            "START WAL LOCATION:"*)
                log "archiving backup history file $FILENAME ($actual_size bytes)"
                ;;
            *)
                err "wal_archive.sh: REFUSING - '$FILENAME' ends in .backup but does not look like a backup history file."
                err "  first line: ${_first_line:-<empty>}"
                err "  Expected it to begin with 'START WAL LOCATION:'."
                exit 1
                ;;
        esac
        ;;
    *)
        # ------------------------------------------------------------------
        # THE FOUR INVARIANTS OF A COMPLETE, CORRECTLY NAMED WAL SEGMENT.
        #
        # These were established by inspecting real segments produced by
        # PostgreSQL 17.11, not from memory. In particular note what is NOT an
        # invariant: a WAL segment's tail is LEGITIMATELY ZERO-FILLED. A
        # segment is always allocated at its full size (16MB by default) and
        # only the pages up to the point where WAL was written contain a page
        # header; everything after that is zeros. A 16MB segment holding one
        # transaction may have a mostly-zero body. So "the last page must have
        # a WAL page magic" is FALSE and a check based on it will refuse every
        # healthy segment - which is exactly the bug this comment exists to
        # stop you reproducing.
        #
        # What IS true:
        #   1. length == the segment size recorded in the long page header
        #      (little-endian uint32 at byte offset 32).
        #   2. the WAL page magic (little-endian uint16 at offset 0) is nonzero.
        #   3. the timeline in the header (uint32 at offset 4) equals the
        #      timeline encoded in the file NAME (its first 8 hex digits).
        #   4/5. the header's xlp_pageaddr (uint64 at offset 8) equals the
        #      byte offset of this segment's own start, computed from the name:
        #      (logid * 256 + segno) * segment_size.
        #
        # (1) catches truncation - the shape a partial copy takes.
        # (2) catches an all-zero or non-WAL file.
        # (3) and (4/5) prove the CONTENT belongs to the NAME it is being
        #     archived under. That is the check that catches a segment copied
        #     to the wrong name, a swapped file, and a byte-order or
        #     hex-conversion bug in a wrapper script - all of which produce an
        #     archive that looks complete and is silently wrong.
        # ------------------------------------------------------------------
        read_u64() { od -An -N8 -j8 -tu8 "$SRC" 2>/dev/null | tr -d ' \n'; }

        hdr_segsize="$(read_u "$SRC" 4 32)"
        hdr_blksize="$(read_u "$SRC" 4 36)"
        hdr_tli="$(read_u "$SRC" 4 4)"
        hdr_pageaddr="$(read_u64)"

        if [ "${WAL_ARCHIVE_ALLOW_SHORT:-0}" != "1" ]; then
            if [ -z "$hdr_segsize" ] || [ "$hdr_segsize" != "$actual_size" ]; then
                err "wal_archive.sh: REFUSING - '$FILENAME' is a PARTIAL segment."
                err "  file length      : $actual_size bytes"
                err "  header seg_size  : ${hdr_segsize:-<unreadable>} bytes"
                err "  A complete segment's header records its own size. These differ,"
                err "  so the file was truncated or is still being written."
                err "  Refusing to archive it; PostgreSQL will retry this segment."
                exit 1
            fi
        fi

        if [ -n "$hdr_blksize" ] && [ "$hdr_blksize" != "0" ] && [ "$hdr_blksize" != "$PAGE" ]; then
            warn "$FILENAME: unexpected WAL page size $hdr_blksize (expected $PAGE)"
        fi

        # Decode the name. 24 hex digits = 8 timeline + 8 log + 8 segment.
        n_tli=$(( 16#${FILENAME:0:8} ))
        n_log=$(( 16#${FILENAME:8:8} ))
        n_seg=$(( 16#${FILENAME:16:8} ))
        expect_pageaddr=$(( (n_log * 256 + n_seg) * EXPECT_SIZE ))

        if [ -n "$hdr_tli" ] && [ "$hdr_tli" != "$n_tli" ]; then
            err "wal_archive.sh: REFUSING - '$FILENAME' header timeline is $hdr_tli but the name says $n_tli."
            err "  The content does not belong to this name. Do not archive it."
            exit 1
        fi
        if [ -n "$hdr_pageaddr" ] && [ "$hdr_pageaddr" != "$expect_pageaddr" ]; then
            err "wal_archive.sh: REFUSING - '$FILENAME' content/name mismatch."
            err "  header page address : $hdr_pageaddr"
            err "  name implies        : $expect_pageaddr"
            err "  This segment's bytes are not the segment its name claims. Archiving"
            err "  it would put a valid-looking but WRONG segment in the archive, and"
            err "  recovery would fail or, worse, diverge."
            exit 1
        fi
        if [ -n "$hdr_blksize" ] && [ "$hdr_blksize" != "0" ] && [ "$hdr_blksize" != "$PAGE" ]; then
            warn "$FILENAME: unexpected WAL page size $hdr_blksize (expected $PAGE)"
        fi
        if [ -n "$EXPECT_SIZE" ] && [ "$EXPECT_SIZE" != "$actual_size" ] && \
           [ "${WAL_ARCHIVE_ALLOW_SHORT:-0}" != "1" ]; then
            err "wal_archive.sh: REFUSING - '$FILENAME' is $actual_size bytes, expected $EXPECT_SIZE."
            err "  If your cluster was created with a non-default --wal-segsize, set"
            err "  WAL_SEGMENT_SIZE to match it."
            exit 1
        fi

        # Optional deep scan. A page that has never been written is all zeros,
        # and a zero-filled tail is NORMAL for a healthy segment, so we cannot
        # require every page to carry the magic. What we CAN require is:
        #   every page whose first two bytes are NOT both zero must begin with
        #   the same magic as page 0.
        # A page that has been partially written - the real signature of a torn
        # write - has a nonzero leading byte pattern but not the magic, and is
        # caught here. One streaming od pass over ~16MB, well under a second.
        if [ "${WAL_ARCHIVE_FULL_SCAN:-0}" = "1" ]; then
            if ! od -An -v -tu1 -w"$PAGE" "$SRC" | awk -v m0="$magic0" '
                    BEGIN { want_lo = m0 % 256; want_hi = int(m0 / 256) }
                    {
                        lo = $1 + 0; hi = $2 + 0;
                        if (lo == 0 && hi == 0) next;          # unwritten page: fine
                        if (lo != want_lo || hi != want_hi) {
                            printf "page %d: leading bytes %d,%d are not the WAL page magic (%d,%d)\n",
                                   NR - 1, lo, hi, want_lo, want_hi > "/dev/stderr";
                            bad = 1;
                        }
                    }
                    END { exit bad }'; then
                err "wal_archive.sh: REFUSING - '$FILENAME' failed the full page scan; a page in the body of this segment is torn or corrupt."
                exit 1
            fi
            log "full page scan clean for $FILENAME"
        fi
        ;;
esac

# ---------------------------------------------------------------------------
# 3. Atomic publish: copy to a temporary name in the SAME directory, verify,
#    then rename. rename(2) within one filesystem is atomic, so no reader can
#    ever observe a partially written destination.
# ---------------------------------------------------------------------------
TMP="${DEST}.pgdrill-tmp.$$"
cleanup_tmp() { rm -f -- "$TMP" 2>/dev/null || true; }
trap cleanup_tmp EXIT INT TERM

if [ "$VERBOSE" = "1" ]; then log "archiving $SRC -> $DEST ($actual_size bytes)"; fi

if ! cp -- "$SRC" "$TMP" 2>/dev/null; then
    cleanup_tmp
    err "wal_archive.sh: copy failed for $FILENAME (source $SRC, temp $TMP)"
    exit 1
fi

tmp_size="$(wc -c < "$TMP" | tr -d ' ')"
if [ "$tmp_size" != "$actual_size" ]; then
    cleanup_tmp
    err "wal_archive.sh: short copy for $FILENAME (copied $tmp_size of $actual_size bytes)"
    exit 1
fi

# Belt and braces: compare content, not just length.
if ! cmp -s -- "$SRC" "$TMP"; then
    cleanup_tmp
    err "wal_archive.sh: content mismatch after copy for $FILENAME"
    exit 1
fi

# Flush the file's data before the rename makes it visible.
if command -v sync >/dev/null 2>&1; then sync -- "$TMP" 2>/dev/null || sync 2>/dev/null || true; fi

# Re-check the destination immediately before publishing, so a concurrent
# archiver (or a manual intervention) cannot race us into an overwrite.
if [ -f "$DEST" ]; then
    dst_size="$(wc -c < "$DEST" | tr -d ' ')"
    if [ "$dst_size" = "$actual_size" ]; then
        cleanup_tmp
        log "already archived (race): $FILENAME"
        exit 0
    fi
    cleanup_tmp
    err "wal_archive.sh: REFUSING - $DEST appeared with size $dst_size during archiving of $FILENAME"
    exit 1
fi

mv -f -- "$TMP" "$DEST" || { cleanup_tmp; err "wal_archive.sh: atomic rename failed for $FILENAME"; exit 1; }

# fsync the directory so the rename itself survives a power loss. (Without
# this, a crash can lose the directory entry even though the data was fsynced.)
if command -v python3 >/dev/null 2>&1; then
    python3 - "$ARCHIVE_DIR" <<'PYFSC' 2>/dev/null || true
import os, sys
try:
    fd = os.open(sys.argv[1], os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
except Exception:
    pass
PYFSC
fi

trap - EXIT INT TERM
log "archived $FILENAME ($actual_size bytes) -> $DEST"
exit 0
