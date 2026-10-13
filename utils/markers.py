"""Completion markers: resume state that lives WITH the data, not beside the client.

The ledger (``utils.ledger``) is a SQLite file in the client package's ``state/``
dir. Unpack a fresh copy of the client, move the package, or wipe ``state/``, and
a finished 4 TB download starts over. The ledger also cannot be put in the output
dir: SQLite in WAL mode needs shm mmap and explicitly disclaims network
filesystems, which is exactly where these datasets live (GPFS, Lustre, NFS).

So completion is *also* recorded as one empty file per finished trip:

    <output_dir>/.natix/done/<scope_digest>/<trip_id>_<sub_trip>_<grade>[_<segment>]

The grade is always spelled out. The ``_<segment>`` suffix is present only for one
segment of a chopped (``seiData: with_and_chop``) trip, which is delivered — and
certified — as its own folder; every other marker keeps the three-part name it
has always had, so markers written before segments existed still parse.

Every operation is a direct path op — ``open(O_CREAT|O_EXCL)``, ``unlink``,
``scandir`` of one directory — all of which are atomic on network filesystems,
with no shared file to append to and no lock to take.

**The digest is a directory, not a filename suffix.** A scope is
(output dir, delivery target, dry mode, camera selection): the same tuple the
ledger name is keyed on, because it decides *what a completed trip contains*.
Trips of every scope share one directory tree, so publishing for scope B
replaces what scope A completed, and A's claim has to be revoked. With the digest
as a directory that revocation is ``unlink(done/<A>/<key>)`` — a known path per
scope. With the digest in the filename it would be a glob per trip, i.e. a scan
of the whole marker set per published trip.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import quote, unquote

from utils.ledger import LegacyCompletion
from utils.path_helpers import SEGMENT_LABEL_RE, TripKey, _segment_suffix

logger = logging.getLogger(__name__)

# Inside the client's dot-state dir in the output, so QA skips it and it travels
# with the data it describes.
MARKERS_DIRNAME = "done"
# Output-wide, one-shot boundary for importing pre-marker ledgers. See
# ``import_legacy_ledgers``.
MIGRATED_SENTINEL = ".migrated"


def markers_root(output_dir: Path, state_dirname: str = ".natix") -> Path:
    return output_dir / state_dirname / MARKERS_DIRNAME


def trip_key_name(
    trip_id: str, sub_trip: int, health_grade: int, segment: str = ""
) -> str:
    """Filename for one trip's (or segment's) marker. Mirrors the ledger's key.

    Percent-encoded, not sanitised. ``safe_name`` maps several characters onto
    ``_``, so ``a+b`` and ``a_b`` produce the same filename and reading it back
    yields whichever the encoding happens to spell — one trip's completion would
    certify a different trip. Percent-encoding is reversible, so the key that
    comes back is the key that went in, and a UUID (what these ids are in
    practice) is left exactly as it is.
    """
    return (
        f"{quote(trip_id, safe='')}_{int(sub_trip)}_{int(health_grade)}"
        f"{_segment_suffix(segment)}"
    )


def parse_key_name(name: str) -> TripKey | None:
    """Inverse of :func:`trip_key_name`; ``None`` for anything else in the dir.

    ``quote`` leaves ``_`` unencoded, so a trip_id can contain underscores and
    the name is split from the right. The grade is always present and numeric,
    so an alphabetic last part can only be a segment label.
    """
    parts = name.rsplit("_", 3)
    segment = ""
    if len(parts) == 4 and SEGMENT_LABEL_RE.fullmatch(parts[3]):
        segment = parts.pop()
    else:
        parts = name.rsplit("_", 2)
    if len(parts) != 3:
        return None
    trip_id, sub_trip, health_grade = parts
    try:
        return TripKey(unquote(trip_id), int(sub_trip), int(health_grade), segment)
    except ValueError:
        return None


def scope_digests(output_dir: Path) -> list[str]:
    """Every scope that has ever recorded markers in this output dir."""
    root = markers_root(output_dir)
    if not root.is_dir():
        return []
    with os.scandir(root) as entries:
        return [e.name for e in entries if not e.name.startswith(".") and e.is_dir()]


def completed_keys(output_dir: Path, digest: str) -> set[TripKey]:
    """Trip keys this scope has published into this output dir. One scandir."""
    scope_dir = markers_root(output_dir) / digest
    if not scope_dir.is_dir():
        return set()
    keys: set[TripKey] = set()
    with os.scandir(scope_dir) as entries:
        for entry in entries:
            if entry.name.startswith("."):
                continue
            key = parse_key_name(entry.name)
            if key is not None:
                keys.add(key)
    return keys


def existing_trip_dir_names(output_dir: Path) -> set[str]:
    """Every trip directory name under ``output_dir``, from region-level listings.

    Trips live at ``<out>/<country>/<trip>`` or ``<out>/United States/<state>/
    <trip>`` — never deeper — so this lists the root, each country and each US
    state, and nothing below: a handful of listings for a whole dataset, not a
    walk. The names are what a marker is checked against, because a marker says
    "delivered" and a directory that is not there says otherwise.
    """
    if not output_dir.is_dir():
        return set()

    def plain_dirs(parent: Path) -> list[os.DirEntry]:
        try:
            with os.scandir(parent) as it:
                return [e for e in it if not e.name.startswith(".") and e.is_dir()]
        except OSError:
            return []

    names: set[str] = set()
    for country in plain_dirs(output_dir):
        children = plain_dirs(Path(country.path))
        if country.name == "United States":
            for state in children:
                names.update(e.name for e in plain_dirs(Path(state.path)))
            # A US trip filed with no state sits directly under the country too.
        names.update(e.name for e in children)
    return names


def drop_markers_without_a_directory(
    output_dir: Path, digest: str, keys: set[TripKey]
) -> set[TripKey]:
    """Return the markers still backed by a trip directory; unlink the rest.

    A marker certifies a delivery; if someone removed the trip's directory by
    hand (or a run of another kind did, without revoking), the marker outlives
    the data and start-up would skip a trip that is not there. The check is the
    directory's existence, from the region-level listings above — one name
    lookup per marker, no stat per trip.
    """
    if not keys:
        return keys
    present = existing_trip_dir_names(output_dir)
    backed: set[TripKey] = set()
    stale: list[TripKey] = []
    for key in keys:
        name = key.dir_name
        if name in present:
            backed.add(key)
        else:
            stale.append(key)
    for key in stale:
        try:
            (markers_root(output_dir) / digest / trip_key_name(*key)).unlink()
        except (FileNotFoundError, NotADirectoryError):
            pass
    if stale:
        logger.warning(
            "%d completion marker(s) name trips whose directory is no longer in %s; "
            "dropped, those trips will be fetched again.",
            len(stale),
            output_dir,
        )
    return backed


def mark_complete(
    output_dir: Path,
    digest: str,
    key: TripKey,
    files: "list[str] | tuple[str, ...]" = (),
    *,
    country: str = "",
    state: str = "",
) -> None:
    """Record that this scope has delivered ``key``. Idempotent.

    Called by the worker at the point it decides the trip is COMPLETE — after the
    S3 upload commits when uploading, after publish otherwise — never merely after
    publish: an upload can still fail and roll back, and the digest names the S3
    target, so a post-publish marker would claim a delivery that never happened.

    The marker's content is the evidence behind the claim: where the trip lives
    (``country``/``state``, which decide its directory) and the trip-relative
    files delivered, one per line. A ledger rebuilt from markers gets both back,
    so its rows can be checked against the data instead of taken on trust —
    and checked in the right directory, not under ``unknown/``.
    """
    scope_dir = markers_root(output_dir) / digest
    scope_dir.mkdir(parents=True, exist_ok=True)
    path = scope_dir / trip_key_name(*key)
    if path.exists():
        return
    # Written in full to a private temp file, then linked into place, so the
    # marker's name never exists without its evidence behind it. A kill between
    # create and write used to leave an empty marker that import skipped (it
    # existed) and a rebuild then dropped (it proved nothing); a short write
    # left a truncated inventory that a rebuild trusted. `link` keeps the
    # first-writer-wins exclusivity that O_EXCL gave.
    import tempfile  # noqa: PLC0415

    fd, tmp = tempfile.mkstemp(prefix=".marker-", dir=scope_dir)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(("\n".join([f"@{country}\t{state}", *files]) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            pass
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def marker_evidence(
    output_dir: Path, digest: str, key: TripKey
) -> tuple[str, str, list[str]]:
    """``(country, state, trip-relative files)`` a marker records.

    Empty strings and an empty list for a marker with no content (written before
    markers carried evidence); such a completion cannot be verified and a
    rebuilt ledger will drop it rather than trust it.
    """
    try:
        text = (markers_root(output_dir) / digest / trip_key_name(*key)).read_text("utf-8")
    except OSError:
        return "", "", []
    lines = [line for line in text.splitlines() if line]
    country = state = ""
    if lines and lines[0].startswith("@"):
        country, _, state = lines.pop(0)[1:].partition("\t")
    return country, state, lines


def marker_files(output_dir: Path, digest: str, key: TripKey) -> list[str]:
    """The trip-relative files a marker records; empty for a marker that has none."""
    return marker_evidence(output_dir, digest, key)[2]


def invalidate_claims(output_dir: Path, key: TripKey, digests: list[str]) -> None:
    """Revoke the listed scopes' claims on ``key``, before its directory changes.

    ``_publish_trip`` overwrites the trip directory, so whatever another scope
    published there is gone. Its marker must go first: a kill between the two
    leaves no marker, and an absent marker only ever costs a re-download.

    **Fails closed.** An unlink that fails for any reason other than "it is not
    there" raises, so the caller abandons the trip instead of publishing over
    data whose marker still vouches for it. A shared output dir where another
    user owns the marker directory is exactly that case: the data can be
    replaced while the claim cannot be revoked, and continuing would leave that
    scope skipping a trip it no longer has.
    """
    root = markers_root(output_dir)
    name = trip_key_name(*key)
    for digest in digests:
        try:
            (root / digest / name).unlink()
        except (FileNotFoundError, NotADirectoryError):
            pass


def is_migrated(output_dir: Path) -> bool:
    return (markers_root(output_dir) / MIGRATED_SENTINEL).exists()


def import_legacy_ledgers(
    output_dir: Path,
    ledgers: "list[tuple[str, dict[TripKey, LegacyCompletion], set[TripKey]]]",
) -> int:
    """One-shot, output-wide import of pre-marker resume state.

    ``ledgers`` is ``(scope_digest, {completed key: its delivered files}, all keys
    mentioned)`` for EVERY scope whose ledger describes this output dir. All of them are imported in one go, before any trip
    is published, and legacy import is then disabled for this output dir for good.

    Per-scope migration was tried first and is wrong: a scope that had not yet
    migrated could import its pre-upgrade ledger *after* another scope had already
    replaced a trip's directory, resurrecting a claim on data that is no longer
    there. Importing everything at the one moment when nothing newer than the
    ledgers can exist removes that window.

    **A trip any other scope has touched is not imported at all.** Before markers
    existed nothing revoked a ledger's claim when another scope replaced a trip,
    so two pre-upgrade ledgers can disagree about one trip while only the last one
    written is on disk — and neither records when it published. "Touched" is
    deliberately wider than "claimed complete": a scope killed part-way through
    replacing a trip leaves an in-progress or partial row, which is not a
    competing claim but is still evidence that the data changed under the older
    scope. Those trips get no marker and are re-fetched. Trips only one scope
    knows about are unambiguous and keep their progress, which is the ordinary
    single-scope case.

    **And a completion is imported only when its files are still on disk.** The
    ledgers visible here are the ones in THIS copy of the client's ``state/``;
    another copy, or a copy whose state was lost, leaves no trace to contest a
    claim with. So the absence of a competing record proves nothing, and the row
    is checked against the data instead: every file the ledger recorded for that
    trip must exist. A copy that replaced an all-camera delivery with a
    front-only one leaves the directory standing with the rear files gone, which
    is exactly what this catches and "no competing mention" does not.

    Idempotent (``O_EXCL``, EEXIST ignored), so an interrupted import is simply
    re-run by the next start-up; the sentinel is written last.
    """
    root = markers_root(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    contested: set[TripKey] = set()
    for digest, delivered, _mentioned in ledgers:
        for other_digest, _other, other_mentioned in ledgers:
            if other_digest == digest:
                continue
            contested |= delivered.keys() & other_mentioned
    if contested:
        logger.warning(
            "%d trip(s) are claimed by one delivery scope's existing resume state "
            "and also touched by another's; only the last one published is on "
            "disk and the records do not say which, so they will be fetched again.",
            len(contested),
        )
    imported = 0
    unbacked = 0
    for digest, delivered, _mentioned in ledgers:
        for key, files in delivered.items():
            if key in contested:
                continue
            # No recorded files means nothing to check against, so nothing is
            # proven; the owning scope's own ledger still resumes it.
            if not files.paths or not all(path.exists() for path in files.paths):
                unbacked += 1
                continue
            mark_complete(
                output_dir,
                digest,
                key,
                files.rel_paths,
                country=files.country,
                state=files.state,
            )
            imported += 1
    if unbacked:
        logger.info(
            "%d pre-marker completion(s) name files that are not in %s; they are "
            "not recorded as delivered and will be fetched again.",
            unbacked,
            output_dir,
        )
    sentinel = root / MIGRATED_SENTINEL
    tmp = root / f"{MIGRATED_SENTINEL}.tmp"
    tmp.write_text("1\n", encoding="utf-8")
    os.replace(tmp, sentinel)
    return imported
