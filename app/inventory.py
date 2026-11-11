"""Inventory validation: a delivered dataset against its manifest export.

The export says which files must exist (:func:`expected_files`, built with the
shipped path builders, the same rule a download writes by); each target says
which files do exist (:func:`list_target`); :func:`diff` compares the two. A
target is a local directory or ``s3://bucket[/prefix]``; an uploader delivery
spans several, and a ``Placement`` says which one each unit belongs to.

Memory: manifests stream from the export one at a time and listings stream from
their targets, so no code path holds every manifest or a whole listing. Only the
expected set is held (paths as ``str``, ``ExpectedFile`` with slots), plus, while
diffing, the first target each expected path was found in and the findings
themselves. That is about 0.5 KB per expected file at peak: ≈2 GB at 5k hours,
≈12 GB at 30k hours.

Ships to customers (nxdl): never import ``app.uploader`` here.
"""

import argparse
import os
import sys
from collections.abc import Callable, Container, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from app.manifest_coverage import expected_minute_dirs, build_expected_trip_files
from app.manifest_export import ExportError, ManifestExport, open_export
from app.validation import ALLOWED_DATASET_ROOT_FILENAMES, DATASET_REQUEST_FILENAME
from schemas.camera_selection import CAMERA_SELECTION
from schemas.dataset_request import DatasetOutput
from schemas.trip_manifest import TripManifest
from utils.path_helpers import (
    TripKey,
    build_trip_directory_path,
    is_camera_allowed,
    resolve_camera_selection,
)
from utils.s3_uploader import get_s3_client, parse_s3_uri
from utils.settings import settings
from utils.trip_selection import TripSelectionMap, load_trips_selection, segment_units

# The target a unit belongs to, or None when there is no placement rule for it.
Placement = Callable[[TripManifest, TripKey], str | None]

# The completion signal, at a target's root. It and the other files a dataset root
# may hold (ALLOWED_DATASET_ROOT_FILENAMES), the client's transients and the report
# dir are bookkeeping: never expected, never extra.
SIGNAL = DATASET_REQUEST_FILENAME
_BOOKKEEPING_DIRS = ("inventory/",)


def _is_transient(path: str) -> bool:
    """The client's own state: any dot component (``.natix/``, ``.replaced-*``,
    ``.copying-*``; as ``app.validation`` skips them) and ``*.part`` downloads."""
    return path.endswith(".part") or any(
        part.startswith(".") for part in path.split("/")
    )

# Why a unit is held, most final first, in the order the writers hold by: an
# archived file never comes without a restore, an anonymizing video comes on its
# own, a restoring file comes once the restore lands, and a video sent with no link
# and no hold status is held by the writers as pending too.
_HELD_REASONS = ("archived", "anonymizing", "restoring", "unavailable")
_VIDEO_HOLDS = ("anonymizing", "restoring", "archived")
_ARCHIVE_STATUSES = ("restoring", "archived")


@dataclass(slots=True)
class ExpectedFile:
    path: str  # posix, relative to the target root
    unit: TripKey
    # The unit's held reason (one of _HELD_REASONS), or None. diff reports the unit
    # held only while some of its files are found nowhere.
    held: str | None
    target: str | None  # the placement's answer for the unit


@dataclass(slots=True)
class ExpectedSet:
    files: dict[str, ExpectedFile]
    # "<dir>/" prefixes the TRIPS_FILE left out: a trip outside it, a segment its
    # minutes drop, an unselected minute of a kept unit. Empty without a selection.
    ignored_prefixes: frozenset[str]
    # unit -> (unit dir, held reason), one entry per unit with a held reason
    held_units: dict[TripKey, tuple[str, str]]
    # Manifests the export held (the header's count is what was requested).
    exported: int | None = None
    # TRIPS_FILE trips the export does not hold, in file order.
    absent_selected: list[TripKey] = field(default_factory=list)
    # Built for a dry download: a present "<x>.mp4.link" then stands for "<x>.mp4".
    dry_mode: bool = False


@dataclass(slots=True)
class TargetSummary:
    files: int = 0  # every listed file, bookkeeping included
    signal: bool = False


@dataclass
class InventoryReport:
    dataset_id: str
    exported_at: str
    trips: int
    expected: int
    targets: dict[str, TargetSummary]
    missing: list[str] = field(default_factory=list)
    extra: list[tuple[str, str]] = field(default_factory=list)  # (target, path)
    # (target, path, expected_target)
    misplaced: list[tuple[str, str, str]] = field(default_factory=list)
    duplicated: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    held: list[tuple[str, str]] = field(default_factory=list)  # (unit_dir, reason)
    signal_missing: list[str] = field(default_factory=list)  # targets
    exported: int | None = None  # manifests the export holds; None when unknown
    absent_selected: list[TripKey] = field(default_factory=list)

    @property
    def incomplete_export(self) -> bool:
        """The server could not serve some requested trips (``trips`` > manifests)."""
        return self.exported is not None and self.exported < self.trips

    @property
    def exit_code(self) -> int:
        """0 when nothing was found and nothing is held, else 1.

        2 (an unreadable export or target) is the caller's to return.
        """
        findings = (
            self.missing,
            self.extra,
            self.misplaced,
            self.duplicated,
            self.held,
            self.signal_missing,
        )
        return 1 if any(findings) or self.incomplete_export else 0


def expected_files(
    export: ManifestExport,
    *,
    camera_selection: CAMERA_SELECTION,
    selection: TripSelectionMap | None,
    placement: Placement | None = None,
    dry_mode: bool = False,
) -> ExpectedSet:
    """The files the export says the delivery holds, keyed by relative posix path.

    One pass over ``export.manifests()``. Each trip is split into the units it is
    delivered as (``segment_units``, under the TRIPS_FILE ``selection``), and each
    unit's files come from ``build_expected_trip_files``, as a download lays them
    out. ``placement`` is asked once per unit, while its manifest is in hand.
    """
    output = export.output
    files: dict[str, ExpectedFile] = {}
    ignored: set[str] = set()
    held_units: dict[TripKey, tuple[str, str]] = {}
    exported = 0
    seen: set[TripKey] = set()
    for manifest in export.manifests():
        exported += 1
        parent = TripKey(manifest.trip_id, manifest.sub_trip, manifest.health_grade)
        if selection is not None:
            seen.add(parent)
        if selection is not None and parent not in selection:
            ignored.update(
                f"{_unit_dir(manifest, key)}/"
                for key, _unit in segment_units(parent, manifest)
            )
            continue
        minutes = selection[parent] if selection is not None else None
        units = segment_units(parent, manifest, minutes)
        if minutes is not None:
            ignored.update(_unselected_prefixes(parent, manifest, units))
        for key, unit in units:
            unit_dir = _unit_dir(manifest, key)
            held = _held_reason(unit, output, camera_selection)
            if held is not None:
                held_units[key] = (unit_dir, held)
            target = placement(unit, key) if placement is not None else None
            for rel in build_expected_trip_files(
                unit, output, dry_mode=dry_mode, camera_selection=camera_selection
            ):
                path = f"{unit_dir}/{rel.as_posix()}"
                files[path] = ExpectedFile(path, key, held, target)
    absent = [key for key in selection or () if key not in seen]
    return ExpectedSet(
        files, frozenset(ignored), held_units, exported, absent, dry_mode
    )


def list_target(target: str) -> Iterator[tuple[str, int]]:
    """Every file under ``target`` as ``(relative posix path, size)``, streamed.

    ``target`` is a local directory (walked) or ``s3://bucket[/prefix]`` (one
    ``list_objects_v2`` sweep). Bookkeeping files are yielded; :func:`diff`
    classifies them. A target that cannot be listed raises (``OSError``, or the
    S3 client's error) rather than reading as empty.
    """
    if target.startswith("s3://"):
        yield from _list_s3(target)
    else:
        yield from _list_dir(target)


def diff(
    expected: ExpectedSet,
    listings: dict[str, Iterable[tuple[str, int]]],
    export: ManifestExport,
    *,
    require_signal: bool,
) -> InventoryReport:
    """Compare the expected set with each target's listing.

    Each listing is consumed once and none of it is kept beyond the findings.
    Only when the expected set was built in dry mode does a present
    ``<path>.mp4.link`` count as the presence of an expected ``<path>.mp4``, as
    ``app.validation`` reads a dry download; otherwise a ``.link`` is just a file
    (extra) and the video is missing. Nothing under an
    ignored prefix is extra. A unit with a held reason is held
    only while some of its files are found nowhere: the writers check "already
    complete" before any hold, so a unit found whole is complete. While held, none
    of its files is missing and nothing under its dir is extra. With
    ``require_signal``, a target holding an expected file but no root
    ``dataset_request.json`` is reported.
    """
    files = expected.files
    held_dirs = {f"{d}/": key for key, (d, _reason) in expected.held_units.items()}
    report = InventoryReport(
        dataset_id=export.dataset_id,
        exported_at=export.exported_at,
        trips=export.trips,
        expected=len(files),
        targets={},
        exported=expected.exported,
        absent_selected=expected.absent_selected,
    )
    found: dict[str, str] = {}  # expected path -> the first target holding it
    duplicated: dict[str, list[str]] = {}
    # Unexpected files under a held unit's dir: extra only if the unit is complete.
    strays: list[tuple[str, str, TripKey]] = []
    for target, listing in listings.items():
        summary = TargetSummary()
        holds_expected = False
        for path, _size in listing:
            summary.files += 1
            if path == SIGNAL:
                summary.signal = True
                continue
            if (
                path in ALLOWED_DATASET_ROOT_FILENAMES
                or path.startswith(_BOOKKEEPING_DIRS)
                or _is_transient(path)
            ):
                continue
            entry = files.get(path)
            if entry is None and expected.dry_mode and path.endswith(".mp4.link"):
                # A dry download leaves the link where the video would be.
                entry = files.get(path[: -len(".link")])
            if entry is None:
                if _matching_prefix(path, expected.ignored_prefixes) is not None:
                    continue
                held_dir = _matching_prefix(path, held_dirs)
                if held_dir is None:
                    report.extra.append((target, path))
                else:
                    strays.append((target, path, held_dirs[held_dir]))
                continue
            holds_expected = True
            if entry.target is not None and entry.target != target:
                report.misplaced.append((target, entry.path, entry.target))
            first = found.setdefault(entry.path, target)
            if first != target:
                duplicated.setdefault(entry.path, [first]).append(target)
        report.targets[target] = summary
        if require_signal and holds_expected and not summary.signal:
            report.signal_missing.append(target)

    still_held: set[TripKey] = set()
    for path, entry in files.items():
        if path in found:
            continue
        if entry.held is None:
            report.missing.append(path)
        else:
            still_held.add(entry.unit)
    report.missing.sort()
    report.extra.extend(
        (target, path) for target, path, unit in strays if unit not in still_held
    )
    report.extra.sort()
    report.misplaced.sort()
    report.duplicated = sorted((path, tuple(ts)) for path, ts in duplicated.items())
    report.held = sorted(
        held for key, held in expected.held_units.items() if key in still_held
    )
    return report


def _unit_dir(manifest: TripManifest, key: TripKey) -> str:
    return build_trip_directory_path(
        Path("."), manifest.country or "", manifest.state or "", *key
    ).as_posix()


def _unselected_prefixes(
    parent: TripKey,
    manifest: TripManifest,
    kept: list[tuple[TripKey, TripManifest]],
) -> Iterator[str]:
    """What a minute selection leaves out of a selected trip, as ``<dir>/`` prefixes.

    A unit ``segment_units`` drops goes whole; a kept unit loses the minute dirs
    of its unselected minutes.
    """
    kept_minutes = {key: expected_minute_dirs(unit) for key, unit in kept}
    for key, whole in segment_units(parent, manifest):
        unit_dir = _unit_dir(manifest, key)
        if key not in kept_minutes:
            yield f"{unit_dir}/"
            continue
        for name in expected_minute_dirs(whole) - kept_minutes[key]:
            yield f"{unit_dir}/{name}/"


def _held_reason(
    unit: TripManifest, output: DatasetOutput, camera_selection: CAMERA_SELECTION
) -> str | None:
    """Why the server still withholds some of the unit's delivered files, or None.

    The cases ``enumerate_trip_assets`` holds (and both writers turn into a held
    trip), over the files the delivery carries: the selected cameras' videos
    (anonymizing, restoring, archived, or no link at all), and SEI / fused SEI
    (restoring, archived) only when the output asks for them.
    """
    statuses: set[str] = set()
    for minute in unit.minutes:
        if minute.minute_epoch is None:
            continue
        if (
            output.footage.fused_sei is not None
            and minute.fused_sei_status in _ARCHIVE_STATUSES
        ):
            statuses.add(minute.fused_sei_status)
        for camera in minute.cameras:
            if not is_camera_allowed(camera.camera_type, camera_selection):
                continue
            if camera.video_status in _VIDEO_HOLDS:
                statuses.add(camera.video_status)
            elif camera.camera is None:
                statuses.add("unavailable")
            if (
                output.footage.sei is not None
                and camera.sei_status in _ARCHIVE_STATUSES
            ):
                statuses.add(camera.sei_status)
    return next((reason for reason in _HELD_REASONS if reason in statuses), None)


def _matching_prefix(path: str, prefixes: Container[str]) -> str | None:
    """The ``<dir>/`` prefix among ``prefixes`` that ``path`` lies under, if any."""
    cut = path.find("/")
    while cut != -1:
        if path[: cut + 1] in prefixes:
            return path[: cut + 1]
        cut = path.find("/", cut + 1)
    return None


def _list_dir(root: str) -> Iterator[tuple[str, int]]:
    def _fail(exc: OSError) -> None:
        raise exc  # os.walk would otherwise skip what it cannot read

    for dirpath, _dirnames, filenames in os.walk(root, onerror=_fail):
        rel = os.path.relpath(dirpath, root)
        base = "" if rel == "." else rel.replace(os.sep, "/") + "/"
        for name in filenames:
            try:
                size = os.stat(os.path.join(dirpath, name)).st_size
            except FileNotFoundError:
                continue  # renamed or removed since the walk (a live .part file)
            yield base + name, size


def _list_s3(uri: str) -> Iterator[tuple[str, int]]:
    bucket, prefix = parse_s3_uri(uri)
    # As in build_snapshot, the trailing slash is mandatory: a bare prefix would
    # also match sibling prefixes that merely start with the same characters.
    list_prefix = f"{prefix}/" if prefix else ""
    paginator = get_s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue  # a directory marker, not a file
            yield key[len(list_prefix) :], obj["Size"]


def canonical_target(target: str) -> str:
    """One spelling per tree: ``s3://<bucket>[/<prefix>]`` with no trailing slash,
    or a local directory's resolved absolute path.

    :func:`run` canonicalizes every target and lists each tree once; a
    ``Placement`` must return targets in this form to match the report's keys.
    """
    if target.startswith("s3:"):  # "s3:/x" is a typo'd URI, not a directory
        bucket, prefix = parse_s3_uri(target)
        return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
    return str(Path(target).resolve())


class _TargetError(Exception):
    def __init__(self, target: str, cause: Exception) -> None:
        super().__init__(f"cannot list {target}: {cause}")


def _named(target: str, listing: Iterable[tuple[str, int]]) -> Iterator[tuple[str, int]]:
    """Listings are lazy: say which target failed when one raises mid-diff."""
    try:
        yield from listing
    except (OSError, ClientError, BotoCoreError) as exc:
        raise _TargetError(target, exc) from exc


def run(
    export: ManifestExport,
    targets: list[str],
    *,
    placement: Placement | None,
    selection: TripSelectionMap | None,
    camera_selection: CAMERA_SELECTION,
    out_dir: Path,
    require_signal: bool,
    dry_mode: bool = False,
) -> int:
    """Expected set, listings, diff, report on stdout, flat lists under ``out_dir``.

    Returns the exit code: 0 or 1 (lists written), 2 when the export or a target
    cannot be read (message on stderr, nothing written).
    """
    canonical: list[str] = []
    for target in targets:
        try:
            canonical.append(canonical_target(target))
        except ValueError as exc:
            print(f"bad target {target!r}: {exc}", file=sys.stderr)
            return 2
    unique = list(dict.fromkeys(canonical))
    try:
        expected = expected_files(
            export,
            camera_selection=camera_selection,
            selection=selection,
            placement=placement,
            dry_mode=dry_mode,
        )
        listings = {t: _named(t, list_target(t)) for t in unique}
        report = diff(expected, listings, export, require_signal=require_signal)
    except (ExportError, _TargetError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(render(report))
    write_lists(report, out_dir)
    return report.exit_code


_SHOWN = 20


def render(report: InventoryReport) -> str:
    """The report text: header, targets, then the lists (first 20 lines each)."""
    trips = f"{report.trips} trip(s)"
    if report.incomplete_export:
        trips = f"covers {report.exported} of {report.trips} trip(s)"
    lines = [
        "Inventory report",
        f"Export: {report.dataset_id} at {report.exported_at}, {trips}",
        "Targets:",
    ]
    for target, summary in report.targets.items():
        signal = "yes" if summary.signal else "no"
        lines.append(f"  {target}: {summary.files} file(s), signal: {signal}")
    lines.append(f"Expected files: {report.expected}")
    if report.incomplete_export:
        lines.append(
            f"Note: {report.trips - (report.exported or 0)} trip(s) could not be "
            f"exported; see the export's `errors` (GET /v1/dataset/"
            f"{report.dataset_id}/manifests/exports) and re-export"
        )
    if report.absent_selected:
        first = ", ".join(
            f"{k.trip_id}_{k.sub_trip}_{k.health_grade}"
            for k in report.absent_selected[:3]
        )
        more = "" if len(report.absent_selected) <= 3 else ", ..."
        lines.append(
            f"Note: {len(report.absent_selected)} TRIPS_FILE trip(s) are not in "
            f"the export: {first}{more}"
        )
    sections = {
        "Missing": report.missing,
        "Extra": [f"{t}: {p}" for t, p in report.extra],
        "Misplaced": [
            f"{t}: {p} (expected in {e})" for t, p, e in report.misplaced
        ],
        "Duplicated": [f"{p} in {', '.join(ts)}" for p, ts in report.duplicated],
        "Held units": [f"{d} ({reason})" for d, reason in report.held],
        "Signal missing": list(report.signal_missing),
    }
    for title, entries in sections.items():
        lines.append("")
        lines.append(f"{title} ({len(entries)})")
        lines.extend(f"  {entry}" for entry in entries[:_SHOWN])
        if len(entries) > _SHOWN:
            lines.append(f"  \u2026 and {len(entries) - _SHOWN} more")
    lines.append("")
    lines.append(f"Overall: {'PASS' if report.exit_code == 0 else 'FAIL'}")
    lines.append(f"exit_code: {report.exit_code}")
    return "\n".join(lines)


def write_lists(report: InventoryReport, out_dir: Path) -> None:
    """Every entry, one per line: ``missing.txt``, ``extra.txt`` and
    ``misplaced.txt`` (``<target>: <path>``), ``held.txt`` (``<unit dir>\\t<reason>``)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    lists = {
        "missing.txt": report.missing,
        "extra.txt": [f"{t}: {p}" for t, p in report.extra],
        "misplaced.txt": [f"{t}: {p}" for t, p, _expected in report.misplaced],
        "held.txt": [f"{d}\t{reason}" for d, reason in report.held],
    }
    for name, entries in lists.items():
        (out_dir / name).write_text("".join(f"{e}\n" for e in entries), encoding="utf-8")


def _parse_args(
    argv: Sequence[str] | None, prog: str = "python -m app.inventory"
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Validate a delivered dataset against its manifest export.",
    )
    parser.add_argument(
        "--manifests",
        default="api",
        help="The manifest export: a path, an s3:// URI, or 'api' (default).",
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=None,
        help="Local dataset directory (defaults to S3_UPLOAD, else OUTPUT_DIR).",
    )
    parser.add_argument(
        "--s3",
        action="append",
        default=None,
        metavar="URI",
        help="An S3 prefix to inventory; repeat for several.",
    )
    parser.add_argument(
        "--trips-file", default=None, help="Overrides TRIPS_FILE from settings."
    )
    parser.add_argument(
        "--camera-selection",
        type=CAMERA_SELECTION,
        choices=list(CAMERA_SELECTION),
        default=None,
        metavar="{front,front_rear,all}",
        help="Overrides CAMERA_SELECTION from settings.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Report directory (default ./inventory/<dataset_id>/<exported_at>/).",
    )
    return parser.parse_args(argv)


def main(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "python -m app.inventory",
    default_targets: Callable[[], list[str]] | None = None,
    make_placement: Callable[[ManifestExport, CAMERA_SELECTION], Placement | None]
    | None = None,
) -> int:
    """The CLI. The uploader wrapper passes ``default_targets`` (used when neither
    ``--dataset-dir`` nor ``--s3`` is given) and ``make_placement`` (which may raise
    ``ExportError`` to exit 2)."""
    args = _parse_args(argv, prog)
    api_key = settings.api_key.strip()
    dataset_id = settings.dataset_id.strip()
    missing = [
        name
        for name, value in (("API_KEY", api_key), ("DATASET_ID", dataset_id))
        if not value and (name == "DATASET_ID" or args.manifests == "api")
    ]
    if missing:
        print(f"{' and '.join(missing)} must be set in the active .env.", file=sys.stderr)
        return 2

    if args.dataset_dir is not None or args.s3:
        targets = [str(args.dataset_dir)] if args.dataset_dir is not None else []
        targets += args.s3 or []
    elif default_targets is not None:
        targets = default_targets()
    else:
        targets = [settings.s3_upload.strip() or str(settings.output_dir)]

    trips_file = (args.trips_file or settings.trips_file).strip()
    selection: TripSelectionMap | None = None
    if trips_file:
        try:
            selection = load_trips_selection(Path(trips_file).expanduser())
        except (OSError, ValueError) as exc:
            print(f"TRIPS_FILE: {exc}", file=sys.stderr)
            return 2

    camera_selection = resolve_camera_selection(args.camera_selection)
    try:
        with open_export(args.manifests, api_key=api_key, dataset_id=dataset_id) as export:
            out_dir = args.out or (
                Path("inventory") / export.dataset_id / export.exported_at
            )
            return run(
                export,
                targets,
                placement=(
                    make_placement(export, camera_selection) if make_placement else None
                ),
                selection=selection,
                camera_selection=camera_selection,
                out_dir=out_dir,
                # A subset run never writes the completion signal.
                require_signal=selection is None,
                dry_mode=settings.dry_mode,
            )
    except ExportError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
