"""Validate committed landing runs and stage bounded pages for distributed loading."""
import hashlib
import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path


def validate_manifest(run_path):
    folder = Path(run_path).resolve()
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "success" or manifest.get("source") != "taxi_trips":
        raise ValueError("Only successful taxi manifests can be loaded")
    run_id = manifest.get("run_id", "")
    if not re.fullmatch(r"[0-9a-f]{32}", run_id) or folder.name != "run_id=" + run_id:
        raise ValueError("Manifest run identity does not match folder")
    start, end = date.fromisoformat(manifest["start"]), date.fromisoformat(manifest["end_exclusive"])
    if start >= end:
        raise ValueError("Invalid manifest date bounds")
    started = datetime.fromisoformat(manifest["started_at_utc"])
    completed = datetime.fromisoformat(manifest["completed_at_utc"])
    if started.utcoffset() is None or completed.utcoffset() is None or completed < started:
        raise ValueError("Invalid extraction timestamps")
    if type(manifest["extracted_rows"]) is not int or manifest["extracted_rows"] < 0:
        raise ValueError("Invalid manifest row count")
    total, seen = 0, set()
    for item in manifest["files"]:
        path = Path(item["path"]).resolve()
        if not path.is_relative_to(folder) or path in seen or path.suffix != ".json":
            raise ValueError("File outside run folder or repeated file")
        seen.add(path)
        body = path.read_bytes()
        if len(body) != item["bytes"] or hashlib.sha256(body).hexdigest() != item["sha256"]:
            raise ValueError("Landing file size/hash mismatch")
        rows = json.loads(body)
        if not isinstance(rows, list) or len(rows) != item["rows"]:
            raise ValueError("Landing row count mismatch")
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("trip_id"), str) or not row["trip_id"].strip():
                raise ValueError("Missing or invalid trip_id")
        total += len(rows)
    if total != manifest["extracted_rows"]:
        raise ValueError("Manifest total row count mismatch")
    windows = manifest["windows"]
    cursor, window_total = start, 0
    for window in windows:
        stop = date.fromisoformat(window["end_exclusive"])
        if date.fromisoformat(window["start"]) != cursor or not cursor < stop <= end:
            raise ValueError("Manifest windows are not contiguous")
        count = sum(item["rows"] for item in manifest["files"]
                    if item["window_start"] == window["start"]
                    and item["window_end_exclusive"] == window["end_exclusive"])
        if count != window["rows"]:
            raise ValueError("Window row count mismatch")
        window_total += count
        cursor = stop
    if cursor != end or window_total != total:
        raise ValueError("Manifest windows do not cover extraction")
    return manifest


def stage_run(run_path):
    """Validate entire run before staging; memory is bounded by a single landing page."""
    manifest = validate_manifest(run_path)
    stage = Path(run_path) / "_bronze_stage" / uuid.uuid4().hex
    stage.mkdir(parents=True)
    try:
        for index, item in enumerate(manifest["files"]):
            body = Path(item["path"]).read_bytes()
            # Check again against mutation between validation and staging.
            if hashlib.sha256(body).hexdigest() != item["sha256"]:
                raise ValueError("Landing file changed during staging")
            with (stage / f"part-{index:06d}.jsonl").open("w", encoding="utf-8") as out:
                for row in json.loads(body):
                    raw = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                    out.write(json.dumps(dict(trip_id=row["trip_id"], raw_payload=raw,
                        payload_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                        landing_run_id=manifest["run_id"], source_file=item["path"],
                        extracted_at_utc=manifest["started_at_utc"]), ensure_ascii=False) + "\n")
        return manifest, stage
    except Exception:
        import shutil
        shutil.rmtree(stage)
        raise
