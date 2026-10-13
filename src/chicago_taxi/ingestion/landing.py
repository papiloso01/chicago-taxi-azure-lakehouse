"""Immutable run folders; only success manifests are consumable by future bronze ETL."""
import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from chicago_taxi.ingestion.taxi_api import pages


def month_windows(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start >= end:
        raise ValueError("start must precede exclusive end")
    while start < end:
        next_month = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
        stop = min(next_month, end)
        yield start.isoformat(), stop.isoformat()
        start = stop


def ingest(root, start, end, page_size=5000, token="", fetch=None, max_pages=50000):
    windows = list(month_windows(start, end))
    if date.fromisoformat(start).year < 2024:
        raise ValueError("Taxi source covers 2024 onward")
    root = Path(root)
    if not root.is_absolute():
        raise ValueError("Landing root must be an absolute local/volume path")
    run_id = uuid.uuid4().hex
    folder = root / "taxi_trips" / ("run_id=" + run_id)
    folder.mkdir(parents=True, exist_ok=False)
    report = dict(run_id=run_id, source="taxi_trips", status="running", start=start,
                  end_exclusive=end, started_at_utc=datetime.now(timezone.utc).isoformat(),
                  extracted_rows=0, files=[], windows=[])
    audit = folder / "audit.json"
    audit.write_text(json.dumps(report, indent=2), encoding="utf-8")
    try:
        for window_start, window_end in windows:
            count = 0
            for index, rows in enumerate(pages(window_start, window_end, page_size, token, fetch, max_pages)):
                # Preserve source field values and an array per page; retain file hash for verification.
                body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
                path = folder / f"start={window_start}" / f"part-{index:06d}.json"
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(body)
                report["files"].append(dict(path=str(path), rows=len(rows), bytes=len(body),
                    sha256=hashlib.sha256(body).hexdigest(), window_start=window_start, window_end_exclusive=window_end))
                count += len(rows)
                report["extracted_rows"] += len(rows)
                if (index + 1) % 100 == 0:
                    audit.write_text(json.dumps(report, indent=2), encoding="utf-8")
            report["windows"].append(dict(start=window_start, end_exclusive=window_end, rows=count))
            audit.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report.update(status="success", completed_at_utc=datetime.now(timezone.utc).isoformat())
        audit.write_text(json.dumps(report, indent=2), encoding="utf-8")
        # No shared latest pointer, no overwrite of prior runs. Consumers must validate a complete
        # success manifest, not discover page JSONs indiscriminately or infer success from an audit.
        (folder / "manifest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    except Exception as error:
        (folder / "manifest.json").unlink(missing_ok=True)
        report.update(status="failed", completed_at_utc=datetime.now(timezone.utc).isoformat(),
                      error_type=type(error).__name__)
        # Do not persist exception text: request/auth errors can include credentials or headers.
        audit.write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise
