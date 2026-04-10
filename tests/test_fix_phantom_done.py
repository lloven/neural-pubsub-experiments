"""Tests for scripts.fix_phantom_done.

A "phantom done" entry is a record in a phase's `.progress.json` whose
status is `done` but whose corresponding result CSV does not exist on
disk. This happens when a previous run was marked done by the runner
(e.g. crashed mid-write, or the .csv was renamed to .csv.old after a
bug fix invalidated it) but the entry was never reset to `queued`.
With `--resume` enabled the runner skips these phantom entries forever,
so the affected runs are never re-executed.

The phantom-fix utility detects these entries and re-marks them
queued so a subsequent `--resume` will re-execute them. The utility
is dry-run by default for safety; `--apply` actually mutates the
progress file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def _make_progress_file(tmp_path: Path, entries: dict[str, dict]) -> Path:
    """Create a .progress.json file at the given location."""
    f = tmp_path / ".progress.json"
    f.write_text(json.dumps(entries, indent=2))
    return f


def _touch_csv(tmp_path: Path, run_id: str) -> Path:
    """Create an empty .csv next to the progress file."""
    f = tmp_path / f"{run_id}.csv"
    f.write_text("pipeline_id,success\n")
    return f


class TestDetectPhantomDone:
    """Detection of done-without-csv entries."""

    def test_detects_done_with_no_csv(self, tmp_path):
        from scripts.fix_phantom_done import find_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "/some/path", "timestamp": ""},
            "run_b": {"status": "done", "detail": str(tmp_path / "run_b.csv"), "timestamp": ""},
        })
        _touch_csv(tmp_path, "run_b")

        phantoms = find_phantom_done(progress)
        assert phantoms == ["run_a"]

    def test_skips_non_done_entries(self, tmp_path):
        from scripts.fix_phantom_done import find_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "queued"},
            "run_b": {"status": "running"},
            "run_c": {"status": "failed"},
            "run_d": {"status": "done", "detail": "/x"},
        })
        phantoms = find_phantom_done(progress)
        # Only run_d is done with no csv
        assert phantoms == ["run_d"]

    def test_done_with_existing_csv_is_not_phantom(self, tmp_path):
        from scripts.fix_phantom_done import find_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "irrelevant"},
        })
        _touch_csv(tmp_path, "run_a")

        phantoms = find_phantom_done(progress)
        assert phantoms == []


class TestApply:
    """Re-marking phantom entries as queued."""

    def test_dry_run_does_not_modify_file(self, tmp_path):
        from scripts.fix_phantom_done import fix_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "/x"},
        })
        before = progress.read_text()

        fixed = fix_phantom_done(progress, apply=False)
        assert fixed == ["run_a"]
        assert progress.read_text() == before  # unchanged

    def test_apply_marks_phantoms_as_queued(self, tmp_path):
        from scripts.fix_phantom_done import fix_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "/x"},
            "run_b": {"status": "done", "detail": str(tmp_path / "run_b.csv")},
        })
        _touch_csv(tmp_path, "run_b")

        fixed = fix_phantom_done(progress, apply=True)
        assert fixed == ["run_a"]

        after = json.loads(progress.read_text())
        assert after["run_a"]["status"] == "queued"
        assert after["run_b"]["status"] == "done"  # unchanged

    def test_apply_preserves_other_fields(self, tmp_path):
        from scripts.fix_phantom_done import fix_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {
                "status": "done",
                "detail": "/x",
                "timestamp": "2026-04-01",
                "extra": {"k": "v"},
            },
        })
        fix_phantom_done(progress, apply=True)
        after = json.loads(progress.read_text())
        assert after["run_a"]["status"] == "queued"
        assert after["run_a"]["timestamp"] == "2026-04-01"
        assert after["run_a"]["extra"] == {"k": "v"}

    def test_creates_backup_on_apply(self, tmp_path):
        from scripts.fix_phantom_done import fix_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "/x"},
        })
        original = progress.read_text()

        fix_phantom_done(progress, apply=True)
        backup = progress.with_suffix(".json.bak")
        assert backup.exists()
        assert backup.read_text() == original

    def test_no_phantoms_means_no_changes(self, tmp_path):
        from scripts.fix_phantom_done import fix_phantom_done

        progress = _make_progress_file(tmp_path, {
            "run_a": {"status": "done", "detail": "ok"},
            "run_b": {"status": "queued"},
        })
        _touch_csv(tmp_path, "run_a")
        before = progress.read_text()

        fixed = fix_phantom_done(progress, apply=True)
        assert fixed == []
        assert progress.read_text() == before
        assert not progress.with_suffix(".json.bak").exists()
