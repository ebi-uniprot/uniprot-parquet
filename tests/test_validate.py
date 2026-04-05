"""Tests for the production validation script (validate_lake.py).

Runs the full transform pipeline, then runs the validator against the
resulting lake + source JSONL.  Verifies that all checks pass.
"""

import os
import sys
import subprocess

import pytest

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")


def test_validate_passes_on_good_lake(small_jsonl, parquet_lake, tmp_path):
    """The validator should pass on a correctly built lake."""
    validate_script = os.path.join(BIN_DIR, "validate_lake.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    report_path = str(tmp_path / "validation_report.txt")

    cmd = [
        sys.executable, validate_script,
        "--lake", parquet_lake["lake_dir"],
        "--jsonl", small_jsonl,
        "--spot-check-n", "10",
        "-o", report_path,
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    # Print stderr for debugging if it fails
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        if os.path.exists(report_path):
            with open(report_path) as f:
                print("REPORT:", f.read())

    assert result.returncode == 0, (
        f"Validator failed on a correct lake:\n{result.stderr}"
    )

    # Verify report file was created and contains PASS
    assert os.path.exists(report_path)
    with open(report_path) as f:
        report = f.read()
    assert "ALL CHECKS PASSED" in report
    assert "[FAIL]" not in report
