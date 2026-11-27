"""Test fixtures of the public repository: the test lake the pipeline built
from 52 UniProtKB entries (tests/fixtures/lake/, written by the export)."""

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)   # uniprot_parquet.py, however pytest is started

LAKE = os.path.join(os.path.dirname(__file__), "fixtures", "lake")


@pytest.fixture(scope="session")
def parquet_lake() -> dict[str, str]:
    """The test lake (tests copy it before changing anything)."""
    return {"lake_dir": LAKE}
