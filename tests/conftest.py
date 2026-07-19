"""Shared test setup.

`sys.path` gets the repo root added so `import archiver` works when pytest
is run from anywhere. `redirect_storage` points the archiver's on-disk
paths at a throwaway tmp_path directory so tests never touch the real
data/ folder.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import archiver  # noqa: E402

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text())


@pytest.fixture
def fixture():
    return load_fixture


@pytest.fixture
def redirect_storage(tmp_path, monkeypatch):
    """Point every on-disk path the archiver writes to at a tmp_path
    sandbox, so storage tests never touch the real data/ directory."""
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    agg_dir = data_dir / "agg"
    meta_dir = data_dir / "meta"
    monkeypatch.setattr(archiver, "DATA_DIR", data_dir)
    monkeypatch.setattr(archiver, "RAW_DIR", raw_dir)
    monkeypatch.setattr(archiver, "AGG_DIR", agg_dir)
    monkeypatch.setattr(archiver, "META_DIR", meta_dir)
    monkeypatch.setattr(archiver, "STATE_FILE", meta_dir / "state.json")
    monkeypatch.setattr(archiver, "MARKETS_META_FILE", meta_dir / "markets.parquet")
    monkeypatch.setattr(archiver, "SERIES_META_FILE", meta_dir / "series.parquet")
    monkeypatch.setattr(archiver, "HOT_TRADES_FILE", meta_dir / "hot_trades.parquet")
    return data_dir
