# -*- coding: utf-8 -*-
"""宽库只读连接器（A0-2）：只读语义、结构探针、路径缺失不抛异常。"""
import sqlite3

import pytest

from core.selection import wide_db


@pytest.fixture
def sample_db(tmp_path):
    p = tmp_path / "wide.sqlite3"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE fin_wide (code TEXT, asof TEXT, FN1 REAL)")
    conn.execute("INSERT INTO fin_wide VALUES ('000001.SZ', '2024-04-30', 1.0)")
    conn.execute("CREATE TABLE kline_daily (code TEXT, close REAL)")
    conn.commit()
    conn.close()
    return str(p)


def test_missing_db_returns_not_ready(tmp_path):
    info = wide_db.probe(str(tmp_path / "nope.sqlite3"))
    assert info["ready"] is False
    assert info["error"] == "wide_db_not_found"
    assert info["tables"] == []


def test_probe_lists_tables_and_columns(sample_db):
    info = wide_db.probe(sample_db)
    assert info["ready"] is True
    assert info["readonly"] is True
    tables = {t["name"]: t for t in info["tables"]}
    assert tables["fin_wide"]["columns"] == 3
    assert tables["fin_wide"]["has_rows"] is True
    assert tables["kline_daily"]["has_rows"] is False


def test_connection_is_readonly(sample_db):
    with wide_db.open_wide_db(sample_db) as conn:
        rows = conn.execute("SELECT code FROM fin_wide").fetchall()
        assert rows == [("000001.SZ",)]
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO fin_wide VALUES ('x', 'y', 1)")


def test_read_sql_rejects_writes(sample_db):
    assert wide_db.read_sql("SELECT COUNT(*) FROM fin_wide", path=sample_db)[0][0] == 1
    with pytest.raises(ValueError, match="只读"):
        wide_db.read_sql("DELETE FROM fin_wide", path=sample_db)


def test_table_columns(sample_db):
    assert wide_db.table_columns("fin_wide", path=sample_db) == ["code", "asof", "FN1"]
