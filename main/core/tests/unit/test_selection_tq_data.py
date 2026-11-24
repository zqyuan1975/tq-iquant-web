"""条件选股取数封装（A0-1）：参数透传、P2 分片、空输入短路、异常不吞。

打 SDK 层：`core.tq.data` 以 `from .utils import get_tq` 绑定名字，
必须 patch `core.tq.data.get_tq`（patch utils 不生效）。
"""
import pandas as pd
import pytest

import core.tq.data as tq_data
from core.tq.data import TQData


class FakeTq:
    """记录每次调用的参数；可配置返回值与抛错行为。"""

    def __init__(self):
        self.calls = []
        self.fin = pd.DataFrame()
        self.gp = pd.DataFrame()
        self.go = pd.DataFrame()
        self.fail_on_call = None

    def _maybe_fail(self):
        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise RuntimeError("SDK boom")

    def get_financial_data(self, stock_list=None, table_list=None,
                           start_time="", report_type=""):
        self._maybe_fail()
        self.calls.append({
            "api": "fin", "stock_list": list(stock_list),
            "table_list": list(table_list),
            "start_time": start_time, "report_type": report_type,
        })
        return self.fin

    def get_gpjy_value_by_date(self, stock_list=None, year=None, mmdd=None,
                               table_list=None):
        self._maybe_fail()
        self.calls.append({
            "api": "gp", "stock_list": list(stock_list), "year": year,
            "mmdd": mmdd, "table_list": list(table_list),
        })
        return self.gp

    def get_gp_one_data(self, stock_list=None, table_list=None):
        self._maybe_fail()
        self.calls.append({
            "api": "go", "stock_list": list(stock_list),
            "table_list": list(table_list),
        })
        return self.go


@pytest.fixture
def fake(monkeypatch):
    tq = FakeTq()
    monkeypatch.setattr(tq_data, "get_tq", lambda: tq)
    monkeypatch.setattr(
        tq_data, "gate_config",
        lambda: {"enabled": True, "p2_chunk_size": 2})
    return tq


def test_financial_data_passes_params(fake):
    fake.fin = pd.DataFrame({"Code": ["000001.SZ"], "FN1": [1.0]})
    out = TQData().get_financial_data(
        ["000001.SZ"], ["FN1", "FN313"], start_time="20100101")
    call = fake.calls[0]
    assert call["api"] == "fin"
    assert call["stock_list"] == ["000001.SZ"]
    assert call["table_list"] == ["FN1", "FN313"]
    assert call["start_time"] == "20100101"
    assert call["report_type"] == "report_time"
    assert len(out) == 1


def test_financial_data_chunks_under_p2(fake):
    fake.fin = pd.DataFrame({"Code": ["x"], "FN1": [1.0]})
    codes = ["a", "b", "c", "d", "e"]
    TQData().get_financial_data(codes, ["FN1"], priority="P2")
    sizes = [len(c["stock_list"]) for c in fake.calls]
    assert sizes == [2, 2, 1]
    assert [c["stock_list"] for c in fake.calls] == [["a", "b"], ["c", "d"], ["e"]]


def test_no_chunk_when_below_threshold(fake):
    fake.fin = pd.DataFrame({"Code": ["a"], "FN1": [1.0]})
    TQData().get_financial_data(["a", "b"], ["FN1"], priority="P2")
    assert len(fake.calls) == 1


def test_empty_input_skips_sdk(fake):
    assert TQData().get_financial_data([], ["FN1"]).empty
    assert TQData().get_financial_data(["a"], []).empty
    assert TQData().get_gpjy_value_by_date([], 2024, "0430", ["GP16"]).empty
    assert TQData().get_gp_one_data([], ["GO3"]).empty
    assert fake.calls == []


def test_gpjy_passes_year_mmdd(fake):
    fake.gp = pd.DataFrame({"Code": ["a"], "GP16": [100.0]})
    out = TQData().get_gpjy_value_by_date(
        ["a", "b", "c"], 2024, "0430", ["GP16"], priority="P2")
    assert len(fake.calls) == 2
    first = fake.calls[0]
    assert first["year"] == 2024 and first["mmdd"] == "0430"
    assert first["table_list"] == ["GP16"]
    assert len(out) == 2


def test_gp_one_data_passes_table_list(fake):
    fake.go = pd.DataFrame({"Code": ["a"], "GO3": [12.0]})
    TQData().get_gp_one_data(["a"], ["GO3", "GO8"])
    assert fake.calls[0]["table_list"] == ["GO3", "GO8"]


def test_chunk_failure_propagates(fake):
    """任一片失败必须整体抛出，不能返回“部分成功”的帧。"""
    fake.fin = pd.DataFrame({"Code": ["a"], "FN1": [1.0]})
    fake.fail_on_call = 1
    with pytest.raises(RuntimeError, match="SDK boom"):
        TQData().get_financial_data(
            ["a", "b", "c", "d"], ["FN1"], priority="P2")
