"""B4 通道能力矩阵：engine_status 对外暴露 capabilities / limits。"""
from core.services import live_service


def test_engine_status_exposes_capabilities_per_engine():
    data = live_service.engine_status("tdx")
    assert data["active"] == "tdx_tq"
    assert data["capabilities"]["split_order"] is True
    assert data["capabilities"]["remark_claim"] is False
    engines = {e["key"]: e for e in data["engines"]}
    assert set(engines) == {"goldminer", "tdx", "ths", "iquant"}
    for e in engines.values():
        assert set(e["capabilities"]) == {
            "place_order", "cancel_order", "quotes", "limit_price",
            "remark_claim", "split_order", "premarket_limit"}
        assert isinstance(e["capability_limits"], list)


def test_split_only_tdx_and_remark_only_http_bridge():
    caps = live_service.CHANNEL_CAPABILITIES
    assert [k for k, v in caps.items() if v["split_order"]] == ["tdx"]
    assert {k for k, v in caps.items() if v["remark_claim"]} == {
        "iquant", "goldminer"}
    assert all(v["place_order"] and v["cancel_order"] for v in caps.values())


def test_engine_status_unknown_falls_back_to_tdx():
    data = live_service.engine_status("qmt")
    assert data["active"] == "tdx_tq"
    assert data["capabilities"]["split_order"] is True
