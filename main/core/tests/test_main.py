from httpx import ASGITransport, AsyncClient
from pathlib import Path
import pytest

from core.main import app

# SPA 回退依赖构建产物 web/dist；未构建时跳过（避免测试环境无前端导致失败）
_DIST_EXISTS = (Path(__file__).resolve().parents[3] / "web" / "dist").exists()


@pytest.mark.anyio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        # 自愈父进程靠这些字段确认「真的是我们的后端」（端口动态，见 server.json）
        assert body["ok"] is True
        assert body["app"] == "tq-iquant"
        assert body["version"]
        assert body["pid"] > 0
        assert body["uptime_seconds"] >= 0


@pytest.mark.anyio
@pytest.mark.skipif(not _DIST_EXISTS, reason="web/dist 未构建，跳过 SPA 回退测试")
async def test_spa_fallback_serves_index_html_for_frontend_routes():
    """前端路由（如 /dashboard）刷新 → 回退 index.html，而非 {"detail":"Not Found"}。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/dashboard")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        assert '<div id="app"></div>' in resp.text


@pytest.mark.anyio
@pytest.mark.skipif(not _DIST_EXISTS, reason="web/dist 未构建，跳过 SPA 回退测试")
async def test_health_serves_spa_index_for_browser_navigation():
    """浏览器地址栏访问 /health（Accept 含 text/html）→ 前端 index.html，不再被 JSON 挡住。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/health",
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        )
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        assert '<div id="app"></div>' in resp.text


@pytest.mark.anyio
async def test_health_still_json_for_probes():
    """探针 / curl（Accept: */*、application/json 或缺省）仍拿原 JSON，字段不变。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        for accept in ("*/*", "application/json", None):
            headers = {"Accept": accept} if accept else {}
            resp = await client.get("/health", headers=headers)
            assert resp.status_code == 200, accept
            assert "application/json" in resp.headers.get("content-type", ""), accept
            body = resp.json()
            assert body["ok"] is True
            assert body["app"] == "tq-iquant"
            assert body["version"]
            assert body["pid"] > 0


@pytest.mark.anyio
async def test_api_health_returns_same_json_even_for_browser_accept():
    """/api/health 是纯 JSON 冗余入口：字段与 /health 一致，且不参与内容协商。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        plain = await client.get("/health")
        api = await client.get("/api/health", headers={"Accept": "text/html"})
        assert api.status_code == 200
        assert "application/json" in api.headers.get("content-type", "")
        assert api.json().keys() == plain.json().keys()
        assert api.json()["app"] == "tq-iquant"
        assert api.json()["pid"] > 0


@pytest.mark.anyio
async def test_unknown_api_path_still_returns_json_404():
    """未知 /api 路径必须保持 JSON 404，不能被 SPA 回退劫持。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/definitely-not-a-route-xyz")
        assert resp.status_code == 404
        assert resp.json() == {"detail": "Not Found"}


@pytest.mark.anyio
async def test_auto_shutdown_endpoints_return_ok():
    """心跳/关闭/退出接口在开发模式（未 arm）下安全 no-op 并返回 ok。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        for method, url in [("GET", "/api/system/heartbeat"),
                            ("POST", "/api/system/heartbeat"),  # sendBeacon 固定 POST
                            ("POST", "/api/system/closing"),
                            ("POST", "/api/system/shutdown")]:
            resp = await client.request(method, url)
            assert resp.status_code == 200, (method, url)
            assert resp.json()["code"] == 0, (method, url)


def test_openapi_operation_ids_are_unique():
    """同一函数挂多个方法会生成重复 operationId（2026-11-19 修复 heartbeat）。

    FastAPI 用「函数名 + 路径 + 方法」生成 operationId；``api_route`` 双方法会让
    GET/POST 共用同一个 id，启动时打 Duplicate Operation ID 告警并污染自动生成
    SDK。拆成两个函数后全量 operationId 必须唯一。
    """
    schema = app.openapi()
    ids = [
        op.get("operationId")
        for item in schema["paths"].values()
        for method, op in item.items()
        if method in ("get", "post", "put", "patch", "delete")
    ]
    assert all(ids)
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    assert duplicated == []

    heartbeat = schema["paths"]["/api/system/heartbeat"]
    assert heartbeat["get"]["operationId"] != heartbeat["post"]["operationId"]
