"""SPA 静态托管缓存头：入口 HTML 不缓存，带哈希的 assets 长缓存。"""
import asyncio
from pathlib import Path

from core.main import SPAStaticFiles

_SCOPE = {"type": "http", "method": "GET", "path": "/", "headers": [],
          "query_string": b""}


def _get(app, path):
    return asyncio.run(app.get_response(path, _SCOPE))


def test_index_html_is_no_cache(tmp_path: Path):
    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")
    app = SPAStaticFiles(directory=str(tmp_path), html=True)

    resp = _get(app, "index.html")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-cache, must-revalidate, max-age=0"


def test_hashed_assets_are_immutable(tmp_path: Path):
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "app-abc123.js").write_text("console.log(1)", encoding="utf-8")
    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")
    app = SPAStaticFiles(directory=str(tmp_path), html=True)

    resp = _get(app, "assets/app-abc123.js")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=31536000, immutable"


def test_spa_fallback_serves_index_with_no_cache(tmp_path: Path):
    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")
    app = SPAStaticFiles(directory=str(tmp_path), html=True)

    resp = _get(app, "dashboard")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-cache, must-revalidate, max-age=0"
