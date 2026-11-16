"""版本号单一来源护栏：core.version.APP_VERSION 必须等于 packaging/app-version.iss。

两处分别被后端（/api/status、build-info）与安装器（AppVersion）读取，只改一处会造成
「新版本号 + 旧代码」或安装包版本与运行版本不一致，故用测试钉住。
"""
import re
from pathlib import Path

from core.version import APP_VERSION

REPO_ROOT = Path(__file__).resolve().parents[4]


def test_core_version_matches_app_version_iss():
    text = (REPO_ROOT / "packaging" / "app-version.iss").read_text(encoding="utf-8-sig")
    match = re.search(r'#define\s+AppVersion\s+"([^"]+)"', text)
    assert match, "packaging/app-version.iss 缺少 #define AppVersion"
    assert match.group(1) == APP_VERSION, (
        "版本号两处不一致：packaging/app-version.iss=%s，"
        "main/core/version.py APP_VERSION=%s；请同步后再出包" % (match.group(1), APP_VERSION)
    )
