#!/usr/bin/env python
"""Nuitka standalone(onedir) 构建 + 产物整理。

三种模式（避免每次小改动都走完整 Nuitka 构建）：
- 默认：保留并复用 Nuitka 的导入分析缓存（省依赖扫描几分钟）；注意 Nuitka 4.2.2
  **没有真正的增量 C 编译**，改后端仍需完整构建，后端出包集中在稳定节点/夜间。
- --clean：清空模块缓存与上次编译中间目录后全量重编；正式出包/缓存可疑时用。
- --web-only：不跑 Nuitka，只把 web/dist 刷新进已有 dist/tq-iquant；只改前端用这条（秒级）。

产物：<仓库根>/dist/tq-iquant/（含 exe、_internal/、web/dist、alembic、config 模板）
安装器：packaging/installer.iss（全量包）/ packaging/installer_web.iss（前端补丁包，约 3 MB）

注意：
- 旧版打包产物 main/dist/tq-iquant 已废弃并删除，不再维护。
- 桥脚本（live/bridge/*.py）**不编译进 exe**，由安装器以明文放到 <安装目录>\\bridge\\。
- 版本号单一来源：packaging/app-version.iss（本脚本与两个 Inno 脚本共用）。
"""
import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent            # main/
REPO = ROOT.parent                                # 仓库根
WEB_DIST = REPO / "web" / "dist"
DIST_OUT = REPO / "dist" / "tq-iquant"            # 最终分发目录
BUILD_OUT = REPO / "dist" / "build"               # Nuitka 输出中转
NIGHT_BUILD_DIR = REPO / "dist" / "build" / "nuitka-build"
ENTRY = ROOT / "desktop.py"
ICON = REPO / "packaging" / "icon" / "app.ico"
CONFIG_TEMPLATE = REPO / "packaging" / "config.template.yaml"
VERSION_FILE = REPO / "packaging" / "app-version.iss"


def read_version() -> str:
    """从 packaging/app-version.iss 读取版本号（唯一来源，避免两处写死不一致）。"""
    if not VERSION_FILE.exists():
        sys.exit(f"缺少版本号文件：{VERSION_FILE}")
    text = VERSION_FILE.read_text(encoding="utf-8-sig")
    match = re.search(r'#define\s+AppVersion\s+"([^"]+)"', text)
    if not match:
        sys.exit(f"无法从 {VERSION_FILE} 解析 AppVersion（期望格式：#define AppVersion \"1.0.1\"）")
    return match.group(1)


VERSION = read_version()


def run(cmd, cwd=None, env=None):
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        print(f"FAILED: {cmd}", flush=True)
        sys.exit(1)


def prepare_cache_dir():
    """缓存重定向到项目内：本机 %LOCALAPPDATA%\\Nuitka 写入不稳定（PermissionError）。

    只做重定向，不清理——清理会让模块缓存归零，下一次必然全量重编。
    """
    NIGHT_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["NUITKA_CACHE_DIR"] = str(NIGHT_BUILD_DIR)


def purge_nuitka_cache():
    """--clean 模式：清空模块缓存 + 上次编译中间目录，恢复全量重编。"""
    prepare_cache_dir()
    module_cache = NIGHT_BUILD_DIR / "module-cache"
    if module_cache.exists():
        shutil.rmtree(module_cache, ignore_errors=True)
        print(f"已清空 Nuitka 模块缓存：{module_cache}")
    build_dir = BUILD_OUT / "desktop.build"
    if build_dir.exists():
        shutil.rmtree(build_dir, ignore_errors=True)
        print(f"已清空 Nuitka 中间目录：{build_dir}")


def cache_stats() -> str:
    module_cache = NIGHT_BUILD_DIR / "module-cache"
    if not module_cache.exists():
        return "模块缓存为空（本次为全量编译）"
    files = [f for f in module_cache.rglob("*") if f.is_file()]
    size_mb = sum(f.stat().st_size for f in files) / 1024 / 1024
    return f"Nuitka 模块缓存：{len(files)} 个文件 / {size_mb:.1f} MB（命中即跳过重编）"


def nuitka_cmd(python_exe: str):
    cmd = [
        python_exe, "-m", "nuitka",
        "--standalone",
        "--low-memory",
        "--enable-plugin=anti-bloat",
        "--include-package=core",
        "--include-package-data=core",   # tdx_formula 等包内数据
        "--include-package=tq_iquant_shared",
        "--include-package=sqlalchemy",
        "--include-package=fastapi",
        "--include-package=uvicorn",
        "--include-package=pandas",
        "--include-package=polars",
        "--include-package=pydantic",
        "--include-package=httpx",
        "--include-package=pytdx",
        "--include-package=openpyxl",
        "--include-package=yaml",
        "--include-package=alembic",
        # 同花顺 UI 下单通道：pywinauto 窗口自动化 + 剪贴板 + 截图
        "--include-package=pywinauto",
        "--include-package=pyperclip",
        "--include-package=PIL",
        # 验证码 OCR：ddddocr（onnxruntime 推理 + 内置 onnx 模型）
        "--include-package=ddddocr",
        "--include-package-data=ddddocr",
        "--include-package=onnxruntime",
        "--include-package=cv2",
        # Windows 内置 OCR 兜底（WinRT）
        "--include-package=winrt",
        "--include-package-data=winrt",
        # 桌面壳：pywebview + WebView2（pythonnet/clr）；加载失败时启动器自动回退浏览器
        # 注意：webview 交给 Nuitka 内置 pywebview 插件处理平台分支，
        # 显式 --include-package=webview 会与插件的平台模块决策冲突（FATAL）。
        "--include-package=pythonnet",
        "--include-package-data=pythonnet",
        "--include-package=clr_loader",
        "--include-package-data=clr_loader",
        "--include-module=clr",
        # 排除第三方包自带测试代码（--include-package 会连测试一起编译，极慢且无用）
        "--nofollow-import-to=*.tests",
        "--nofollow-import-to=*.testing",
        "--nofollow-import-to=alembic.testing",
        "--update-check=never",
        "--assume-yes-for-downloads",
        "--windows-console-mode=disable",
        f"--windows-icon-from-ico={ICON}",
        "--company-name=创懿私募基金",
        "--product-name=创懿基金量化投资平台",
        f"--file-version={VERSION}",
        f"--product-version={VERSION}",
        "--output-filename=tq-iquant",
        f"--output-dir={BUILD_OUT}",
        str(ENTRY),
    ]
    return cmd


def copy_web(dist_dir: Path):
    """把最新 web/dist 刷进分发目录（先清旧目录，避免旧 hash chunk 残留）。"""
    if not WEB_DIST.exists():
        sys.exit(f"缺少前端构建产物：{WEB_DIST}（先在 web/ 下执行 npm run build）")

    dest_web = dist_dir / "_internal" / "web" / "dist"
    if dest_web.exists():
        shutil.rmtree(dest_web)
    dest_web.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(WEB_DIST, dest_web)

    # 证据：前端产物时间 + 文件数，确认打进包的是最新构建
    web_mtime = datetime.datetime.fromtimestamp((WEB_DIST / "index.html").stat().st_mtime)
    web_count = sum(1 for _ in WEB_DIST.rglob("*") if _.is_file())
    print(f"  web/dist -> {dest_web}")
    print(f"  web/dist 构建时间：{web_mtime:%Y-%m-%d %H:%M:%S}（{web_count} 个文件）")


def copy_assets(dist_dir: Path):
    print("\n=== 复制运行期资源 ===")
    copy_web(dist_dir)

    # core/main.py 打包版按 <exe>/_internal/web/dist 定位静态资源，此处不再额外复制。

    echarts = REPO / "web" / "node_modules" / "echarts" / "dist" / "echarts.min.js"
    if echarts.exists():
        shutil.copy2(echarts, dist_dir / "echarts.min.js")
        print("  echarts.min.js（离线报告内联运行时）")
    else:
        print("  WARNING: 未找到 echarts.min.js，离线报告导出将失败")

    if CONFIG_TEMPLATE.exists():
        shutil.copy2(CONFIG_TEMPLATE, dist_dir / "config.template.yaml")
        print("  config.template.yaml（首次启动生成用户 config.yaml）")

    alembic_src = ROOT / "alembic"
    if not alembic_src.exists():
        sys.exit("缺少 main/alembic，无法在目标机自动建库/升级")
    shutil.copytree(alembic_src, dist_dir / "alembic", dirs_exist_ok=True)
    print("  alembic/（首次启动自动迁移）")

    (dist_dir / "data").mkdir(parents=True, exist_ok=True)
    (dist_dir / "logs").mkdir(parents=True, exist_ok=True)
    # 数据目录只占位，安装器不覆盖用户已有 data/

    # 桥脚本明文随包：QMT(8790 股票账户) + 掘金(7801) 两个生产桥 + 部署说明。
    # 信用账户腿 iquant_bridge_live.py（8791）不进安装包。
    bridge_dir = dist_dir / "bridge"
    bridge_dir.mkdir(parents=True, exist_ok=True)
    for name in ("iquant_bridge.py", "goldminer_bridge.py"):
        src = REPO / "live" / "bridge" / name
        if not src.exists():
            sys.exit(f"缺少桥脚本：{src}")
        shutil.copy2(src, bridge_dir / name)
    deploy = REPO / "packaging" / "bridge-deploy.txt"
    if deploy.exists():
        shutil.copy2(deploy, bridge_dir / "bridge-deploy.txt")
    print("  bridge/（两个生产桥明文 + 部署说明）")

    # VC++ 运行库：Nuitka 只搬它找到的几个，msvcp140_1.dll 常在构建机被漏掉，
    # 目标机若无 VC++ 2015-2022 Redistributable 会直接启动失败 → 从 System32 补齐。
    system32 = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32"
    for dll in ("msvcp140.dll", "msvcp140_1.dll", "vcruntime140.dll", "vcruntime140_1.dll"):
        if (dist_dir / dll).exists() or not (system32 / dll).exists():
            continue
        shutil.copy2(system32 / dll, dist_dir / dll)
        print(f"  {dll}（VC++ 运行库补齐）")


def _git_info():
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(REPO), capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        dirty = bool(subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(REPO), capture_output=True, text=True, timeout=30,
        ).stdout.strip())
        return commit, dirty
    except Exception:
        return "", False


def write_build_info(dist_dir: Path, mode: str):
    """分发目录内留一份构建档案：装到目标机后可直接看版本/来源，便于核对与回滚。"""
    index = WEB_DIST / "index.html"
    commit, dirty = _git_info()
    info = {
        "app_version": VERSION,
        "build_mode": mode,
        "build_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "git_commit": commit,
        "git_dirty": dirty,
        "frontend_dist_mtime": (
            datetime.datetime.fromtimestamp(index.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            if index.exists() else ""
        ),
    }
    (dist_dir / "build-info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"  build-info.json（{info['build_mode']} / {info['app_version']} / {commit or 'no-git'}）")


def web_only():
    """只刷新前端产物：不跑 Nuitka，几秒完成；随后用 installer_web.iss 出几 MB 补丁包。"""
    if not (DIST_OUT / "tq-iquant.exe").exists():
        sys.exit(f"未找到已有分发目录：{DIST_OUT}（先跑一次完整构建：python main\\build_package.py）")
    print("=== 仅刷新前端（--web-only） ===")
    copy_web(DIST_OUT)
    write_build_info(DIST_OUT, "web-only")
    print(f"\n=== 完成：{DIST_OUT}（仅前端已更新） ===")


def main():
    parser = argparse.ArgumentParser(description="Nuitka onedir 构建（默认保留导入分析缓存）")
    parser.add_argument(
        "--clean", action="store_true",
        help="清空 Nuitka 模块缓存与中间目录后全量重编（正式出包/缓存可疑时用）",
    )
    parser.add_argument(
        "--web-only", action="store_true",
        help="不跑 Nuitka，只把 web/dist 刷新进已有 dist/tq-iquant（只改前端时用）",
    )
    args = parser.parse_args()

    if args.web_only:
        web_only()
        return

    if not ENTRY.exists():
        sys.exit(f"缺少桌面入口：{ENTRY}")
    if not ICON.exists():
        sys.exit(f"缺少图标：{ICON}（先运行 packaging/icon/build_icon.py）")

    if args.clean:
        purge_nuitka_cache()
        mode = "full-clean"
    else:
        prepare_cache_dir()
        mode = "full-incremental"

    print(f"版本：{VERSION}　模式：{mode}")
    print(f"缓存：{cache_stats()}")

    venv_python = ROOT / ".venv" / "Scripts" / "python.exe"
    python_exe = str(venv_python) if venv_python.exists() else sys.executable
    print(f"Python: {python_exe}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO / "shared") + os.pathsep + env.get("PYTHONPATH", "")
    run(nuitka_cmd(python_exe), cwd=str(ROOT), env=env)

    built = BUILD_OUT / "tq-iquant.dist"
    if not built.exists():
        candidates = list(BUILD_OUT.glob("*.dist"))
        if not candidates:
            sys.exit(f"未找到 Nuitka 产物目录：{BUILD_OUT}")
        built = candidates[0]

    if DIST_OUT.exists():
        shutil.rmtree(DIST_OUT)
    shutil.move(str(built), str(DIST_OUT))
    print(f"\n=== Nuitka 产物 -> {DIST_OUT} ===")

    copy_assets(DIST_OUT)
    write_build_info(DIST_OUT, mode)

    size_mb = sum(f.stat().st_size for f in DIST_OUT.rglob("*") if f.is_file()) / 1024 / 1024
    print(f"\n=== 完成：{DIST_OUT}（{size_mb:.1f} MB） ===")


if __name__ == "__main__":
    main()
