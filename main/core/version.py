"""应用版本号（Python 运行时唯一来源）。

出包版本号以 ``packaging/app-version.iss`` 的 ``AppVersion`` 为唯一真相：
``main/build_package.py`` 读取该文件写入 exe 元数据与 build-info.json。
本常量仅供 FastAPI 元数据与 ``/api/status`` 展示使用，必须与该文件保持一致
（tests 无强校验，升级版本时两处同步改）。
"""

APP_VERSION = "1.0.4"
