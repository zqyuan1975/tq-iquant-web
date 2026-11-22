from datetime import date
from threading import Lock
from typing import List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.config import load_config
from core.db import get_db
from core.models import BacktestRecord, PortfolioStrategy
from core.engine.market_gate import MarketGateUnavailableError
from core.services import backtest_cancel
from core.services.html_report import build_html_report
from core.services.backtest_export import (
    CSV_KINDS, PdfExportUnavailable, build_csv, html_to_pdf,
)
from core.services.signal_replay import replay_signals as _svc_replay_signals

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

# 并发保护：回测为同步内联执行，同时运行数上限由 config.max_concurrent_backtest
# 控制（默认 1）。超限时新请求返回 HTTP 409。与实盘互不互斥——实盘有自己的单
# session 守卫（live.py）。
_BACKTEST_GUARD = Lock()
_running_backtests = 0


def _max_concurrent_backtest() -> int:
    try:
        n = int(load_config().get("max_concurrent_backtest", 1) or 1)
    except (TypeError, ValueError):
        n = 1
    return max(1, n)


def _try_acquire_slot() -> bool:
    global _running_backtests
    with _BACKTEST_GUARD:
        if _running_backtests >= _max_concurrent_backtest():
            return False
        _running_backtests += 1
        return True


def _release_slot() -> None:
    global _running_backtests
    with _BACKTEST_GUARD:
        if _running_backtests > 0:
            _running_backtests -= 1



class BacktestRequest(BaseModel):
    portfolio_strategy_id: int
    name: str
    start_date: date
    end_date: date
    # 复权模式：front 前复权（默认，历史行为） / back 后复权 / dual 前复权信号+后复权记账
    dividend_mode: str = "front"
    # B6 考核口径：absolute 绝对收益（默认） / relative 相对收益；仅影响详情 acceptance 验收块
    evaluation_profile: str = "absolute"
    # C7 IS/OOS 采纳门槛（默认关）：启用后回测完成时评估 OOS CAGR ≥ min_ratio × IS CAGR
    # 且 OOS 不跑输基准，否则 gate_status=rejected（记录保留，仅标记不被采纳）
    is_oos_gate_enabled: bool = False
    oos_gate_min_ratio: float = 0.5
    # 费用预留（默认关）：开启后买入审批含佣金，避免现金为负；关闭为原行为
    fee_reserve_enabled: bool = False
    # 可转债 run 级开关（默认关，仅回测生效；关闭时与原逻辑完全一致）：
    # bond_t0_enabled：转债 T+0 回转（当日买入当日可卖，股票仍强制 T+1）；
    # bond_lot_enabled：转债最小交易单位 10 张（股票仍 100 股/手）
    bond_t0_enabled: bool = False
    bond_lot_enabled: bool = False
    # 自适应滑点 run 级覆盖（None=用组合默认）：临时开启/调整市场冲击成本，
    # 不改组合配置，便于同一组合 A/B 对比
    adaptive_slippage_enabled: bool | None = None
    slippage_impact_coef: float | None = None
    # 成交量约束 run 级覆盖（None=用组合默认）：临时开启/调整单笔成交量占比封顶
    volume_limit_enabled: bool | None = None
    volume_limit_ratio: float | None = None
    # 容量上限 run 级覆盖（None=用组合默认）：临时封顶 dynamic 仓位资金基座
    equity_cap: float | None = None
    # 上限处理模式 run 级覆盖（None=跟随组合 JSON 配置）：
    # cap=只封顶新开仓定量（默认，旧行为）；distribute=日终把超上限部分
    # 作为外部流出扣出现金（仅回测生效，不强制减仓）
    equity_cap_mode: str | None = None
    # 层 1 容量模型 run 级覆盖（None=linear 旧口径）：
    # linear=4% 成交量闸门 + 固定滑点（v1）；sqrt=参与率预算 + sqrt 冲击 +
    # 残量顺延切片（v2）。k 为 sqrt 冲击系数（0.05/0.10/0.20 做敏感性对照）。
    capacity_model: str | None = None
    capacity_impact_k: float | None = None
    # 成交量闸门流动性口径（2026-11-19，默认 None=same_day_bar 旧口径）：
    # same_day_bar=成交当根 bar 量（改动前行为）；prev5d_avg=前 5 个交易日
    # 日均量（显式选择才启用，与实盘同口径、无前视）。
    liquidity_caliber: str | None = None
    # 日末 peak 保护（默认关）：开启后 peak/max_drawdown 只在当日最后一根 bar 更新，
    # 避免混合周期粗 bar 的周期末 close 污染峰值；纯 1d 行为不变
    daily_close_peak_enabled: bool = False
    # 日内亏损熔断分母口径 run 级覆盖（None=跟随组合配置）：
    # equity=上一交易日收盘净值；initial=初始资金（旧口径，阈值恒定）
    daily_loss_basis: str | None = None
    # 市场多空门控 run 级三态覆盖（None=跟随组合市场门控配置）：
    # True=强制开启，False=强制关闭；空头状态只禁开仓/加仓，持仓不强平
    market_gate_enabled: bool | None = None
    # 行业门控 run 级临时覆盖（2026-11-22，默认 None=跟随组合 JSON 配置）：
    # 仅本次回测生效、不落盘。键同 portfolio_settings.INDUSTRY_DEFAULTS，
    # 未携带的键沿用组合设置；非法值按默认/夹取处理。
    industry_settings: dict | None = None
    # 执行档位（N1，2026-11-18）：None/legacy=旧成交口径（默认，行为逐字不变）；
    # realistic=一次性打开「涨跌停校验 + 成交量约束(10%) + 自适应滑点 + 日终抬峰」
    execution_profile: str | None = None
    # 执行档位逐开关覆盖（None=跟随档位）：成交量不足时买单单部分成交，残量顺延；
    # first_bar_no_fill=当日首根 bar 不接受买单（模拟「日线信号次日开盘才可能成交」）
    partial_fill_enabled: bool | None = None
    first_bar_no_fill: bool | None = None
    # 资金池 L2 借据建模（0021 B3，默认 None=不启用）：
    # {"lender_strategy_id": int, "borrower_strategy_id": int, "principal": float,
    #  "due_days": int, "enabled": bool}。仅当两策略同属该组合时成立；
    # 额度来自 A2 推导并注入 B2 capital_budget，快照写 loan_model_version。
    loan_model_plan: dict | None = None
    # 现金出口收益率建模（0021 C5，默认 None=不启用）：只估利息与可用时点，
    # 单列返回 cash_yield_contribution，不并入净值/成交/策略指标。
    cash_outlet_plan: dict | None = None
    # 门控释放/召回建模（0021 D3/D4，默认 None=不启用）：需 loan_model_plan 已成立。
    # {"enabled": bool, "portfolio_gate_enabled": bool?, "release_delay_days": int?,
    #  "reopen_confirm_days": int?, "due_days": int?}。只读时间轴落 gate_release_json。
    gate_release_plan: dict | None = None
    # 资金池拼盘（0021 B6）：由 L2 拼盘向导带出；回测成功后把记录绑到拼盘头。
    pairing_plan_id: int | None = None


class MultiBacktestRequest(BaseModel):
    """N1 多组合共享现金回测（2026-11-18）：≥2 个组合共用同一资金池。

    与单组合回测的区别只在资金审批：共享可用额（Σcash − Σ在途BUY）统一逐笔
    过 ``cash_allocator``。逐组合仍落独立 ``backtest_records``，另有一个
    ``backtest_run_groups`` 批记录承载聚合曲线。
    """
    portfolio_strategy_ids: List[int]
    name: str = "多组合回测"
    start_date: date
    end_date: date
    dividend_mode: str = "front"
    evaluation_profile: str = "absolute"
    daily_loss_basis: str | None = None
    market_gate_enabled: bool | None = None
    execution_profile: str | None = None
    partial_fill_enabled: bool | None = None
    first_bar_no_fill: bool | None = None
    # 层 1 容量模型 run 级覆盖（None=linear 旧口径）
    capacity_model: str | None = None
    capacity_impact_k: float | None = None
    # 成交量闸门流动性口径（None=same_day_bar 旧口径；prev5d_avg 需显式选择）
    liquidity_caliber: str | None = None
    # 0021 E2 会话池动态额度（None/未启用=各组合现金独立、旧行为）：
    # {enabled, min_share?, max_share?, min_cash_reserve_pct?}
    capital_pool_plan: dict | None = None
    # 0021 E3：绑定 L2 拼盘头（可空；普通多组合回测不传）。
    pairing_plan_id: int | None = None


class PoolCompareRequest(BaseModel):
    portfolio_strategy_id: int
    pool_ids: List[int]
    start_date: date
    end_date: date
    name_prefix: str = "换池对比"
    dividend_mode: str = "dual"


class CompareRecordsRequest(BaseModel):
    """K2 多记录横向对比：2~10 条已完成记录，只读汇总。"""
    record_ids: List[int]


class SignalReplayRequest(BaseModel):
    """N3 信号回放校验：同组合同区间重算信号，可选与实盘会话委托对照。"""
    portfolio_strategy_id: int
    start_date: date
    end_date: date
    dividend_mode: str = "front"
    session_id: int | None = None


class WalkForwardRequest(BaseModel):
    """C8 walk-forward 滚动验证：滚动 train_years 年训练 + test_years 年验证逐段前移，
    每窗子回测过 IS/OOS 门槛（C7 同一判定），全窗 passed 才 overall_pass。"""
    portfolio_strategy_id: int
    name: str
    start_date: date
    end_date: date
    train_years: int = 3
    test_years: int = 1
    oos_gate_min_ratio: float = 0.5
    dividend_mode: str = "front"
    evaluation_profile: str = "absolute"
    # 日内亏损熔断分母口径 run 级覆盖（None=跟随组合）
    daily_loss_basis: str | None = None


class ParamPerturbRequest(BaseModel):
    """P2b 参数微扰敏感性分析（可选，默认不使用）：对子策略数值参数逐一
    ±perturb_pct 扰动各跑一轮子回测 + 1 轮基准，汇总各参数 CAGR 敏感度。"""
    portfolio_strategy_id: int
    name: str
    start_date: date
    end_date: date
    perturb_pct: float = 0.10
    dividend_mode: str = "front"
    evaluation_profile: str = "absolute"
    # 日内亏损熔断分母口径 run 级覆盖（None=跟随组合）
    daily_loss_basis: str | None = None


# ---------------------------------------------------------------------------
# 业务逻辑已下沉到 core.services.backtest_service（P1 #9）。
# 下方 re-export 供测试 bt_api.xxx 兼容（test_backtest_data.py / test_backtest_api.py
# 直接 import core.api.backtest 引用 build_klines 等数据层符号 + monkeypatch TQData/TQFormula）。
# 类方法 monkeypatch 与 import 路径无关，re-export 后仍生效。
# ---------------------------------------------------------------------------
from core.services.backtest_service import (  # noqa: E402
    build_klines,
    build_benchmark_data,
    build_signal_cache,
    DIVIDEND_MODES,
    warmup_counts_by_period,
    _convert_market_data,
    _convert_market_data_multi,
    _convert_formula_output,
    _merge_raw_by_period,
    _slice_klines_from,
    _MINUTE_PERIODS,
    list_records as _svc_list_records,
    compare_records as _svc_compare_records,
    get_record_detail as _svc_get_record_detail,
    delete_record as _svc_delete_record,
    run_backtest as _svc_run_backtest,
    recompute_crisis_alpha as _svc_recompute_crisis_alpha,
    recompute_market_regime as _svc_recompute_market_regime,
    recompute_evaluation as _svc_recompute_evaluation,
    recompute_robustness as _svc_recompute_robustness,
    recompute_validation_crisis as _svc_recompute_validation_crisis,
    recompute_validation as _svc_recompute_validation,
    recompute_persistence as _svc_recompute_persistence,
    run_pool_comparison as _svc_run_pool_comparison,
    run_multi_backtest as _svc_run_multi_backtest,
    list_run_groups as _svc_list_run_groups,
    get_run_group as _svc_get_run_group,
    cancel_run_group as _svc_cancel_run_group,
)
from core.services.walk_forward_service import (  # noqa: E402
    run_walk_forward as _svc_run_walk_forward,
    list_walk_forward_runs as _svc_list_walk_forward_runs,
    get_walk_forward_run as _svc_get_walk_forward_run,
    delete_walk_forward_run as _svc_delete_walk_forward_run,
)
from core.services.param_perturb_service import (  # noqa: E402
    run_param_perturb as _svc_run_param_perturb,
    list_param_perturb_runs as _svc_list_param_perturb_runs,
    get_param_perturb_run as _svc_get_param_perturb_run,
    delete_param_perturb_run as _svc_delete_param_perturb_run,
)
from core.tq.data import TQData  # noqa: E402  re-export（测试 monkeypatch bt_api.TQData）
from core.tq.formula import TQFormula  # noqa: E402  re-export（同上）


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------
@router.get("/records")
def list_records(db: Session = Depends(get_db)):
    return ok(_svc_list_records(db))


@router.post("/records/compare")
def compare_records(req: CompareRecordsRequest, db: Session = Depends(get_db)):
    """K2 多记录横向对比（只读）：曲线叠加 + 指标表 + 参数 diff + 成交 diff。

    用 POST 避免与 ``GET /records/{record_id}`` 的路径参数歧义；
    记录不存在 → 404，条数越界/不足 → 400。
    """
    payload, msg = _svc_compare_records(db, req.record_ids)
    if payload is None:
        return err(404 if msg and "不存在" in msg else 400, msg)
    return ok(payload)


@router.post("/signal-replay")
def signal_replay(req: SignalReplayRequest, db: Session = Depends(get_db)):
    """N3 信号回放校验：重算区间信号，可选与实盘会话委托按 (日期,股票,方向) 对照。

    只读、不落库；组合不存在 → 404，日期区间非法 → 400。
    """
    payload, msg = _svc_replay_signals(
        db, req.portfolio_strategy_id, req.start_date, req.end_date,
        dividend_mode=req.dividend_mode, session_id=req.session_id,
    )
    if payload is None:
        return err(404 if msg and "不存在" in msg else 400, msg)
    return ok(payload)


@router.get("/records/{record_id}")
def get_record(record_id: int, profile: Optional[str] = None, db: Session = Depends(get_db)):
    # B6：?profile=absolute|relative 临时切换验收口径查看（不改记录存储口径）
    detail = _svc_get_record_detail(db, record_id, profile_override=profile)
    if detail is None:
        return err(404, "回测记录不存在")
    return ok(detail)


@router.delete("/records/{record_id}")
def delete_record(record_id: int, db: Session = Depends(get_db)):
    """删除回测记录 + 级联子表（trades/snapshots/evaluations/decisions）。
    子表 FK 虽配 ondelete=CASCADE，但显式删更稳妥（不依赖连接级 PRAGMA）。

    #4: status=running 时拒绝删除（409），防止并发删除运行中的回测引发
    FK IntegrityError 500 或破坏其写入——对照 live 删除守卫。
    """
    ok_flag, msg = _svc_delete_record(db, record_id)
    if not ok_flag:
        status = 409 if (msg and "运行中" in msg) else 404
        return err(status, msg)
    return ok()


@router.post("/records/{record_id}/cancel")
def cancel_record(record_id: int, db: Session = Depends(get_db)):
    """K1 协作式取消：请求中止正在运行的回测（引擎在下一根 bar 前退出）。

    同步内联执行无法强杀，故只置位取消令牌；记录最终由 service 层标
    ``canceled`` 并写入进度。多组合回测的任一子记录会中止整批。
    - 404：记录不存在
    - 409：记录不在运行中，或不在本进程运行列表（已结束/其它实例执行）
    """
    rec = db.get(BacktestRecord, record_id)
    if rec is None:
        return err(404, "回测记录不存在")
    if rec.status != "running":
        return err(409, "回测不在运行中，无法取消")
    if not backtest_cancel.cancel(record_id):
        return err(409, "该回测不在本进程运行列表中（可能已结束），无法取消")
    return ok({"record_id": record_id, "cancel_requested": True})


@router.post("/records/{record_id}/crisis-alpha/recompute")
def recompute_record_crisis_alpha(record_id: int, db: Session = Depends(get_db)):
    result = _svc_recompute_crisis_alpha(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/records/{record_id}/market-regime/recompute")
def recompute_record_market_regime(record_id: int, db: Session = Depends(get_db)):
    """离线重算市场状态×绩效分析（机遇/中性Alpha，不重跑回测）。"""
    result = _svc_recompute_market_regime(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/records/{record_id}/evaluations/recompute")
def recompute_record_evaluation(record_id: int, db: Session = Depends(get_db)):
    """离线重算组合级评估（不重跑回测），供老记录补算基准相对等新增指标。"""
    result = _svc_recompute_evaluation(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    if result == "running":
        return err(409, "回测运行中，请稍后再试")
    return ok(result)


@router.post("/records/{record_id}/robustness/recompute")
def recompute_record_robustness(record_id: int, db: Session = Depends(get_db)):
    """离线重算稳健性检验（年度/3年滚动/集中度/样本内外，不触发回测）。"""
    result = _svc_recompute_robustness(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/records/{record_id}/validation-crisis/recompute")
def recompute_record_validation_crisis(record_id: int, db: Session = Depends(get_db)):
    """离线刷新验证结论的危机防御块（新窗口/新判定，仅用落库快照，不重跑回测）。"""
    result = _svc_recompute_validation_crisis(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/records/{record_id}/validation/recompute")
def recompute_record_validation(record_id: int, db: Session = Depends(get_db)):
    """离线重算验证结论全量（capacity 含敞口上限维度 + crisis + oos，不重跑回测）。

    capacity 从落库快照/评估/策略配置重建；ADV 容量预警需逐股日线（离线无
    klines）→ 跳过。新回测完成时由 build_validation_verdict 自动含全部维度。
    """
    result = _svc_recompute_validation(db, record_id)
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/records/{record_id}/persistence/recompute")
def recompute_record_persistence(
    record_id: int, benchmark_code: str | None = None, db: Session = Depends(get_db),
):
    """离线重算持续性检验（分年度超额/风险/交易结构 + 滚动年化，判定阿尔法衰减 vs 风格钟摆）。

    benchmark_code：可选判定基准覆盖（默认组合基准）。仅影响本模块，不改组合配置。
    """
    try:
        result = _svc_recompute_persistence(db, record_id, benchmark_code=benchmark_code)
    except ValueError as e:
        return err(400, str(e))
    if result is None:
        return err(404, "回测记录不存在")
    return ok(result)


@router.post("/robustness/compare")
def compare_pool_robustness(req: PoolCompareRequest, db: Session = Depends(get_db)):
    """换池稳健性对比：同参数仅换池逐个重跑回测并返回对比结果。

    同步内联执行（每池分钟级），需持回测并发锁（与单跑回测互斥）。
    """
    ps = db.get(PortfolioStrategy, req.portfolio_strategy_id)
    if ps is None:
        return err(404, "组合策略不存在")
    if not req.pool_ids:
        return err(400, "请至少选择一个备选股票池")
    if req.start_date >= req.end_date:
        return err(400, f"开始日期必须早于结束日期，收到 {req.start_date} ~ {req.end_date}")
    # 未来日期 TQ 拉不到行情：提前拦截，避免每池各跑一轮才失败（多池耗时放大）
    today = date.today()
    if req.start_date > today:
        return err(400, f"开始日期不可在未来，收到 {req.start_date}（今天 {today}）")
    if req.end_date > today:
        return err(400, f"结束日期不可在未来，收到 {req.end_date}（今天 {today}）")
    if req.dividend_mode not in DIVIDEND_MODES:
        return err(400, f"复权模式仅支持 {'/'.join(DIVIDEND_MODES)}，收到 {req.dividend_mode!r}")

    if not _try_acquire_slot():
        raise HTTPException(
            status_code=409,
            detail="回测正在进行中，请等待当前回测完成后再启动（已达并发上限）",
        )
    try:
        results = _svc_run_pool_comparison(
            db, req.portfolio_strategy_id, req.pool_ids,
            req.start_date, req.end_date,
            req.name_prefix, req.dividend_mode,
        )
        return ok({"comparisons": results})
    finally:
        _release_slot()


@router.get("/records/{record_id}/export/html")
def export_record_html(record_id: int, db: Session = Depends(get_db)):
    """导出单文件离线 HTML 分析报告（FR-21/22，Task 19）。

    例外于统一 envelope：文件下载直接返回 text/html + Content-Disposition，
    前端以直链/a 标签触发浏览器下载（不经 axios 拦截器）。数字与详情接口同源。
    """
    detail = _svc_get_record_detail(db, record_id)
    if detail is None:
        return err(404, "回测记录不存在")
    report = build_html_report(detail)
    rec = detail.get("record") or {}
    fname = quote(f"回测报告-{rec.get('name') or record_id}.html")
    return HTMLResponse(
        content=report,
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{fname}"},
    )


_CSV_KIND_LABEL = {"trades": "成交明细", "snapshots": "日快照", "metrics": "指标汇总"}


@router.get("/records/{record_id}/export/csv")
def export_record_csv(
    record_id: int, kind: str = "trades", db: Session = Depends(get_db),
):
    """K6 导出 CSV（kind=trades|snapshots|metrics）。

    与 HTML 导出一致：文件下载例外于统一 envelope，直接返回 text/csv +
    Content-Disposition；带 UTF-8 BOM，Excel 打开不乱码。
    """
    if kind not in CSV_KINDS:
        return err(400, f"不支持的导出类型：{kind}（可选 {'/'.join(CSV_KINDS)}）")
    detail = _svc_get_record_detail(db, record_id)
    if detail is None:
        return err(404, "回测记录不存在")
    text = build_csv(detail, kind)
    rec = detail.get("record") or {}
    fname = quote(f"回测-{rec.get('name') or record_id}-{_CSV_KIND_LABEL[kind]}.csv")
    return Response(
        content=text.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{fname}"},
    )


@router.get("/records/{record_id}/export/pdf")
def export_record_pdf(record_id: int, db: Session = Depends(get_db)):
    """K6 导出 PDF：无头浏览器打印离线 HTML 报告（中文字体/图表与在线一致）。

    离线环境无 Edge/Chrome 时返回 503 并提示改用 CSV/HTML（不静默降级）。
    """
    detail = _svc_get_record_detail(db, record_id)
    if detail is None:
        return err(404, "回测记录不存在")
    try:
        pdf = html_to_pdf(build_html_report(detail))
    except PdfExportUnavailable as exc:
        return err(503, str(exc))
    rec = detail.get("record") or {}
    fname = quote(f"回测报告-{rec.get('name') or record_id}.pdf")
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{fname}"},
    )


def _validate_backtest_request(req: BacktestRequest) -> Optional[str]:
    """回测请求基础校验，返回错误消息或 None（通过）。
    校验日期区间：start < end，且 start 不在未来（TQ 拉不到未来行情）。
    校验复权模式取值。"""
    if req.start_date >= req.end_date:
        return f"开始日期必须早于结束日期，收到 {req.start_date} ~ {req.end_date}"
    today = date.today()
    if req.start_date > today:
        return f"开始日期不可在未来，收到 {req.start_date}（今天 {today}）"
    if req.end_date > today:
        return f"结束日期不可在未来，收到 {req.end_date}（今天 {today}）"
    if not req.name.strip():
        return "回测名称不能为空"
    if req.dividend_mode not in DIVIDEND_MODES:
        return f"复权模式仅支持 {'/'.join(DIVIDEND_MODES)}，收到 {req.dividend_mode!r}"
    if req.evaluation_profile not in ("absolute", "relative"):
        return f"评估口径仅支持 absolute/relative，收到 {req.evaluation_profile!r}"
    # C7：门槛比例须在 [0.1, 2]（<0.1 过松失意义，>2 要求 OOS 两倍于 IS 已失合理性）
    if not (0.1 <= req.oos_gate_min_ratio <= 2):
        return f"IS/OOS 门槛比例须在 [0.1, 2] 区间，收到 {req.oos_gate_min_ratio}"
    # 自适应滑点 run 级覆盖：冲击系数须在 (0,1]
    if req.slippage_impact_coef is not None and not (0 < req.slippage_impact_coef <= 1):
        return f"slippage_impact_coef 须在 (0,1]，收到 {req.slippage_impact_coef}"
    # 成交量约束 run 级覆盖：占比须在 (0,1]
    if req.volume_limit_ratio is not None and not (0 < req.volume_limit_ratio <= 1):
        return f"volume_limit_ratio 须在 (0,1]，收到 {req.volume_limit_ratio}"
    # 容量上限 run 级覆盖：必须 > 0
    if req.equity_cap is not None and req.equity_cap <= 0:
        return f"equity_cap 必须 > 0（None=不设上限/用组合默认），收到 {req.equity_cap}"
    # 上限处理模式：None=跟随组合；显式值仅限 cap/distribute
    if req.equity_cap_mode not in (None, "cap", "distribute"):
        return (f"equity_cap_mode 仅支持 cap/distribute（None=跟随组合），"
                f"收到 {req.equity_cap_mode!r}")
    # 日内亏损熔断分母口径 run 级覆盖：None=跟随组合，显式值仅限 equity/initial
    if req.daily_loss_basis is not None and req.daily_loss_basis not in ("equity", "initial"):
        return (f"daily_loss_basis 仅支持 equity/initial（None=跟随组合），"
                f"收到 {req.daily_loss_basis!r}")
    # 执行档位：None=legacy（旧成交口径）；显式值仅限 legacy/realistic
    if req.execution_profile not in (None, "", "legacy", "realistic"):
        return (f"execution_profile 仅支持 legacy/realistic（None=legacy），"
                f"收到 {req.execution_profile!r}")
    # 层 1 容量模型：None=linear；显式值仅限 linear/sqrt；k 须在 (0,1]
    if req.capacity_model not in (None, "linear", "sqrt"):
        return (f"capacity_model 仅支持 linear/sqrt（None=linear），"
                f"收到 {req.capacity_model!r}")
    if (req.capacity_impact_k is not None
            and not (0 < req.capacity_impact_k <= 1)):
        return f"capacity_impact_k 须在 (0,1]，收到 {req.capacity_impact_k}"
    if req.liquidity_caliber not in (None, "", "same_day_bar", "prev5d_avg"):
        return (f"liquidity_caliber 仅支持 same_day_bar/prev5d_avg"
                f"（None=same_day_bar），收到 {req.liquidity_caliber!r}")
    return None


@router.post("")
def run_backtest(req: BacktestRequest, db: Session = Depends(get_db)):
    """启动回测（同步内联执行）。并发保护：并发上限由 config.max_concurrent_backtest
    控制（默认 1），达到上限则 409。

    校验（404/400）在抢槽外，只对合法请求抢槽；槽由 finally 保证异常路径也释放。
    主链路在 core.services.backtest_service.run_backtest（P1 #9 阶段 3 迁出）。
    """
    ps = db.get(PortfolioStrategy, req.portfolio_strategy_id)
    if ps is None:
        return err(404, "组合策略不存在")

    err_msg = _validate_backtest_request(req)
    if err_msg:
        return err(400, err_msg)

    if not _try_acquire_slot():
        # 有意保留真实 HTTP 409（非 body-code）：并发契约 + 测试断言 status_code==409
        # （test_post_backtest_409_when_already_running）。body-code 会破坏前端对并发冲突
        # 的 HTTP 状态码判断。与统一 envelope 的模式 A 不同——此为刻意的模式 B 例外。
        raise HTTPException(
            status_code=409,
            detail="回测正在进行中，请等待当前回测完成后再启动（已达并发上限）",
        )
    try:
        payload = _svc_run_backtest(db, ps, req)
        if req.pairing_plan_id:
            try:
                from core.services import pairing_service
                pairing_service.link_backtest(
                    db, req.pairing_plan_id, payload.get("record_id"))
            except Exception:  # noqa: BLE001 — 绑定失败不影响回测结果
                logger.warning("拼盘回测记录绑定失败 plan=%s", req.pairing_plan_id,
                               exc_info=True)
        return ok(payload)
    except MarketGateUnavailableError as exc:
        # 门控基准取数/公式无输出：属可操作的配置/数据问题，明确回 400
        # （不落 record，service 层在写库前抛出）。其它异常保持 500 语义不变。
        return err(400, str(exc))
    finally:
        _release_slot()


# ---------------------------------------------------------------------------
# N1 多组合共享现金回测（2026-11-18）
# ---------------------------------------------------------------------------
def _validate_multi_request(req: MultiBacktestRequest) -> Optional[str]:
    """多组合回测请求校验：≥2 个不重复组合 + 日期/口径与单组合同一套规则。"""
    if len(req.portfolio_strategy_ids) < 2:
        return "多组合回测至少需要 2 个组合策略"
    if len(set(req.portfolio_strategy_ids)) != len(req.portfolio_strategy_ids):
        return "portfolio_strategy_ids 不可重复"
    if req.start_date >= req.end_date:
        return f"开始日期必须早于结束日期，收到 {req.start_date} ~ {req.end_date}"
    today = date.today()
    if req.start_date > today:
        return f"开始日期不可在未来，收到 {req.start_date}（今天 {today}）"
    if req.end_date > today:
        return f"结束日期不可在未来，收到 {req.end_date}（今天 {today}）"
    if req.dividend_mode not in DIVIDEND_MODES:
        return f"复权模式仅支持 {'/'.join(DIVIDEND_MODES)}，收到 {req.dividend_mode!r}"
    if req.evaluation_profile not in ("absolute", "relative"):
        return f"评估口径仅支持 absolute/relative，收到 {req.evaluation_profile!r}"
    if req.daily_loss_basis not in (None, "equity", "initial"):
        return (f"daily_loss_basis 仅支持 equity/initial（None=跟随组合），"
                f"收到 {req.daily_loss_basis!r}")
    if req.execution_profile not in (None, "", "legacy", "realistic"):
        return (f"execution_profile 仅支持 legacy/realistic（None=legacy），"
                f"收到 {req.execution_profile!r}")
    if req.capacity_model not in (None, "linear", "sqrt"):
        return (f"capacity_model 仅支持 linear/sqrt（None=linear），"
                f"收到 {req.capacity_model!r}")
    if (req.capacity_impact_k is not None
            and not (0 < req.capacity_impact_k <= 1)):
        return f"capacity_impact_k 须在 (0,1]，收到 {req.capacity_impact_k}"
    if req.liquidity_caliber not in (None, "", "same_day_bar", "prev5d_avg"):
        return (f"liquidity_caliber 仅支持 same_day_bar/prev5d_avg"
                f"（None=same_day_bar），收到 {req.liquidity_caliber!r}")
    return None


@router.post("/run-multi")
def run_multi_backtest(req: MultiBacktestRequest, db: Session = Depends(get_db)):
    """多组合共享现金回测（N1）：≥2 个组合共用同一资金池，同步内联执行。

    与 ``POST /api/backtest`` 共用同一个并发槽（同一时刻最多 N 个回测在跑），
    超限同样返回真实 HTTP 409。
    """
    err_msg = _validate_multi_request(req)
    if err_msg:
        return err(400, err_msg)
    portfolios = (
        db.query(PortfolioStrategy)
        .filter(PortfolioStrategy.id.in_(req.portfolio_strategy_ids))
        .all()
    )
    found = {p.id for p in portfolios}
    missing = [pid for pid in req.portfolio_strategy_ids if pid not in found]
    if missing:
        return err(404, f"组合策略不存在：{missing}")
    # 保持请求顺序（资金优先级 = 用户看到的顺序）
    order = {pid: i for i, pid in enumerate(req.portfolio_strategy_ids)}
    portfolios.sort(key=lambda p: order.get(p.id, 0))

    if not _try_acquire_slot():
        raise HTTPException(
            status_code=409,
            detail="回测正在进行中，请等待当前回测完成后再启动（已达并发上限）",
        )
    try:
        return ok(_svc_run_multi_backtest(db, portfolios, req))
    except MarketGateUnavailableError as exc:
        return err(400, str(exc))
    except ValueError as exc:
        # 取不到行情等可操作的数据问题（不落 running 记录）
        return err(400, str(exc))
    finally:
        _release_slot()


@router.get("/run-groups")
def list_run_groups(db: Session = Depends(get_db)):
    """多组合回测批列表（id 倒序；含 status 与参数快照）。"""
    return ok(_svc_list_run_groups(db))


@router.get("/run-groups/{group_id}")
def get_run_group(group_id: int, db: Session = Depends(get_db)):
    """批详情：组信息 + 子记录 + 聚合曲线。"""
    payload = _svc_get_run_group(db, group_id)
    if payload is None:
        return err(404, "多组合回测批不存在")
    return ok(payload)


@router.get("/run-groups/{group_id}/aggregate")
def get_run_group_aggregate(group_id: int, db: Session = Depends(get_db)):
    """仅聚合曲线（前端画多组合合计净值用）。"""
    payload = _svc_get_run_group(db, group_id)
    if payload is None:
        return err(404, "多组合回测批不存在")
    return ok(payload.get("aggregate") or {})


@router.post("/run-groups/{group_id}/cancel")
def cancel_run_group(group_id: int, db: Session = Depends(get_db)):
    """组级取消（0021 E3）：取消组内任一成员即中止整批。"""
    payload = _svc_cancel_run_group(db, group_id)
    if payload is None:
        return err(404, "多组合回测批不存在")
    if not payload.get("running_members"):
        return err(409, f"该批当前没有运行中的成员（状态 {payload.get('status')}）")
    if not payload.get("cancel_requested"):
        return err(409, "运行中成员均未在本进程注册，无法取消")
    return ok(payload)


# ---------------------------------------------------------------------------
# C8 walk-forward 滚动验证
# ---------------------------------------------------------------------------
def _validate_walk_forward_request(req: WalkForwardRequest) -> Optional[str]:
    """WF 请求校验，返回错误消息或 None（通过）。"""
    if req.start_date >= req.end_date:
        return f"开始日期必须早于结束日期，收到 {req.start_date} ~ {req.end_date}"
    today = date.today()
    if req.start_date > today:
        return f"开始日期不可在未来，收到 {req.start_date}（今天 {today}）"
    if req.end_date > today:
        return f"结束日期不可在未来，收到 {req.end_date}（今天 {today}）"
    if req.dividend_mode not in DIVIDEND_MODES:
        return f"复权模式仅支持 {'/'.join(DIVIDEND_MODES)}，收到 {req.dividend_mode!r}"
    if req.evaluation_profile not in ("absolute", "relative"):
        return f"评估口径仅支持 absolute/relative，收到 {req.evaluation_profile!r}"
    if req.train_years < 1:
        return f"训练窗口须 ≥ 1 年，收到 {req.train_years}"
    if req.test_years < 1:
        return f"验证窗口须 ≥ 1 年，收到 {req.test_years}"
    if not (0.1 <= req.oos_gate_min_ratio <= 2):
        return f"IS/OOS 门槛比例须在 [0.1, 2] 区间，收到 {req.oos_gate_min_ratio}"
    if req.daily_loss_basis not in (None, "equity", "initial"):
        return (f"daily_loss_basis 仅支持 equity/initial（None=跟随组合），"
                f"收到 {req.daily_loss_basis!r}")
    return None


@router.post("/walk-forward")
def run_walk_forward(req: WalkForwardRequest, db: Session = Depends(get_db)):
    """启动 walk-forward 滚动验证（同步内联，逐窗串行子回测）。

    整轮持一个回测并发槽（与单跑回测/换池对比互斥）；内部逐窗调
    run_backtest（服务层不持锁，锁只在本路由层）。
    """
    ps = db.get(PortfolioStrategy, req.portfolio_strategy_id)
    if ps is None:
        return err(404, "组合策略不存在")
    err_msg = _validate_walk_forward_request(req)
    if err_msg:
        return err(400, err_msg)

    if not _try_acquire_slot():
        raise HTTPException(
            status_code=409,
            detail="回测正在进行中，请等待当前回测完成后再启动（已达并发上限）",
        )
    try:
        return ok(_svc_run_walk_forward(db, ps, req))
    finally:
        _release_slot()


@router.get("/walk-forward")
def list_walk_forward_runs(db: Session = Depends(get_db)):
    return ok(_svc_list_walk_forward_runs(db))


@router.get("/walk-forward/{run_id}")
def get_walk_forward_run(run_id: int, db: Session = Depends(get_db)):
    run = _svc_get_walk_forward_run(db, run_id)
    if run is None:
        return err(404, "walk-forward 运行不存在")
    return ok(run)


@router.delete("/walk-forward/{run_id}")
def delete_walk_forward_run(run_id: int, db: Session = Depends(get_db)):
    """删除 walk-forward 运行 + 其子回测记录（级联清子表）。running → 409。"""
    ok_flag, msg = _svc_delete_walk_forward_run(db, run_id)
    if not ok_flag:
        status = 409 if "运行中" in msg else 404
        return err(status, msg)
    return ok()


# ---------------------------------------------------------------------------
# P2b 参数微扰敏感性分析（可选，默认不使用）
# ---------------------------------------------------------------------------
def _validate_param_perturb_request(req: ParamPerturbRequest) -> Optional[str]:
    """微扰请求校验，返回错误消息或 None（通过）。"""
    if req.start_date >= req.end_date:
        return f"开始日期必须早于结束日期，收到 {req.start_date} ~ {req.end_date}"
    today = date.today()
    if req.start_date > today:
        return f"开始日期不可在未来，收到 {req.start_date}（今天 {today}）"
    if req.end_date > today:
        return f"结束日期不可在未来，收到 {req.end_date}（今天 {today}）"
    if req.dividend_mode not in DIVIDEND_MODES:
        return f"复权模式仅支持 {'/'.join(DIVIDEND_MODES)}，收到 {req.dividend_mode!r}"
    if req.evaluation_profile not in ("absolute", "relative"):
        return f"评估口径仅支持 absolute/relative，收到 {req.evaluation_profile!r}"
    # 扰动幅度上限 50%：再大已不是"微"扰，且 int 参数缩放失真严重
    if not (0 < req.perturb_pct <= 0.5):
        return f"扰动幅度须在 (0, 0.5] 区间，收到 {req.perturb_pct}"
    if req.daily_loss_basis not in (None, "equity", "initial"):
        return (f"daily_loss_basis 仅支持 equity/initial（None=跟随组合），"
                f"收到 {req.daily_loss_basis!r}")
    return None


@router.post("/param-perturb")
def run_param_perturb(req: ParamPerturbRequest, db: Session = Depends(get_db)):
    """启动参数微扰敏感性分析（同步内联，逐轮串行子回测）。

    整轮持一个回测并发槽（与单跑回测/walk-forward 互斥）；内部逐轮调
    run_backtest（服务层不持锁，锁只在本路由层）。
    """
    ps = db.get(PortfolioStrategy, req.portfolio_strategy_id)
    if ps is None:
        return err(404, "组合策略不存在")
    err_msg = _validate_param_perturb_request(req)
    if err_msg:
        return err(400, err_msg)

    if not _try_acquire_slot():
        raise HTTPException(
            status_code=409,
            detail="回测正在进行中，请等待当前回测完成后再启动（已达并发上限）",
        )
    try:
        return ok(_svc_run_param_perturb(db, ps, req))
    finally:
        _release_slot()


@router.get("/param-perturb")
def list_param_perturb_runs(db: Session = Depends(get_db)):
    return ok(_svc_list_param_perturb_runs(db))


@router.get("/param-perturb/{run_id}")
def get_param_perturb_run(run_id: int, db: Session = Depends(get_db)):
    run = _svc_get_param_perturb_run(db, run_id)
    if run is None:
        return err(404, "参数微扰运行不存在")
    return ok(run)


@router.delete("/param-perturb/{run_id}")
def delete_param_perturb_run(run_id: int, db: Session = Depends(get_db)):
    """删除参数微扰运行 + 其子回测记录（级联清子表）。running → 409。"""
    ok_flag, msg = _svc_delete_param_perturb_run(db, run_id)
    if not ok_flag:
        status = 409 if "运行中" in msg else 404
        return err(status, msg)
    return ok()
