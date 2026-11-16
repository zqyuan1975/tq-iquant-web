"""HttpBridgeDispatcher — 通过 HTTP 调 QMT 客户端内桥（0009 切片2）。

桥（live/bridge/iquant_bridge.py）在 QMT 客户端内暴露 127.0.0.1:8790 HTTP 服务，
本 dispatcher 实现 OrderDispatcher 接口，把下单/查询转成对桥的 HTTP 调用。

真实下单语义：
  - place_order 桥「受理成功」（passorder 返回 0）即构造 TradeEvent 返回。
    成交价格首期用请求价（order.price = bar.close 近似）；prType=14 对手价实际
    成交价是盘口一档价（≠ close），真实成交回报轮询在切片5 /deals 回填。
  - /order 返回 ok:false 时按「错误性质」分三类处理（见 `_classify_bridge_error`）：
      * **明确拒单**（柜台 rejected=true 带 reason，或业务校验文案如涨跌停/可用不足/
        数量非法）→ 抛 BridgeOrderRejected（带桥侧原文），上层标 rejected 并回显原因；
      * **瞬时故障**（桥内 gm 交易服务断连窗口的 "order_volume raised: ..." /
        "returned empty list" / "query_*" / 连接层文案如 WinError 10061、timed out）
        → 返回 None（在途）+ 记 last_unconfirmed_order_id，**绝不判拒单**：
        这类错误不代表柜台收到了委托，判 rejected 会让真实在途单脱跟踪；
      * **未知错误** → 保守判「明确拒单」（宁可人工确认，也不把真拒单挂成在途）。
    桥返回 {ok:false} 但无 error（非 JSON 故障路径）→ 返回 None（#24 语义，不静默吞）。
  - 桥网络不可用（QMT 客户端离线）→ 抛 BridgeUnavailableError，上层暂停交易。
  - **下单超时/连接中断（请求已发出但未拿到应答）→ 返回 None（在途）+ 记
    last_unconfirmed_order_id**：慢桥（全局锁排队）下不得把真实在途单判成拒单，
    引擎保持 submitted 由后续 /orders 回查认领（对齐 TdxTqDispatcher 先例）。
    连接被拒（请求从未发出）仍抛 BridgeUnavailableError。
  - 单次调用耗时超 slow_threshold → logger.warning + on_slow 回调（引擎转 risk 事件），
    按「会话（本 dispatcher 实例）」维度做窗口节流：每窗口最多一条，窗口内的后续
    慢请求累计计数，在窗口过期后的首条里汇总（避免同一分钟对多个端点各报一条）。

幂等：同一 OrderEvent 生成确定性 order_id（策略/组合/股票/方向/信号/时间 的 MD5），
桥侧按 order_id 去重，重复请求不重复下单。
"""
import hashlib
import logging
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Callable, Dict, Optional, Tuple

import httpx

from .event import OrderEvent, TradeEvent
from .execution_engine import OrderDispatcher
from .market_rules import _limit_ratio_for
from ..config import load_config
from tq_iquant_shared.constants import TradeType

logger = logging.getLogger(__name__)


def _resolve_timeout(explicit, config_key: str, default: float) -> float:
    """超时解析：显式参数 > config.live.<config_key> > 默认（非法/<=0 回退默认）。

    桥（QMT/掘金）单次请求会被桥内全局锁/GIL 排队，读超时贴太近（旧 10s）会把
    正常慢应答误判成断链，进而误判通道异常。默认读超时 15s、连接 3s，均可经
    config.live 调整（不改代码）。
    """
    def _valid(v) -> Optional[float]:
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return f if f > 0 else None

    if explicit is not None:
        return _valid(explicit) or default
    try:
        cfg = load_config() or {}
        live = cfg.get("live") if isinstance(cfg, dict) else None
        value = live.get(config_key) if isinstance(live, dict) else None
    except Exception:  # 配置读取/格式异常不得影响构造（回退默认）
        return default
    return _valid(value) or default


def _positive_float(value) -> Optional[float]:
    """正数价格 → float；None/0/负数/nan/不可解析 → None（桥缺字段的兜底口径）。"""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f <= 0:
        return None
    return f


class BridgeUnavailableError(RuntimeError):
    """桥不可用（QMT 客户端离线 / 未启动 / 网络异常）。"""


class BridgeOrderRejected(RuntimeError):
    """桥业务拒绝下单（白名单/限额/重复），message 带桥侧真实原因。

    与 BridgeUnavailableError 的区别：桥在线、正常受理请求，但业务上拒单
    （如 volume 超限、股票不在白名单、重复单）。上层据此标 rejected 并回显原因，
    而不是笼统的 "approval failed"。
    """


class BridgeOrderIdError(BridgeOrderRejected):
    """确定性订单 ID / 认领键护栏失败（长度不足或 20 位 remark 撞单）。

    继承 ``BridgeOrderRejected`` 是刻意的：引擎既有拒单路径（标 rejected +
    决策事件 + SSE 回显 error_message）直接生效，不需要新增捕获分支。拒单原因
    带「订单ID护栏」字样，与桥业务拒单（涨跌停/可用不足）可区分。
    """


class HttpBridgeDispatcher(OrderDispatcher):
    channel = "iquant_bridge"
    #: 桥把确定性 bridge_order_id（前 20 位）写进柜台 remark，支持 remark 精确认领
    #: （与 TDX/THS 的 supports_remark=False 相对）。订单超窗无法追溯时按「保持
    #: pending + risk 事件」处理，不自动驳回（柜台当日委托查询不含废单）。
    supports_remark = True
    #: P2-6：/quote 返回柜台 K 线，断线恢复后可按更大 count 回补错过的历史 bar
    #: （快照合成源没有该能力，BarPoller 据此区分「可补派」与「只能记缺口」）。
    supports_historical_quote = True

    #: 下单请求「已发出但结果未知」的异常（超时/连接中断/协议中断）。
    #: 与连接被拒（ConnectError/ConnectTimeout，请求从未发出）区分：
    #: 前者按在途处理（返回 None），后者仍是桥不可用。
    _INFLIGHT_ERRORS: Tuple[type, ...] = (
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.RemoteProtocolError,
        httpx.ReadError,
        httpx.WriteError,
    )

    #: /order 应答里可识别的委托标识字段（兼容桥契约变更前后的多种命名；
    #: 一律可选读取，缺失即降级为在途，绝不臆造成交）。
    _ORDER_REF_FIELDS: Tuple[str, ...] = (
        "cl_ord_id", "order_ref", "order_id", "entrust_id", "ref",
    )
    #: 委托回查行里可用于与本单匹配的标识字段（remark=桥回写的 oid 前 20 位）
    _ROW_REF_FIELDS: Tuple[str, ...] = (
        "cl_ord_id", "order_ref", "order_id", "entrust_id", "ref", "remark",
    )
    #: 明确「全部成交」的状态文案（数字状态码不猜——未知即按在途，由 /deals 回填）
    _FILLED_STATUS_VALUES = frozenset(
        ("filled", "filled_all", "all_filled", "已成", "全部成交", "已成交")
    )
    _STATUS_FIELDS: Tuple[str, ...] = ("status", "order_status", "state")
    _BROKER_ORDER_ID_FIELDS: Tuple[str, ...] = ("cl_ord_id", "order_ref")
    _FILLED_VOLUME_FIELDS: Tuple[str, ...] = (
        "filled_volume", "volume_traded", "filled_qty", "deal_volume",
    )

    #: 业务校验类拒单文案（小写匹配）：柜台明确不受理这张单，判 rejected 才是真语义。
    #: 优先级高于瞬时故障模式——"order_volume raised: 委托数量非法" 这类 exception
    #: 文案的根因是业务参数，不是连接层。只收录确定性的校验措辞，绝不加宽泛词。
    _REJECT_ERROR_HINTS: Tuple[str, ...] = (
        "涨跌停", "超过涨跌幅", "可用不足", "资金不足", "可用资金",
        "可用数量", "可用股份", "数量非法", "委托数量", "价格非法",
        "非交易时间", "不在交易时间", "停牌", "重复委托",
    )
    #: 连接层/桥内调用瞬时故障文案（小写包含匹配）：gm 交易服务断连、桥内查询失败、
    #: 网络超时等。gm 侧"断连窗口"实测只产出 "order_volume raised:" /
    #: "returned empty list" 两种文案，其余为底层 socket 错误。
    _TRANSIENT_ERROR_HINTS: Tuple[str, ...] = (
        "order_volume raised:",
        "returned empty list",
        "winerror",
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "10061",
        "10054",
        "远程主机强迫关闭",
    )
    #: 桥内查询函数前缀（"query_positions: ..." 等）：查询失败属桥侧瞬时不可用
    _TRANSIENT_ERROR_PREFIXES: Tuple[str, ...] = ("query_",)
    #: 「瞬时故障转在途」告警的同类节流窗口（秒）
    _transient_warn_interval: float = 60.0
    #: 可下发给 QMT 桥的委托价格类型（桥侧映射 prType：opponent=14 对手价、
    #: limit=11 限价、market_best5=42/47 五档即时成交剩余撤销，按代码后缀取）。
    _PRICE_TYPE_VALUES = frozenset(("opponent", "limit", "market_best5"))

    #: 柜台 m_strRemark 实测只回显 20 字符（给 21 字符只回 20）；桥侧
    #: ``remark = oid[:20]``，Core 也按 20 位认领——**前 20 位就是认领键，必须唯一**。
    #: **不改 ID 格式**：完整 oid 仍是 MD5 32 位（DB 存全量、按全量幂等），只护栏派生。
    REMARK_MAX_LEN: int = 20
    #: 认领键护栏的内存窗口（超出按插入序淘汰最旧；正常会话远达不到该量级）。
    _CLAIM_KEY_CAP: int = 10000

    #: 桥读/写/池超时默认（秒）：掘金桥实测 /ping 纯内存应答也要 5~8s（gm SDK
    #: 主线程长持 GIL），旧 10s 贴太近会偶发误判断链。可被构造参数或
    #: config.live.bridge_read_timeout_seconds 覆盖；通道级 request_timeout_seconds
    #: （live_service 传入的 timeout）优先级最高。
    DEFAULT_READ_TIMEOUT: float = 15.0
    #: 建连超时默认（秒）：桥只在本机 loopback，建连正常毫秒级，不必等满读超时。
    DEFAULT_CONNECT_TIMEOUT: float = 3.0

    def __init__(self, base_url: str = "http://127.0.0.1:8790",
                 timeout: Optional[float] = None,
                 connect_timeout: Optional[float] = None,
                 slow_threshold: float = 15.0,
                 slow_warn_interval: float = 300.0,
                 on_slow: Optional[Callable[..., None]] = None,
                 now_fn: Optional[Callable[[], float]] = None,
                 client: Optional[httpx.Client] = None,
                 channel: Optional[str] = None,
                 price_type: Optional[str] = None):
        # channel 可按实例覆盖（掘金桥实例上报 goldminer_bridge，供引擎识别/
        # 错误文案分流；缺省保持 iquant_bridge 兼容旧用法）
        if channel:
            self.channel = channel
        self._base_url = base_url.rstrip("/")
        # 超时按用途拆分：读/写/池走 read 超时（默认 15s），建连走 connect（默认
        # 3s，且不超过 read）。所有端点（/order /orders /deals /positions
        # /account /price_limit /cancel /quote）共用同一 client，一处生效。
        self.timeout = _resolve_timeout(
            timeout, "bridge_read_timeout_seconds", self.DEFAULT_READ_TIMEOUT)
        self.connect_timeout = min(
            _resolve_timeout(connect_timeout, "bridge_connect_timeout_seconds",
                             self.DEFAULT_CONNECT_TIMEOUT),
            self.timeout,
        )
        # 外部注入 client（测试 MockTransport 等）保持其自身配置，不覆盖。
        self._client = client or httpx.Client(
            base_url=self._base_url,
            timeout=httpx.Timeout(
                connect=self.connect_timeout,
                read=self.timeout,
                write=self.timeout,
                pool=self.timeout,
            ),
        )
        # 最近一次 heartbeat 失败分类：refused=端口无监听 / timeout=已监听但无响应
        # / protocol=应答格式不符 / unreachable=其它网络错误。启动闸据此分级提示。
        self.last_probe_error: Optional[str] = None
        # 最近一次成功 /ping 的应答体（含 account/account_type），C2 账户键用。
        self.last_ping: Optional[dict] = None
        # 慢响应阈值（秒，<=0 关闭）与告警窗口（秒）：桥内查询排队时单次调用会
        # 排到数秒~十数秒，超阈值即告警；节流按会话（本实例）维度——每窗口最多
        # 一条，窗口内后续慢请求只累计计数与最慢值，窗口过期后的首条带上汇总。
        self._slow_threshold = slow_threshold
        self._slow_warn_interval = slow_warn_interval
        self._slow_window_at: Optional[float] = None
        self._slow_window_count = 0
        self._slow_window_max: Optional[Tuple[str, float]] = None
        self._now = now_fn or time.monotonic
        # 慢响应回调（引擎注入 → 转 risk/bridge_slow 事件）
        self.on_slow = on_slow
        # 最近一次慢响应 (endpoint, elapsed)；供只读排查/测试
        self.last_slow: Optional[Tuple[str, float]] = None
        # 「下单已发出未确认」线索：引擎按此保持 submitted 待 /orders 回查认领
        # （与 TdxTqDispatcher.last_unconfirmed_order_id 同一语义）。
        self.last_unconfirmed_order_id: Optional[str] = None
        self._last_error: Optional[str] = None
        # 最近一次 /order 受理应答里的柜台委托号（掘金 cl_ord_id）。引擎受理即落库
        # （live_orders.order_ref，重启不丢，作为精确认领键）——DB 是记帐真相，桥的
        # 内存台账重启即丢。与确定性 bridge_order_id（remark 认领键）分开保存，
        # 不得互相覆盖（bridge_order_id[:20] 是写进柜台的 remark）。
        self.last_broker_order_id: Optional[str] = None
        # 最近一次 /order 业务拒单原文（ok:false 的 error/reason），供引擎直接落库回显
        self.last_order_rejection: Optional[str] = None
        # 最近一次 /order 应答回显的「桥实际生效的委托价类型」（limit/opponent/
        # market_best5；桥未回显 = None）。盘前竞价据此核对桥是否把 limit 回退成
        # 对手价——回退意味着盘前算出的限价被丢弃，必须拦截而不是静默照报。
        self.last_order_price_type: Optional[str] = None
        # 「瞬时故障转在途」告警节流水位：断连窗口内可能连续多单命中，同类告警
        # 每 _transient_warn_interval 秒只发一条（每单仍记 INFO 级别的 trail）。
        self._transient_warn_at: Optional[float] = None
        # 撤单能力：新版本桥有 POST /cancel；老桥回 404 时置 False，调用方据此
        # 按「通道不支持撤单」跳过（退回 180s 超时 expire 兜底，不刷失败告警）。
        self.supports_cancel: bool = True
        # 最近一次撤单结局（canceled / pending / not_found / rejected / failed）
        self.last_cancel_result: Optional[str] = None
        # 撤单返回 pending 时的有限复查（2026-11-10）：桥的 pending = 撤单请求已发出
        # 但 ~0.9s 内未在 ORDER 表观测到终态，**不等于已撤**。复查 = 再发一次 POST
        # /cancel（桥侧幂等，会重新读委托表确认），直到 canceled（真终态）或明确
        # 失败；复查仍是 pending → 记「撤单未确认」，由调用方保留在途 + 超时兜底。
        # 默认 2 次（1 次复查，最坏多等 ~2s/单；收盘清扫/多单撤单不会被拖太久），
        # 仍未确认的由 cancel_unfilled_orders 下一轮（60s 节拍）继续重试。
        # 间隔/次数可被调用方或测试覆盖。
        self._cancel_recheck_attempts: int = 2
        self._cancel_recheck_interval: float = 1.0
        # 委托价格类型（None = 按通道解析）：仅 iquant_bridge 通道向桥下发该字段，
        # 缺省 opponent（= 桥侧 prType 14 对手价，与改动前逐字一致）。掘金桥自身
        # 已走限价，payload 不发该字段。显式参数 > config.iquant_bridge.price_type。
        self._price_type_override = (price_type or "").strip().lower() or None
        self._price_type_resolved: Optional[str] = None
        self._price_type_loaded = False
        # 认领键护栏：remark（oid 前 REMARK_MAX_LEN 位）-> 完整 oid。同键不同 oid
        # 意味着柜台 remark 会把两笔单认成同一笔，必须拒绝下单而不是静默放过。
        self._claim_keys: Dict[str, str] = {}

    @property
    def last_unconfirmed_message(self) -> Optional[str]:
        """最近一次「在途未确认」的可读原因（引擎回写 LiveOrder.error_message）。"""
        return self._last_error

    @property
    def last_error(self) -> Optional[str]:
        """最近一次失败原因（撤单等；daily_closer 等调用方只读回显）。"""
        return self._last_error

    # ---------------- 请求计时 ----------------
    def _do_request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """统一请求入口：计时 → 慢响应告警（会话维度窗口节流 + 计数聚合）。"""
        start = self._now()
        try:
            return self._client.request(method, self._base_url + path, **kwargs)
        finally:
            self._note_timing(path, self._now() - start)

    def _note_timing(self, path: str, elapsed: float) -> None:
        """慢响应告警：按「会话（本 dispatcher 实例）」维度做窗口节流。

        - 窗口内首条慢请求**立即**上报（不被节流吞掉）；
        - 同一窗口内后续慢请求只累计次数与最慢值，不再发事件（不同端点也合并）；
        - 窗口过期后的首条慢请求携带上一窗口汇总
          ``{count, slowest_endpoint, slowest_elapsed_seconds, window_seconds}``。
        """
        endpoint = path.split("?", 1)[0]
        if self._slow_threshold <= 0 or elapsed < self._slow_threshold:
            return
        self.last_slow = (endpoint, elapsed)
        now = self._now()
        if (self._slow_window_at is not None
                and now - self._slow_window_at < self._slow_warn_interval):
            self._slow_window_count += 1
            if self._slow_window_max is None or elapsed > self._slow_window_max[1]:
                self._slow_window_max = (endpoint, elapsed)
            logger.debug(
                "bridge slow response (aggregated in window): endpoint=%s "
                "elapsed=%.2fs window_count=%d",
                endpoint, elapsed, self._slow_window_count,
            )
            return

        summary = None
        if self._slow_window_at is not None and self._slow_window_max is not None:
            summary = {
                "count": self._slow_window_count,
                "slowest_endpoint": self._slow_window_max[0],
                "slowest_elapsed_seconds": round(float(self._slow_window_max[1]), 2),
                "window_seconds": round(float(self._slow_warn_interval), 1),
            }
        self._slow_window_at = now
        # 计数只统计「窗口内被抑制、尚未上报」的慢请求（首条已单独上报）
        self._slow_window_count = 0
        self._slow_window_max = (endpoint, elapsed)
        logger.warning(
            "bridge slow response: endpoint=%s elapsed=%.2fs threshold=%.2fs"
            " window_count=%d%s — 桥内查询排队（全局锁串行），下单可能超时转在途",
            endpoint, elapsed, self._slow_threshold, self._slow_window_count,
            (" prev_window=%s" % (summary,)) if summary else "",
        )
        if self.on_slow is not None:
            try:
                self.on_slow(endpoint, elapsed, summary)
            except Exception:  # noqa: BLE001
                logger.warning("bridge on_slow callback failed", exc_info=True)

    # ---------------- 基础 ----------------
    def heartbeat(self) -> bool:
        """桥在线探测（GET /ping）；失败时记录 last_probe_error 供启动闸分级提示。

        refused=连接被拒（端口无监听）；timeout=端口可连但无响应（常见于桥进程
        卡在阻塞查询）；protocol=有 HTTP 应答但不是桥协议（状态码非 200 / 非 JSON
        / ok=false）；unreachable=其它网络错误。
        """
        self.last_probe_error = None
        try:
            r = self._do_request("GET", "/ping")
        except httpx.TimeoutException:
            self.last_probe_error = "timeout"
            return False
        except httpx.ConnectError:
            self.last_probe_error = "refused"
            return False
        except Exception:
            self.last_probe_error = "unreachable"
            return False
        if r.status_code != 200:
            self.last_probe_error = "protocol"
            return False
        try:
            body = r.json()
        except Exception:
            self.last_probe_error = "protocol"
            return False
        if not isinstance(body, dict) or body.get("ok") is False:
            self.last_probe_error = "protocol"
            return False
        # C2 账户键独占（2026-11-16）：桥 /ping 自述 account/account_type，
        # 缓存最近一次成功应答，供 resolve_account_key 取账户身份键。
        self.last_ping = body
        return True

    # ---------------- 下单 ----------------
    @staticmethod
    def order_id(order: OrderEvent) -> str:
        """确定性订单 ID：同订单（含数量/价格）重试生成相同 ID，桥侧幂等去重。

        数量/价格必须参与：同策略/股票/信号但数量变化的重新触发是不同订单。
        返回 MD5 32 位十六进制（DB 全量存、桥按全量去重）；写进柜台的认领键是
        其前 ``REMARK_MAX_LEN``（20）位，由 :meth:`remark_of` 派生并校验。
        """
        key = "%s|%s|%s|%s|%s|%s|%s|%s" % (
            order.strategy_id, order.portfolio_id, order.stock_code,
            order.trade_type.value, order.signal_type.value, order.signal_name,
            order.quantity, order.price,
        )
        if order.bar_time is not None:
            key += "|%s" % order.bar_time.isoformat()
        return hashlib.md5(key.encode("utf-8")).hexdigest()

    @classmethod
    def remark_of(cls, oid: str) -> str:
        """派生柜台 remark（认领键）：恒 ``REMARK_MAX_LEN`` 字符。

        柜台 ``m_strRemark`` 实测只回显 20 字符（给 21 字符只回 20），桥侧与 Core
        都按 20 位认领。护栏口径：
        - oid 为空或不足 20 → 抛 :class:`BridgeOrderIdError`，**绝不生成短键**
          （短键必然撞单认领错，且不会静默截断成短键）；
        - oid 超过 20 → 取前 20 位（既有约定，不改 ID 格式）；唯一性由
          :meth:`_reserve_claim_key` 在提交前校验。
        """
        if not oid or len(oid) < cls.REMARK_MAX_LEN:
            logger.error(
                "bridge order id too short for remark: %r (need >= %d chars)",
                oid, cls.REMARK_MAX_LEN,
            )
            raise BridgeOrderIdError(
                "订单ID护栏：确定性订单号长度 %d < 柜台备注上限 %d，"
                "拒绝下单（避免 remark 撞单认领错误）"
                % (len(oid or ""), cls.REMARK_MAX_LEN))
        return oid[:cls.REMARK_MAX_LEN]

    def _reserve_claim_key(self, claim: str, oid: str) -> None:
        """认领键唯一性护栏：同一 20 位 remark 不得指向两笔不同单。

        - 同一 oid 重复提交（引擎重试 / 桥按 order_id 去重）→ 放行；
        - 同 claim 但不同 oid → 记 ERROR 并抛 :class:`BridgeOrderIdError`
          **拒绝下单**。不重新生成：确定性 ID 重生成会破坏幂等（同单重试被当成
          新单）且让在途单认领失效；出现撞键说明生成规则退化，必须人工修规则。
        """
        prev = self._claim_keys.get(claim)
        if prev is not None and prev != oid:
            logger.error(
                "bridge remark collision: claim=%r existing oid=%s new oid=%s "
                "(first %d chars must be unique) — refuse order",
                claim, prev, oid, self.REMARK_MAX_LEN,
            )
            raise BridgeOrderIdError(
                "订单ID护栏：remark %r 已归属另一笔订单（%s），新单 %s 拒绝下单"
                "（前 %d 位必须唯一，否则柜台认领会串单）"
                % (claim, prev, oid, self.REMARK_MAX_LEN))
        self._claim_keys[claim] = oid
        if len(self._claim_keys) > self._CLAIM_KEY_CAP:
            self._claim_keys.pop(next(iter(self._claim_keys)))

    def resolved_price_type(self) -> Optional[str]:
        """本通道 /order 实际下发的 price_type；None = 不发该字段（老行为）。

        解析顺序：构造参数显式值 > ``config.iquant_bridge.price_type`` > ``opponent``。
        仅 ``channel == "iquant_bridge"`` 才下发：掘金桥自身已走限价，TDX/THS 不走
        HTTP 桥，给它们塞字段只会让 payload 产生无意义变化。结果缓存，避免每单读 yaml。

        语义定位（不是通用滑点保护）：``limit`` 是把「价格由平台决定」这件事复制到
        QMT——TDX 早已按 ``PRICE_MY`` 下发并在提交前做 OCR 价格比对，掘金桥限价原样
        下发，同花顺现网 ``latest`` 模式根本不键入价格（±让价空转）。QMT 桥只有在
        ``limit`` 下才与它们同口径：审批价原样进 passorder，桥侧不做任何改写/跟价。
        """
        if self._price_type_loaded:
            return self._price_type_resolved
        self._price_type_loaded = True
        if self.channel != "iquant_bridge":
            return None
        raw = self._price_type_override
        if raw is None:
            try:
                cfg = load_config().get("iquant_bridge") or {}
                raw = str(cfg.get("price_type") or "").strip().lower() or None
            except Exception:  # noqa: BLE001 配置不可读 -> 退回缺省语义
                logger.warning("read iquant_bridge.price_type failed", exc_info=True)
                raw = None
        if raw is None:
            raw = "opponent"
        if raw not in self._PRICE_TYPE_VALUES:
            logger.warning(
                "未知 iquant_bridge.price_type=%r，回退 opponent（对手价）", raw)
            raw = "opponent"
        self._price_type_resolved = raw
        return raw

    def supports_price_type(self, price_type: str = "limit") -> Optional[bool]:
        """桥进程是否确认支持指定价（``price_type=<price_type>``）。

        判据只看 ``GET /ping`` 的自我声明，不额外探测 /order（探测单在未升级的桥
        上会变成真实委托）：

        1. 新桥若在 /ping 里给出 ``price_types`` 列表（未来扩展），直接查表；
        2. 否则看 ``endpoints`` 是否含 ``/price_limit``：该端点与 ``price_type``
           同一批引入（未升级版桥的 endpoints 里没有它），可视为「已加载支持
           price_type 的桥代码」的指纹。

        返回 ``True`` = 确认支持；``False`` = 桥自述里明确没有该能力（未升级）；
        ``None`` = 判不出（桥不可达 / 应答没有 endpoints / 非桥协议）——调用方
        必须按「不支持」fail-closed 处理，绝不静默退回对手价。
        """
        wanted = str(price_type or "").strip().lower()
        try:
            r = self._do_request("GET", "/ping")
        except Exception:  # noqa: BLE001 探测失败 -> 判不出
            return None
        if r.status_code != 200:
            return None
        try:
            body = r.json()
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(body, dict) or body.get("ok") is False:
            return None
        declared = body.get("price_types")
        if isinstance(declared, list):
            names = [str(x).strip().lower() for x in declared]
            return wanted in names
        endpoints = body.get("endpoints")
        if not isinstance(endpoints, list):
            return None
        return "/price_limit" in [str(e).strip() for e in endpoints]

    def place_order(self, order: OrderEvent,
                    price_type: Optional[str] = None) -> Optional[TradeEvent]:
        """下单。price_type 非空时按单覆盖配置（盘前集合竞价传 "limit"）。"""
        oid = self.order_id(order)
        self.last_unconfirmed_order_id = None
        self._last_error = None
        self.last_broker_order_id = None
        self.last_order_rejection = None
        self.last_order_price_type = None
        # 认领键护栏：派生 20 位 remark 并做在途唯一性校验（不足 20 位或撞键
        # → BridgeOrderIdError 拒绝下单，绝不静默截断/放过）。保持既有 ID 格式。
        claim = self.remark_of(oid)
        self._reserve_claim_key(claim, oid)
        payload = {
            "order_id": oid,
            "code": order.stock_code,
            "op": "buy" if order.trade_type.value == "BUY" else "sell",
            "volume": order.quantity,
            "price": float(order.price) if order.price is not None else 0,
            # pr_type=14 是桥的历史缺省（对手价：BUY 取卖1 / SELL 取买1，price 被
            # 忽略）。保留该字段只为兼容未升级的老桥——新桥以 price_type 为准。
            # price_type=limit 时 price（=审批价）由桥原样下发，桥内不改写/不跟价。
            "pr_type": 14,
            # remark（= oid 前 20 位，由 remark_of 统一派生）:桥作为 passorder 的
            # userOrderId 写入委托/成交的 m_strRemark，回填时 Core 按 remark 精确
            # 认领本单（见 LiveEngine._try_match_order_ref），避免 代码+方向+数量
            # 模糊匹配撞到跨会话遗留的同代码同向同量旧单。
            "remark": claim,
        }
        # 按单覆盖优先（盘前单显式 limit：prType=11、审批价原样下发），否则用
        # 配置解析值（盘中默认 opponent→prType=14，与既有行为一致）。
        effective_price_type = price_type or self.resolved_price_type()
        if effective_price_type:
            payload["price_type"] = effective_price_type
        try:
            r = self._do_request("POST", "/order", json=payload)
        except self._INFLIGHT_ERRORS as e:
            # 请求已发出但未拿到应答（桥全局锁排队导致的读超时/连接中断）：
            # 按「在途未确认」处理，绝不判拒单——否则真实委托脱跟踪（无 ref、
            # 无成交回填、无法撤），虚拟持仓/现金与柜台脱节。引擎据
            # last_unconfirmed_order_id 保持 submitted，后续 /orders 回查认领。
            self.last_unconfirmed_order_id = oid
            self._last_error = (
                "下单请求已发出但未获响应（%s: %s），已按在途处理，"
                "等待 /orders 回查认领" % (type(e).__name__, e)
            )
            logger.warning(
                "bridge /order no response (%s: %s) — keep order in-flight: "
                "oid=%s %s %s qty=%s",
                type(e).__name__, e, oid,
                order.trade_type.value, order.stock_code, order.quantity,
            )
            return None
        except Exception as e:
            # 连接被拒等「请求从未发出」的故障：仍是桥不可用（上层暂停交易）。
            raise BridgeUnavailableError("bridge request failed: %s" % e) from e
        if r.status_code != 200:
            raise BridgeUnavailableError("bridge returned HTTP %s" % r.status_code)
        try:
            data = r.json()
        except Exception as e:
            # #24：桥返回非 JSON（HTML 错误页/坏网关）不静默吞——原行为 data={} →
            # 当业务拒绝，真实桥故障无痕迹。告警让"非 JSON"可见。
            logger.warning(
                "bridge /order returned non-JSON body, treated as rejection: %s", e
            )
            data = {}
        # 桥实际生效的委托价类型（新桥每笔应答都回显；老桥没有该字段 -> None）。
        # 盘前竞价据此发现「请求 limit 但桥回退成 opponent」——价格已不在平台手里。
        echo = str(data.get("price_type") or "").strip().lower()
        self.last_order_price_type = echo or None
        if not data.get("ok"):
            # 桥业务拒绝（白名单/限额/重复/废单）→ 抛 BridgeOrderRejected，上层回显真实原因。
            # 拒单原文可能在 error / reason / rejected=true 的 message 上（掘金桥实测
            # 柜台废单走 rejected+reason）。data={}（非 JSON 路径）维持返回 None（#24 语义：
            # 桥故障非拒单）——不得把桥故障臆造成拒单。
            #
            # 但 ok:false 不等于「柜台明确拒单」：掘金桥在 gm 交易服务断连窗口内会回
            # {"ok": false, "error": "order_volume raised: ..." / "returned empty list"}，
            # 这是连接层瞬时故障。这类必须返回 None（在途）——引擎按
            # last_unconfirmed_order_id 保持 submitted，由 /orders 回查认领 + 180s 超窗
            # + grace 兜底；判 rejected 会让真实委托脱跟踪。
            error = data.get("error") or data.get("reason")
            if not error and data.get("rejected"):
                error = data.get("message") or "rejected"
            if error:
                if self._classify_bridge_error(str(error), data) == "transient":
                    return self._mark_transient_unconfirmed(oid, order, str(error))
                self.last_order_rejection = str(error)
                raise BridgeOrderRejected(str(error))
            return None

        # 受理应答里的柜台委托号（cl_ord_id）：引擎随即落库（精确认领键，桥重启不丢）
        self.last_broker_order_id = self._broker_order_id_from_response(data)

        # ok:true 不等于成交：掘金桥实测对柜台拒单（如价格超涨跌停）也回 ok:true，
        # 且拒单不回显到 /orders、/deals。只有「拿到可识别委托标识 + 回查委托表
        # 确认全部成交」才可构造成交回报，否则一律按在途待确认（返回 None）。
        ref = self._order_ref_from_response(data)
        if ref is None:
            return self._mark_unconfirmed(
                oid, order,
                "桥受理应答无可识别委托标识（%s），已按在途处理，等待委托回查认领"
                % self._short_body(data),
            )
        try:
            rows = self.query_orders(order_id=ref)
        except Exception as e:  # noqa: BLE001
            # 回查失败（含桥超时/离线）不得判拒单：保持 submitted 等后续回填
            return self._mark_unconfirmed(
                oid, order,
                "桥已受理（ref=%s）但委托回查失败（%s: %s），已按在途处理"
                % (ref, type(e).__name__, e),
            )
        row = self._find_status_row(rows, ref, oid)
        if row is None:
            return self._mark_unconfirmed(
                oid, order,
                "桥已受理（ref=%s）但委托表未回读到该单，已按在途处理"
                % ref,
            )
        if not self._row_filled(row, order):
            return self._mark_unconfirmed(
                oid, order,
                "桥已受理（ref=%s）但委托未确认全部成交（%s），已按在途处理"
                % (ref, self._short_body(row)),
            )

        # 已回读到该委托且确认全部成交：构造成交回报（成交价仍用请求价近似）
        fill_price = order.price if order.price is not None else Decimal("0")
        amount = fill_price * order.quantity
        return TradeEvent(
            strategy_id=order.strategy_id,
            portfolio_id=order.portfolio_id,
            stock_code=order.stock_code,
            trade_type=order.trade_type,
            price=fill_price,
            quantity=order.quantity,
            amount=amount,
            commission=Decimal("0"),
            stamp_duty=Decimal("0"),
            trade_time=order.bar_time or datetime.now(),
            signal_type=order.signal_type,
            signal_name=order.signal_name,
        )

    @classmethod
    def _classify_bridge_error(cls, error_text: Optional[str], payload) -> str:
        """把 /order 的 ok:false 应答分两类：``rejected``（明确拒单）/ ``transient``（瞬时故障）。

        纯函数（只读入参，不碰实例状态），便于表驱动单测。判定顺序与依据：

        1. **柜台明确拒单标记**：payload（或 payload.data）里 ``rejected is True``
           并有 reason/error —— 桥已确认柜台判废单，最高优先级，压过文案模式
           （避免把 "order_volume raised: 涨跌停..." 之类误判成连接故障）。
        2. **业务校验文案**：命中 ``_REJECT_ERROR_HINTS``（涨跌停/可用不足/数量非法…）
           → rejected。这类 exception 的根因是委托参数，不是连接层。
        3. **连接层/桥内瞬时故障**：``order_volume raised:`` / ``returned empty list``
           / ``query_*`` 前缀 / WinError 10061/10054、timed out、Connection refused
           → transient。gm 交易服务断连窗口实测只产出前两种文案。
        4. **未知错误 → rejected（保守）**：桥已经明确回了业务层错误文案（不是超时、
           不是无应答），把未知情况挂成在途会占住同名订单 10 分钟（expire/grace），
           期间既无法确认也无法重下，极易被误判为「已下单成功」。宁可判拒单让
           人工/后续信号重新决策，也不静默拖延。
        """
        text = str(error_text or "").strip()
        low = text.lower()
        buckets = [payload]
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            buckets.append(payload["data"])
        for bucket in buckets:
            if isinstance(bucket, dict) and bucket.get("rejected") is True:
                if bucket.get("reason") or text:
                    return "rejected"
        if any(h in text for h in cls._REJECT_ERROR_HINTS):
            return "rejected"
        if low.startswith(cls._TRANSIENT_ERROR_PREFIXES):
            return "transient"
        if any(h in low for h in cls._TRANSIENT_ERROR_HINTS):
            return "transient"
        return "rejected"

    def _mark_transient_unconfirmed(self, oid: str, order: OrderEvent,
                                    error_text: str) -> Optional[TradeEvent]:
        """桥返回 ok:false 但属连接层瞬时故障 → 记在途线索并返回 None（不判拒单）。

        语义与「请求已发出未拿到应答」完全一致：``last_unconfirmed_order_id`` 有值
        → 引擎保持 submitted（不落 rejected、不移出在途集合），后续 /orders 回查
        认领；超过 180s 超窗仍未认领由引擎 expire 兜底。
        """
        self.last_unconfirmed_order_id = oid
        self._last_error = (
            "桥下单返回瞬时故障（%s），已按在途处理，等待 /orders 回查认领"
            % error_text
        )
        logger.info(
            "bridge_order_transient: keep order in-flight: oid=%s %s %s qty=%s "
            "error=%s",
            oid, order.trade_type.value, order.stock_code, order.quantity,
            error_text,
        )
        now = self._now()
        if (self._transient_warn_at is None
                or now - self._transient_warn_at >= self._transient_warn_interval):
            self._transient_warn_at = now
            logger.warning(
                "bridge_order_transient: 桥瞬时故障（连接层）按在途处理，不判拒单: "
                "oid=%s %s %s qty=%s error=%s（同类告警 %.0fs 内只报一次）",
                oid, order.trade_type.value, order.stock_code, order.quantity,
                error_text, self._transient_warn_interval,
            )
        return None

    def _mark_unconfirmed(self, oid: str, order: OrderEvent,
                          message: str) -> Optional[TradeEvent]:
        """记「已受理未确认」线索并返回 None（引擎据此保持 submitted 等回填）。

        绝不在无法确认委托状态时返回 amount>0 的 TradeEvent——桥对柜台拒单
        也回 ok:true，臆造成交会把虚拟持仓/现金改错。
        """
        self.last_unconfirmed_order_id = oid
        self._last_error = message
        logger.warning(
            "bridge_order_unconfirmed: keep order in-flight: oid=%s %s %s qty=%s"
            " reason=%s",
            oid, order.trade_type.value, order.stock_code, order.quantity, message,
        )
        return None

    @staticmethod
    def _short_body(body) -> str:
        """日志/线索用应答片段（桥侧把结果截断为 ~120 字符，这里保持同量级）。"""
        try:
            return str(body)[:120]
        except Exception:  # noqa: BLE001
            return "<unprintable>"

    @classmethod
    def _order_ref_from_response(cls, data: dict) -> Optional[str]:
        """从 /order 应答里取可识别委托标识；取不到返回 None（调用方降级在途）。"""
        if not isinstance(data, dict):
            return None
        buckets = [data]
        nested = data.get("data")
        if isinstance(nested, dict):
            buckets.append(nested)
        for bucket in buckets:
            for key in cls._ORDER_REF_FIELDS:
                v = bucket.get(key)
                if v not in (None, ""):
                    return str(v)
        return None

    @classmethod
    def _broker_order_id_from_response(cls, data: dict) -> Optional[str]:
        """从 /order 受理应答里取柜台委托号（cl_ord_id，兼容 order_ref 命名）。

        与 `_order_ref_from_response`（泛化的「可识别标识」，含回显 order_id）不同：
        这里只要**柜台侧**委托号，用于落库精确认领；缺失返回 None（不影响在途判定）。
        """
        if not isinstance(data, dict):
            return None
        buckets = [data]
        nested = data.get("data")
        if isinstance(nested, dict):
            buckets.append(nested)
        for bucket in buckets:
            for key in cls._BROKER_ORDER_ID_FIELDS:
                v = bucket.get(key)
                if v not in (None, ""):
                    return str(v)
        return None

    @classmethod
    def _find_status_row(cls, rows, ref: str, oid: str) -> Optional[dict]:
        """在 /orders 回查结果里按委托标识/remark 定位本单行；找不到返回 None。"""
        if not isinstance(rows, list):
            return None
        expected_remark = oid[:20]
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in cls._ROW_REF_FIELDS:
                v = row.get(key)
                if v is None:
                    continue
                sv = str(v)
                if sv in (ref, oid) or (key == "remark" and sv == expected_remark):
                    return row
        return None

    @classmethod
    def _row_filled(cls, row: dict, order: OrderEvent) -> bool:
        """委托行是否确认「全部成交」：只认明确状态文案或成交量字段。

        数字状态码不猜（各桥/版本编号不一致，猜错会误判成交）——未知即按在途，
        成交与否交由 /deals 回填周期权威确认。
        """
        for key in cls._FILLED_VOLUME_FIELDS:
            v = row.get(key)
            if v is None:
                continue
            try:
                if float(v) >= float(order.quantity):
                    return True
            except (TypeError, ValueError):
                continue
        for key in cls._STATUS_FIELDS:
            v = row.get(key)
            if v is None:
                continue
            if str(v).strip().lower() in cls._FILLED_STATUS_VALUES:
                return True
        return False

    # ---------------- 查询 ----------------
    def check_price_limit(self, stock_code: str, direction: str, price) -> Optional[str]:
        """涨跌停前置校验：取桥的 GET /price_limit（前收 + 涨跌停价）本地判定。

        口径与 TDX/THS 通道一致：买 ≥ 涨停价 / 卖 ≤ 跌停价 → 返回拒绝原因文案，
        通过/跳过返回 None。桥同时给了交易所涨跌停价时直接用（ST/新股等特例最
        准）；只给前收时按 `_limit_ratio_for` 推出涨跌停价（前收取整到分）。

        **fail-open**（默认放行，校验故障绝不阻断下单）：
          - 桥离线 / 超时 / 非 200（含老版本桥无该端点的 404）
          - 应答 ok:false 或既无涨跌停价也无有效前收（<1 元，前复权早期失真）
          - 市价单（price 空/非正，成交价由交易所决定）
        """
        if price is None:
            return None
        try:
            order_price = Decimal(str(price))
        except (TypeError, ValueError, InvalidOperation):
            return None
        if order_price <= 0:
            return None
        try:
            # 用通道解析后的读超时（与 /orders /deals 同一口径）：掘金桥首字节实测
            # 8~13s，旧的硬编码 3.0s 必然超时，会让涨跌停校验事实上常闭并误报桥异常。
            r = self._do_request(
                "GET", "/price_limit?code=%s" % stock_code, timeout=self.timeout
            )
            if r.status_code != 200:
                # 老桥没有 /price_limit（404）属预期，按「无数据」放行，不打 warning
                logger.debug(
                    "bridge /price_limit unavailable (HTTP %s) for %s, skip check",
                    r.status_code, stock_code,
                )
                return None
            body = r.json() or {}
            if not isinstance(body, dict) or not body.get("ok"):
                return None
            data = body.get("data") or {}
            if not isinstance(data, dict):
                return None
            prev_close = _positive_float(data.get("pre_close"))
            limit_up = _positive_float(data.get("up_limit"))
            limit_down = _positive_float(data.get("down_limit"))
            cent = Decimal("0.01")
            ratio = _limit_ratio_for(stock_code)
            if limit_up is None or limit_down is None:
                if prev_close is None or prev_close < 1:
                    return None
                pc = Decimal(str(prev_close))
                if limit_up is None:
                    limit_up = float(
                        (pc * (1 + ratio)).quantize(cent, rounding=ROUND_HALF_UP)
                    )
                if limit_down is None:
                    limit_down = float(
                        (pc * (1 - ratio)).quantize(cent, rounding=ROUND_HALF_UP)
                    )
            base = ("前收 %s" % prev_close) if prev_close is not None else "桥内涨跌停价"
            if direction == "buy" and order_price >= Decimal(str(limit_up)):
                return ("委托买价 %s ≥ 涨停价 %s（%s），已拦截"
                        % (price, limit_up, base))
            if direction == "sell" and order_price <= Decimal(str(limit_down)):
                return ("委托卖价 %s ≤ 跌停价 %s（%s），已拦截"
                        % (price, limit_down, base))
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning("涨跌停前置校验异常，跳过检查继续下单：%s", e)
            return None

    def price_limit_bounds(self, stock_code: str):
        """当日涨跌停价 ``(prev_close, up_limit, down_limit)``；无数据 → None。

        盘前集合竞价定价用（live_engine 的 pre_open_auction 路径），口径与
        ``check_price_limit`` 完全一致：桥给涨跌停价直接用，只给前收则按
        ``_limit_ratio_for`` 推出（前收取整到分）。**fail-open**：桥离线/超时/
        非 200（含老桥 404）/ok:false/无有效前收 → None，调用方按「不夹价」处理。
        """
        try:
            # 同上：用通道读超时，不用硬编码 3.0s（掘金桥首字节 8~13s）。
            r = self._do_request(
                "GET", "/price_limit?code=%s" % stock_code, timeout=self.timeout
            )
            if r.status_code != 200:
                logger.debug(
                    "bridge /price_limit unavailable (HTTP %s) for %s, no bounds",
                    r.status_code, stock_code,
                )
                return None
            body = r.json() or {}
            if not isinstance(body, dict) or not body.get("ok"):
                return None
            data = body.get("data") or {}
            if not isinstance(data, dict):
                return None
            prev_close = _positive_float(data.get("pre_close"))
            limit_up = _positive_float(data.get("up_limit"))
            limit_down = _positive_float(data.get("down_limit"))
            if limit_up is None or limit_down is None:
                if prev_close is None or prev_close < 1:
                    return None
                pc = Decimal(str(prev_close))
                cent = Decimal("0.01")
                ratio = _limit_ratio_for(stock_code)
                if limit_up is None:
                    limit_up = float(
                        (pc * (1 + ratio)).quantize(cent, rounding=ROUND_HALF_UP)
                    )
                if limit_down is None:
                    limit_down = float(
                        (pc * (1 - ratio)).quantize(cent, rounding=ROUND_HALF_UP)
                    )
            return prev_close, limit_up, limit_down
        except Exception as e:  # noqa: BLE001
            logger.warning("涨跌停取价异常，按无数据放行：%s", e)
            return None

    def cancel_order(self, order_ref: str, stock_code: str = "") -> bool:
        """撤单（按柜台委托号撤单；QMT=order_ref / 掘金=cl_ord_id，同一字段值）。

        True  = 已确认撤销（result=canceled，或桥 ok:true 受理且无 result）；
        False = 明确未撤成功（不存在 / 已成交 / 柜台拒撤 / 桥不可用 / 老桥无端点），
                **或**桥只回 pending 且有限复查后仍未确认（``last_cancel_result``
                此时为 "pending"，调用方应记「撤单未确认」并保留在途，绝不当已撤）。

        pending 复查（2026-11-10）：桥的 pending 只表示撤单请求已在柜台发出、短窗口内
        未观测到终态，实际可能仍在撮合。直接当成功会让在途单一直占门且永不收口。
        这里按 ``_cancel_recheck_attempts`` × ``_cancel_recheck_interval`` 重新 POST
        /cancel（桥侧幂等，会重读 ORDER 表确认），一旦拿到 canceled 即真终态；仍是
        pending 就返回 False 并留 ``last_cancel_result="pending"`` 给调用方判「未确认」。

        老版本桥没有 POST /cancel（HTTP 404）→ 置 ``supports_cancel=False`` 并返回
        False 一次；后续调用方（order_machine / daily_closer / live_engine 停止撤单）
        据该标记按「通道不支持撤单」跳过，行为退回改前的 180s 超时 expire 兜底。
        """
        self._last_error = None
        self.last_cancel_result = None
        ref = str(order_ref or "").strip()
        if not ref:
            self._last_error = "撤单缺少委托编号（order_ref）"
            return False
        attempts = max(1, int(getattr(self, "_cancel_recheck_attempts", 3) or 1))
        for attempt in range(attempts):
            outcome = self._cancel_attempt(ref, stock_code)
            if outcome != "pending":
                return outcome in ("canceled", "accepted")
            if attempt + 1 < attempts:
                interval = float(getattr(self, "_cancel_recheck_interval", 1.0) or 0.0)
                if interval > 0:
                    time.sleep(interval)
                logger.info(
                    "bridge cancel pending, recheck %d/%d ref=%s",
                    attempt + 1, attempts, ref,
                )
        # 复查仍 pending：撤单未确认。绝不判已撤——订单由调用方保持 submitted，
        # 等委托表终态（53/54）或超时兜底收敛；工作台可见「撤单未确认」。
        self.last_cancel_result = "pending"
        self._last_error = (
            "撤单未确认：桥返回 pending，复查 %d 次仍未观察到终态，"
            "该单可能仍在柜台撮合（未撤销），请人工核对" % attempts
        )
        logger.warning(
            "bridge cancel unconfirmed after %d rechecks: ref=%s — keep order "
            "in-flight (NOT canceled)", attempts, ref,
        )
        return False

    def _cancel_attempt(self, ref: str, stock_code: str) -> str:
        """单次撤单请求，返回 canceled / accepted / pending / not_found / rejected / failed。

        accepted = 桥 ok:true 且未给 result（受理）；其余原样透传桥的 data.result，
        传输层问题（无响应/非 JSON/404/HTTP 非 200）统一归 failed。
        """
        payload = {"order_ref": ref, "cl_ord_id": ref, "code": stock_code or ""}
        try:
            r = self._do_request("POST", "/cancel", json=payload)
        except self._INFLIGHT_ERRORS as e:
            self._last_error = ("撤单请求已发出但未获响应（%s: %s）"
                                % (type(e).__name__, e))
            logger.warning(
                "bridge /cancel no response (%s: %s) ref=%s",
                type(e).__name__, e, ref,
            )
            return "failed"
        except Exception as e:  # noqa: BLE001
            self._last_error = "撤单请求失败：%s" % e
            logger.warning("bridge /cancel failed ref=%s: %s", ref, e)
            return "failed"
        if r.status_code == 404:
            self.supports_cancel = False
            self._last_error = "桥不支持撤单（端点 /cancel 不存在，老版本桥）"
            logger.warning(
                "bridge /cancel not available (HTTP 404) — 该通道按「不支持撤单」"
                "处理，超时在途单退回 180s expire 兜底（柜台可能仍挂单，需人工确认）"
            )
            return "failed"
        if r.status_code != 200:
            self._last_error = "撤单返回 HTTP %s" % r.status_code
            logger.warning("bridge cancel failed: HTTP %s ref=%s", r.status_code, ref)
            return "failed"
        try:
            body = r.json() or {}
        except Exception as e:  # noqa: BLE001
            self._last_error = "撤单返回非 JSON：%s" % e
            logger.warning("bridge /cancel non-JSON body ref=%s: %s", ref, e)
            return "failed"
        if not isinstance(body, dict):
            self._last_error = "撤单返回格式非对象：%s" % self._short_body(body)
            logger.warning("bridge /cancel bad body ref=%s: %s", ref, self._short_body(body))
            return "failed"
        data = body.get("data") if isinstance(body.get("data"), dict) else {}
        result = str(data.get("result") or "")
        self.last_cancel_result = result or None
        if result == "pending":
            # 不论 ok:true（桥对 pending 回 ok:true）：pending 不是终态，交调用方复查
            return "pending"
        if result in ("not_found", "rejected", "failed"):
            self._last_error = str(
                body.get("error") or body.get("reason")
                or "桥撤单失败（%s）" % result
            )
            logger.warning(
                "bridge cancel rejected: ref=%s result=%s error=%s",
                ref, result, self._last_error,
            )
            return result
        if body.get("ok") or result == "canceled":
            # 已处于撤销态（result=canceled 但 ok:false）按幂等成功处理
            self.supports_cancel = True
            self.last_cancel_result = result or "accepted"
            return "canceled" if result == "canceled" else "accepted"
        self._last_error = str(
            body.get("error") or body.get("reason")
            or "桥撤单失败（%s）" % (result or "unknown")
        )
        logger.warning(
            "bridge cancel rejected: ref=%s result=%s error=%s",
            ref, result or "-", self._last_error,
        )
        return "failed"

    def _get_json(self, path: str) -> list:
        try:
            r = self._do_request("GET", path)
        except Exception as e:
            raise BridgeUnavailableError("bridge request failed: %s" % e) from e
        if r.status_code != 200:
            raise BridgeUnavailableError("bridge returned HTTP %s" % r.status_code)
        try:
            return (r.json() or {}).get("data", [])
        except Exception as e:
            # #24：非 JSON 不静默吞——返回 [] 会被上层当无持仓/无订单，掩盖桥故障。
            logger.warning(
                "bridge %s returned non-JSON body, returning empty list: %s", path, e
            )
            return []

    def query_positions(self) -> list:
        return self._get_json("/positions")

    def get_asset(self) -> dict:
        """账户资金（快照采集用）：GET /account → 归一化资产字段。"""
        try:
            rows = self._get_json("/account")
            row = rows[0] if isinstance(rows, list) and rows else {}
        except Exception:
            return {}
        out = {}
        for src, dst in (("total_asset", "total_asset"), ("nav", "total_asset"),
                         ("available", "available"), ("market_value", "market_value"),
                         ("balance", "balance"), ("frozen", "frozen_cash")):
            v = row.get(src)
            if v is not None and dst not in out:
                out[dst] = float(v)
        return out

    def query_account(self) -> list:
        return self._get_json("/account")

    def query_orders(self, order_id: Optional[str] = None) -> list:
        path = "/orders"
        if order_id:
            path += "?order_id=%s" % order_id
        return self._get_json(path)

    def query_deals(self, order_id: Optional[str] = None) -> list:
        path = "/deals"
        if order_id:
            path += "?order_id=%s" % order_id
        return self._get_json(path)

    def query_calendar(self) -> list:
        """拉权威交易日历（桥 xtdata.get_trading_dates）。

        返回 'YYYYMMDD' 字符串列表（桥侧把毫秒时间戳转成字符串，兼容 3.6）。
        桥离线或老桥无 /calendar（HTTP 404）时抛 BridgeUnavailableError，
        由 TradingCalendar fail-open 处理（工作日默认交易日）。
        """
        return self._get_json("/calendar")

    def query_quote(self, code: str, period: str = "1m", count: int = 10) -> list:
        """拉 1m/5m/1d bar（0009 切片3 行情通道）。

        桥 GET /quote?code=&period=&count= 返回
        {"ok": True, "data": {code: [bar, ...]}}，每 bar 含 stime/open/high/low/close/volume。
        返回该 code 的 bar dict 列表（空数据 → 空列表）。

        复权口径（P2-9）：桥侧一律返回**不复权**原始价——掘金桥
        ``/quote`` 用 ``adjust=0``；QMT 桥 ``dividend_type="follow"`` 经
        ContextInfo 解析为 ``this.dividend_type``（初始化空串）= 默认不复权。
        当日分钟 bar 与实时价一致，下单/涨跌停/风控不受影响；但与 tdx/ths
        的日线前复权口径不同，1d/1w/1mon 的公式回看段在除权日附近可能有差异
        （只影响信号可比性，不影响下单）。桥支持按请求指定复权前，平台侧无法统一。
        """
        path = "/quote?code=%s&period=%s&count=%s" % (code, period, count)
        try:
            r = self._do_request("GET", path)
        except Exception as e:
            raise BridgeUnavailableError("bridge request failed: %s" % e) from e
        if r.status_code != 200:
            raise BridgeUnavailableError("bridge returned HTTP %s" % r.status_code)
        try:
            body = r.json() or {}
        except Exception as e:
            # #24：非 JSON 不静默吞——返回 [] 会让 BarPoller 拉不到 bar 静默不触发。
            logger.warning(
                "bridge /quote returned non-JSON body for %s, returning empty list: %s",
                code, e,
            )
            return []
        if not body.get("ok"):
            return []
        data = body.get("data") or {}
        return data.get(code, []) if isinstance(data, dict) else []
