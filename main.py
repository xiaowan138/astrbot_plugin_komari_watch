"""Komari Watch - an AstrBot plugin for status and proactive alerts."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from pydantic import BaseModel, Field

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Image
from astrbot.api.star import Context, Star, register

PLUGIN_ID = "astrbot_plugin_komari_watch"

_MSG_TYPES = aiohttp.WSMsgType

_ALERT_HISTORY_LIMIT = 50

# 默认按 0-1 小数上报、需要 ×100 的指标；与配置项 fraction_metrics 默认值一致。
DEFAULT_FRACTION_METRICS = frozenset({"cpu", "memory", "disk"})

# 查询时可用 group:xx / tag:xx 前缀按分组或标签筛选节点。
_SELECTOR_PREFIXES = {"group:": "group", "分组:": "group", "tag:": "tag", "标签:": "tag"}


class KomariWatchConfig(BaseModel):
    komari_url: str = Field("", description="Komari 服务器地址")
    komari_token: str = Field("", description="API Token 或 Session Token")
    image_output: bool = Field(True, description="以图片卡片发送状态报告")
    image_width: int = Field(900, ge=500, le=1600, description="状态图片宽度")
    poll_interval: int = Field(60, ge=15, le=3600)
    offline_grace_cycles: int = Field(2, ge=1, le=10)
    cpu_threshold: float = Field(90, ge=1, le=100)
    memory_threshold: float = Field(90, ge=1, le=100)
    disk_threshold: float = Field(90, ge=1, le=100)
    high_load_cycles: int = Field(2, ge=1, le=10)
    alert_cooldown: int = Field(1800, ge=0, le=86400)
    notify_recovery: bool = True
    request_timeout: int = Field(10, ge=3, le=60)
    filter_mode: str = Field("none", pattern="^(none|allow|deny)$", description="节点过滤：none/allow(仅监控)/deny(排除)")
    filter_nodes: str = Field("", description="要过滤的节点名，多个用英文逗号分隔")
    status_report_interval: int = Field(0, ge=0, le=720, description="定时状态推送间隔（小时），0 表示关闭")
    status_report_time: str = Field("", description="每天定时推送状态卡片的本地时刻（HH:MM，如 09:00），留空不启用")
    prune_missing_cycles: int = Field(5, ge=1, le=100, description="节点消失多少周期后清理其监控状态")
    panel_fail_cycles: int = Field(3, ge=0, le=10, description="面板连续失败多少个周期后推送不可达告警，0 表示关闭")
    notify_restart: bool = True
    long_offline_remind_hours: int = Field(0, ge=0, le=720, description="节点离线超过多少小时后每日提醒一次，0 表示关闭")
    fraction_metrics: str = Field("cpu,memory,disk", description="按 0-1 小数上报的指标（会×100），多个用英文逗号分隔，留空表示不换算")
    expire_remind_days: int = Field(3, ge=0, le=365, description="节点到期前多少天开始每日提醒，0 表示关闭")
    traffic_alert_percent: float = Field(0, ge=0, le=100, description="流量使用率超过该百分比时告警，0 表示关闭")


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: Any) -> Optional[datetime]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
    else:
        if not isinstance(value, str) or not value:
            return None
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
        except ValueError:
            try:
                ts = float(value)
            except ValueError:
                return None
    if ts > 1e12:
        ts /= 1000
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


def _metric(node: dict[str, Any], name: str, fractions: Optional[frozenset[str]] = None) -> Optional[float]:
    """读取 CPU/内存/磁盘/GPU 使用率（百分数）。

    部分面板把 0-1 的小数直接当百分数上报（0.5 表示 50%），另有面板上报的
    0.5 就是 0.5%。因此只有 `fractions` 里显式列出的指标才做 ×100 换算，
    默认与配置项 `fraction_metrics` 保持一致，避免把真实低负载放大 100 倍。
    """
    if fractions is None:
        fractions = DEFAULT_FRACTION_METRICS
    aliases = {
        "cpu": ("cpu_usage", "cpu_percent", "cpuUsage", "cpu_used_percent", "usage"),
        "memory": ("memory_usage", "memory_percent", "memory_usage_percent", "ram_usage", "ram_percent", "mem_usage", "mem_percent"),
        "disk": ("disk_usage", "disk_percent", "disk_usage_percent", "storage_percent"),
        "gpu": ("gpu_usage", "gpu_percent", "gpuUsage", "gpu_used_percent"),
    }

    def scale(value: float) -> float:
        return value * 100 if name in fractions and 0 <= value <= 1 else value

    for key in aliases[name]:
        value = _num(node.get(key))
        if value is not None:
            return scale(value)
    if name == "gpu":
        # 记录接口里 gpu 是数值百分比；部分面板则在 gpu 对象里给出使用率。
        value = _num(node.get("gpu"))
        if value is not None:
            return scale(value)
    containers = {
        "cpu": (node.get("cpu"),),
        "memory": (node.get("ram"), node.get("memory"), node.get("mem")),
        "disk": (node.get("disk"), node.get("storage")),
        "gpu": (node.get("gpu"), node.get("gpus")),
    }
    for nested in containers[name]:
        if not isinstance(nested, dict):
            continue
        value = _num(nested.get("usage", nested.get("percent", nested.get("used_percent", nested.get("percentage")))))
        if value is not None:
            return scale(value)
        used, total = _num(nested.get("used")), _num(nested.get("total"))
        if used is not None and total and total > 0:
            return used / total * 100
    pairs = {
        "memory": (("mem_used", "mem_total"), ("memory_used", "memory_total"), ("ram_used", "ram_total")),
        "disk": (("disk_used", "disk_total"), ("storage_used", "storage_total")),
    }
    for used_key, total_key in pairs.get(name, ()):
        used, total = _num(node.get(used_key)), _num(node.get(total_key))
        if used is not None and total and total > 0:
            return used / total * 100
    return None


@register(PLUGIN_ID, "xiaowan", "Komari 监控推送插件", "1.6.0", "https://github.com/xiaowan138/astrbot_plugin_komari_watch")
class KomariWatchPlugin(Star):
    """Komari queries plus stateful offline/high-load notifications."""

    def __init__(self, context: Context, config: KomariWatchConfig | None = None):
        super().__init__(context)
        self.config = config or KomariWatchConfig()
        self.logger = logging.getLogger(PLUGIN_ID)
        self.state_dir = Path("data") / "plugin_data" / PLUGIN_ID
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "state.json"
        self.state = self._load_state()
        legacy_mute = _num(self.state.pop("muted_until", None))
        if legacy_mute is not None and legacy_mute > time.time():
            # 旧版全局静默字段迁移为按会话静默。
            self.state.setdefault("muted", {}).update({target: legacy_mute for target in self._targets()})
        self._stop = asyncio.Event()
        self._check_lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None
        self._failure_count = 0
        self._filter_warned = False
        self._time_report_warned = False
        self._fractions = self._parse_fractions(self.config.fraction_metrics)
        try:
            self._monitor_task = asyncio.get_running_loop().create_task(self._monitor_loop())
        except RuntimeError:
            self.logger.debug("No running event loop; monitor starts on first command")

    @staticmethod
    def _parse_fractions(spec: str) -> frozenset[str]:
        """解析 fraction_metrics 配置：英文逗号分隔的指标名，留空表示不做 0-1 换算。"""
        return frozenset(token.strip().lower() for token in (spec or "").split(",") if token.strip())

    def _load_state(self) -> dict[str, Any]:
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _save_state(self) -> None:
        try:
            self.state_file.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            self.logger.warning("保存监控状态失败: %s", exc)

    def _targets(self) -> list[str]:
        return [str(item) for item in self.state.get("targets", []) if item]

    def _headers(self) -> dict[str, str]:
        if not self.config.komari_token:
            return {}
        return {"Authorization": f"Bearer {self.config.komari_token}", "Cookie": f"session_token={self.config.komari_token}"}

    async def _get_session(self) -> aiohttp.ClientSession:
        # 方法名不能叫 _session：实例属性 self._session 会遮蔽同名方法，
        # 调用时抛 TypeError 并被当作连接失败吞掉（1.1.0 引入、本版修复的严重问题）。
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)
                self._session = aiohttp.ClientSession(timeout=timeout, headers=self._headers())
            return self._session

    async def _get_json(self, endpoint: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        if not self.config.komari_url:
            return None, "请先在插件配置中填写 Komari 服务器地址。"
        try:
            session = await self._get_session()
            async with session.get(self.config.komari_url.rstrip("/") + endpoint) as response:
                if response.status != 200:
                    return None, f"Komari API 返回 HTTP {response.status}"
                payload = await response.json(content_type=None)
                return payload if isinstance(payload, dict) else None, None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            return None, f"连接 Komari 失败：{exc}"

    async def _nodes(self) -> tuple[list[dict[str, Any]], Optional[str]]:
        payload, error = await self._get_json("/api/nodes")
        if error:
            return [], error
        raw: Any = payload.get("data", []) if payload else []
        if isinstance(raw, dict):
            raw = raw.get("nodes", raw.get("servers", list(raw.values())))
        return ([item for item in raw if isinstance(item, dict)], None) if isinstance(raw, list) else ([], None)

    def _ws_url(self) -> str:
        base = self.config.komari_url.rstrip("/")
        if base.lower().startswith("https://"):
            return "wss://" + base.split("://", 1)[1] + "/api/clients"
        if base.lower().startswith("http://"):
            return "ws://" + base.split("://", 1)[1] + "/api/clients"
        return base + "/api/clients"

    @staticmethod
    def _ws_bytes(data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, bytearray):
            return bytes(data)
        return str(data).encode("utf-8", "replace")

    async def _read_ws_payload(self, ws: aiohttp.ClientWebSocketResponse, timeout: float = 6.0) -> Optional[str]:
        """Read a single complete WS payload, tolerating binary frames, pings and
        (defensively) fragmented frames instead of stopping after a fixed count."""
        parts: list[bytes] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                message = await asyncio.wait_for(ws.receive(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                break
            if message.type == _MSG_TYPES.CONTINUATION:
                parts.append(self._ws_bytes(message.data))
                continue
            if message.type in (_MSG_TYPES.TEXT, _MSG_TYPES.BINARY):
                if isinstance(message.data, str):
                    text: Optional[str] = message.data
                else:
                    text = self._ws_bytes(message.data).decode("utf-8", "replace")
                if not text:
                    continue
                if parts:
                    text = "".join(p.decode("utf-8", "replace") for p in parts) + text
                return text
            if message.type in (_MSG_TYPES.CLOSED, _MSG_TYPES.CLOSE, _MSG_TYPES.ERROR):
                break
            # ignore PING / PONG
        return None

    @staticmethod
    def _parse_ws_clients(payload: Any) -> list[dict[str, Any]]:
        raw = payload.get("data", payload) if isinstance(payload, dict) else payload
        if isinstance(raw, dict) and isinstance(raw.get("data"), dict):
            details = raw["data"]
            online = raw.get("online", details.keys())
            result = []
            for key in online:
                value = details.get(key)
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        value = None
                if isinstance(value, dict):
                    result.append({**value, "uuid": key})
            return result
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        # Some older Komari builds return {uuid: metrics} directly.
        if isinstance(raw, dict):
            mapped = []
            for key, value in raw.items():
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        value = None
                if isinstance(value, dict) and any(field in value for field in ("cpu", "ram", "memory", "disk")):
                    mapped.append({**value, "uuid": key})
            return mapped
        return []

    @staticmethod
    def _is_clients_payload(payload: Any) -> bool:
        """判断 WS 帧是否是"客户端列表"数据帧。

        通道里还可能混有 ack/心跳等非数据帧（如 {"status":"ok"}）。此前任何
        能解析成 JSON 的帧都被当作通道可用，非数据帧会让客户端列表恒为空，
        进而把"所有节点在线"误判成"全部离线"。这里要求帧具备客户端容器的形状。
        """
        if isinstance(payload, list):
            return True
        if not isinstance(payload, dict):
            return False
        if "data" in payload or "online" in payload:
            return True
        # 兼容旧版直接返回 {uuid: 指标} 的扁平结构。
        return any(isinstance(value, (dict, list)) for value in payload.values())

    async def _realtime(self) -> tuple[list[dict[str, Any]], bool]:
        """返回 (在线客户端列表, WS 通道是否可用)。
        通道可用指至少成功解析出一帧客户端数据：此时客户端列表为空代表"所有节点离线"，
        不能误判为通道故障而退回历史时间戳兜底，否则会拖延离线告警。"""
        if not self.config.komari_url:
            return [], False
        try:
            session = await self._get_session()
            ws_timeout = aiohttp.ClientTimeout(total=min(self.config.request_timeout, 15))
            async with session.ws_connect(self._ws_url(), heartbeat=10, timeout=ws_timeout) as ws:
                await ws.send_str("get")
                # 首条消息可能是 ack/pong 等非数据帧，最多再读两条直到解析出客户端数据。
                for _ in range(3):
                    text = await self._read_ws_payload(ws)
                    if not text:
                        break
                    try:
                        payload = json.loads(text)
                    except ValueError:
                        continue
                    if not self._is_clients_payload(payload):
                        continue
                    return self._parse_ws_clients(payload), True
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, KeyError):
            return [], False
        return [], False

    async def _records(self, uuid: Any, hours: int) -> list[dict[str, Any]]:
        """读取一个节点的负载记录。

        部分面板（或反代后的旧版本）不接受 load_type 参数并直接报错，
        因此首次请求失败时去掉该参数重试一次，避免历史曲线与兜底实时数据整体失效。
        """
        query = f"uuid={quote(str(uuid))}&hours={hours}"
        payload, error = await self._get_json(f"/api/records/load?{query}&load_type=all")
        if error:
            self.logger.debug("带 load_type 读取 %s 历史记录失败（%s），改用不带该参数的请求重试", uuid, error)
            payload, error = await self._get_json(f"/api/records/load?{query}")
        if error or not payload:
            return []
        data = payload.get("data", {})
        records = data.get("records", []) if isinstance(data, dict) else []
        return [item for item in records if isinstance(item, dict)] if isinstance(records, list) else []

    async def _history_series(self, node: dict[str, Any], hours: int) -> list[dict[str, Any]]:
        uuid = node.get("uuid") or node.get("id")
        if not uuid:
            return []
        try:
            records = await self._records(uuid, hours)
        except Exception as exc:
            self.logger.debug("读取 %s 历史记录失败: %s", uuid, exc)
            return []
        output: list[dict[str, Any]] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            cpu = _num(item.get("cpu_percent", item.get("cpu")))
            ram = _num(item.get("ram_percent"))
            if ram is None:
                used, total = _num(item.get("ram")), _num(item.get("ram_total"))
                if used is not None and total and total > 0:
                    ram = used / total * 100
            disk = _num(item.get("disk_percent"))
            if disk is None:
                used, total = _num(item.get("disk")), _num(item.get("disk_total"))
                if used is not None and total and total > 0:
                    disk = used / total * 100
            output.append({"time": self._record_time(item), "cpu": cpu, "ram": ram, "disk": disk,
                           "gpu": _num(item.get("gpu_percent", item.get("gpu"))),
                           "net_in": _num(item.get("net_in")), "net_out": _num(item.get("net_out"))})
        # 按时间排序：API 返回乱序时曲线会来回折返。
        output.sort(key=lambda point: point["time"])
        return output

    async def _history_by_node(self, node: dict[str, Any], hours: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        series = await self._history_series(node, hours)
        return (node, series)

    @staticmethod
    def _record_time(item: dict[str, Any]) -> float:
        """记录时间排序键：兼容 unix 秒/毫秒时间戳与 ISO 字符串，无法解析返回 0。"""
        num = _num(item.get("time"))
        if num is not None:
            return num / 1000 if num > 1e12 else num
        parsed = _parse_time(item.get("time"))
        return parsed.timestamp() if parsed else 0.0

    async def _history_one(self, node: dict[str, Any]) -> Optional[dict[str, Any]]:
        uuid = node.get("uuid") or node.get("id")
        if not uuid:
            return None
        try:
            records = await self._records(uuid, 1)
        except Exception as exc:
            self.logger.debug("读取 %s 历史记录失败: %s", uuid, exc)
            return None
        if not records:
            return None
        latest = max(records, key=self._record_time, default=None)
        if not latest:
            return None
        item: dict[str, Any] = {"uuid": str(uuid), "updated_at": latest.get("time")}
        if latest.get("cpu") is not None:
            item["cpu_usage"] = latest["cpu"]
        ram_total = latest.get("ram_total") or node.get("mem_total") or node.get("memory_total")
        if latest.get("ram") is not None:
            item["ram"] = {"used": latest["ram"], "total": ram_total or 0}
        if latest.get("ram_percent") is not None:
            item["ram_usage"] = latest["ram_percent"]
        if latest.get("disk") is not None:
            item["disk"] = {"used": latest["disk"], "total": latest.get("disk_total") or node.get("disk_total") or 0}
        if latest.get("disk_percent") is not None:
            item["disk_usage"] = latest["disk_percent"]
        if latest.get("net_in") is not None or latest.get("net_out") is not None:
            item["network"] = {"down": latest.get("net_in", 0), "up": latest.get("net_out", 0)}
        # 累计流量计数器：用于状态卡片的"剩余量"与流量限额告警。
        for field in ("net_total_up", "net_total_down"):
            if latest.get(field) is not None:
                item[field] = latest[field]
        if latest.get("gpu") is not None:
            item["gpu_usage"] = latest["gpu"]
        item["load"] = {"load1": latest.get("load", "-")}
        return item

    async def _history_realtime(self, nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Fallback for panels where the client WebSocket is disabled by a proxy."""
        tasks = [self._history_one(node) for node in nodes if node.get("uuid") or node.get("id")]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [item for item in results if isinstance(item, dict)]

    async def _recent_record(self, node: dict[str, Any]) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        """读取单个节点的最近一条上报（/api/recent/:uuid）。"""
        uuid = node.get("uuid") or node.get("id")
        if not uuid:
            return None, "节点缺少 uuid，无法查询最近上报。"
        payload, error = await self._get_json(f"/api/recent/{quote(str(uuid))}")
        if error:
            return None, error
        data = payload.get("data", payload) if payload else None
        records = data
        if isinstance(data, dict):
            records = data.get("records", data.get("data", []))
        if not isinstance(records, list) or not records:
            return None, "面板未返回该节点的最近上报。"
        latest = max((item for item in records if isinstance(item, dict)), key=self._record_time, default=None)
        return (latest, None) if latest else (None, "面板未返回该节点的最近上报。")

    async def _ping_records(self, uuid: Any, hours: int) -> list[dict[str, Any]]:
        query = f"hours={hours}" + (f"&uuid={quote(str(uuid))}" if uuid else "")
        payload, error = await self._get_json(f"/api/records/ping?{query}")
        if error or not payload:
            return []
        data = payload.get("data", payload)
        records = data
        if isinstance(data, dict):
            records = data.get("records", data.get("data", []))
        return [item for item in records if isinstance(item, dict)] if isinstance(records, list) else []

    @staticmethod
    def _ping_stats(records: list[dict[str, Any]]) -> dict[str, list[float]]:
        """按 ping 任务聚合延迟样本；丢包记录的值小于 0，一并计入样本数。"""
        grouped: dict[str, list[float]] = {}
        for item in records:
            value = None
            for key in ("value", "latency", "ping", "delay"):
                value = _num(item.get(key))
                if value is not None:
                    break
            if value is None:
                continue
            task = item.get("task_id", item.get("task", "默认"))
            grouped.setdefault(str(task), []).append(value)
        return grouped

    @staticmethod
    def _ping_lines(grouped: dict[str, list[float]]) -> list[str]:
        lines: list[str] = []
        for task, values in grouped.items():
            total = len(values)
            ok = [value for value in values if value >= 0]
            loss = (total - len(ok)) / total * 100 if total else 0.0
            if ok:
                lines.append(f"· 任务 {task}：平均 {sum(ok) / len(ok):.1f}ms / 最低 {min(ok):.1f} / 最高 {max(ok):.1f}"
                             f" / 丢包 {loss:.1f}%（{total} 次）")
            else:
                lines.append(f"· 任务 {task}：全部丢包（{total} 次）")
        return lines

    @staticmethod
    def _merge_nodes(static: list[dict[str, Any]], live: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_key = {str(item.get(key)): item for item in static for key in ("id", "uuid") if item.get(key) is not None}
        merged, live_keys = [], set()
        for item in live:
            key = str(item.get("uuid") or item.get("id") or "")
            if key:
                live_keys.add(key)
            merged.append({**by_key.get(key, {}), **item})
        for item in static:
            key = str(item.get("uuid") or item.get("id") or "")
            if key not in live_keys:
                merged.append(dict(item))
        return merged

    def _is_online(self, node: dict[str, Any], live_keys: set[str], ws_live: bool) -> bool:
        """WS 通道可用时其在线列表是权威信号；仅当 WS 整体不可用、纯靠
        历史记录兜底时才校验记录时间新鲜度——否则稀疏的历史时间戳会
        否决 WS 刚上报的在线状态，制造假离线告警。"""
        if ws_live:
            return self._node_key(node) in live_keys
        updated = _parse_time(node.get("updated_at") or node.get("last_seen"))
        return bool(updated and (datetime.now(timezone.utc) - updated).total_seconds() < self.config.poll_interval * 3)

    def _format_report(self, nodes: list[dict[str, Any]]) -> str:
        lines = ["📡 Komari 服务器状态"]
        for node in nodes:
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            online = "在线" if node.get("is_online") else "离线"
            cpu, memory, disk = (_metric(node, "cpu", self._fractions), _metric(node, "memory", self._fractions),
                                 _metric(node, "disk", self._fractions))
            metrics = " / ".join(f"{label} {value:.1f}%" for value, label in ((cpu, "CPU"), (memory, "内存"), (disk, "磁盘")) if value is not None)
            lines.append(f"\n{'🟢' if online == '在线' else '🔴'} {name} · {online}{(' · ' + metrics) if metrics else ''}")
        return "\n".join(lines) if len(lines) > 1 else "Komari 没有返回节点。"

    @staticmethod
    def _reltime(value: Any) -> str:
        updated = _parse_time(value)
        if updated is None:
            return "等待心跳"
        delta = (datetime.now(timezone.utc) - updated).total_seconds()
        delta = max(0.0, delta)
        if delta < 60:
            return "刚刚"
        if delta < 3600:
            return f"{int(delta // 60)} 分钟前"
        if delta < 86400:
            return f"{int(delta // 3600)} 小时前"
        return f"{int(delta // 86400)} 天前"

    @staticmethod
    def _fmt_bytes(value: Any) -> str:
        amount = _num(value)
        if amount is None:
            return "-"
        units = ("B", "KB", "MB", "GB", "TB")
        index = 0
        while abs(amount) >= 1024 and index < len(units) - 1:
            amount /= 1024
            index += 1
        return f"{amount:.1f} {units[index]}"

    @staticmethod
    def _fmt_speed(value: Any) -> str:
        return f"{KomariWatchPlugin._fmt_bytes(value)}/s"

    @staticmethod
    def _fmt_uptime(value: Any) -> str:
        seconds = _num(value)
        if seconds is None:
            return "-"
        return KomariWatchPlugin._fmt_duration(seconds)

    @staticmethod
    def _fmt_duration(value: Any) -> str:
        seconds = _num(value)
        if seconds is None:
            return "-"
        seconds = max(0, int(seconds))
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes = remainder // 60
        parts = []
        if days:
            parts.append(f"{days}天")
        if hours:
            parts.append(f"{hours}时")
        parts.append(f"{minutes}分")
        return "".join(parts)

    # ---- 流量 / 到期 / 分组 ----

    @staticmethod
    def _traffic(node: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
        """返回 (已用流量, 流量限额)，单位字节。

        优先使用面板直接给出的 traffic_used；否则用记录里的累计计数器
        net_total_up/down 按 traffic_limit_type（sum/max/min/up/down）折算，
        与 Komari 面板自身的口径保持一致。取不到计数器时返回 (None, 限额)。
        """
        limit = _num(node.get("traffic_limit"))
        limit = limit if limit and limit > 0 else None
        used = _num(node.get("traffic_used"))
        if used is None:
            up = _num(node.get("net_total_up"))
            if up is None:
                up = _num(node.get("traffic_up"))
            down = _num(node.get("net_total_down"))
            if down is None:
                down = _num(node.get("traffic_down"))
            if up is not None or down is not None:
                up, down = up or 0.0, down or 0.0
                mode = str(node.get("traffic_limit_type") or "sum").lower()
                used = {"max": max(up, down), "min": min(up, down), "up": up, "down": down}.get(mode, up + down)
        return used, limit

    def _fmt_traffic(self, node: dict[str, Any]) -> str:
        used, limit = self._traffic(node)
        if used is None or not limit:
            return ""
        return f"流量 {self._fmt_bytes(used)} / {self._fmt_bytes(limit)}（{used / limit * 100:.0f}%）"

    @staticmethod
    def _expire_days(node: dict[str, Any]) -> Optional[float]:
        """剩余天数；无到期时间返回 None，已过期返回负数。"""
        expire = _parse_time(node.get("expired_at"))
        if expire is None:
            return None
        return (expire - datetime.now(timezone.utc)).total_seconds() / 86400

    def _fmt_expire(self, node: dict[str, Any]) -> str:
        days = self._expire_days(node)
        if days is None:
            return ""
        if days < 0:
            return f"已到期 {int(-days)} 天"
        if days < 1:
            return "今日到期"
        return f"剩余 {int(days)} 天"

    @staticmethod
    def _fmt_group(node: dict[str, Any]) -> str:
        group = str(node.get("group") or "").strip()
        tags = [tag.strip() for tag in str(node.get("tags") or "").split(";") if tag.strip()]
        parts = ([group] if group else []) + ([",".join(tags)] if tags else [])
        return " / ".join(parts)

    def _page_html(self, title: str, subtitle: str, body: str, stats: str = "") -> str:
        return f'''<!doctype html><html><head><meta charset="utf-8"><style>
        *{{box-sizing:border-box}} body{{width:{self.config.image_width}px;margin:0;padding:28px;background:#d7aabd;font-family:"Microsoft YaHei",sans-serif;color:#392d3b}}
        .wrap{{background:#f7e7ed;border-radius:24px;padding:26px;box-shadow:0 12px 28px #8f627455}} .top{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}}
        .tag{{background:#fff;border-radius:10px;padding:12px 22px;color:#ee6394;font-size:24px;font-weight:700}} .stamp{{background:#25b9e8;color:#fff;border-radius:12px;padding:12px 18px;font-size:18px;font-weight:700}}
        h1{{font-size:30px;margin:0 0 4px}} .sub{{color:#927f8c;font-size:15px}} .grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}}
        .card{{background:#fffafc;border-radius:18px;padding:20px;box-shadow:0 3px 10px #9f708522}} .node-head,.metric>div,.facts{{display:flex;justify-content:space-between;align-items:center}} .node-head{{margin-bottom:15px;font-size:18px}} .node-head small{{font-size:13px;color:#8e7c88}} .dot{{display:inline-block;width:11px;height:11px;border-radius:50%;margin-right:9px}} .online{{background:#42c88a}} .offline{{background:#f05d74}}
        .metric{{margin:10px 0}} .metric>div{{font-size:14px;color:#7d6d77}} .metric b{{color:#392d3b}} .metric i{{display:block;height:8px;background:#f1e5ea;border-radius:8px;margin-top:6px;overflow:hidden}} .metric em{{display:block;height:100%;border-radius:8px}} .facts{{flex-wrap:wrap;gap:8px;margin-top:18px;color:#877681;font-size:12px}} .updated{{border-top:1px solid #f0e2e8;margin-top:15px;padding-top:12px;color:#ad9ba4;font-size:11px}} .empty{{padding:40px;text-align:center;color:#927f8c}}
        .chart{{width:100%;height:72px;display:block;background:#fffafc;border-radius:8px;margin-top:6px}} .chartinfo{{font-size:13px;color:#7d6d77;margin-top:10px}}
        .stats{{display:flex;gap:10px;margin:16px 0 2px}} .pill{{background:#fff;border-radius:999px;padding:7px 16px;font-size:14px;font-weight:700;color:#7d6d77}} .pill.ok{{color:#2fa874}} .pill.bad{{color:#e2556d}}
        </style></head><body><main class="wrap"><div class="top"><span class="tag">Komari 监控</span><span class="stamp">{datetime.now().strftime('%Y-%m-%d %H:%M')}</span></div><h1>{title}</h1><div class="sub">{subtitle}</div>{f'<div class="stats">{stats}</div>' if stats else ''}<div class="grid">{body}</div></main></body></html>'''

    def _report_html(self, nodes: list[dict[str, Any]]) -> str:
        """Build a self-contained card; no external assets or copied template."""
        cards: list[str] = []
        for node in nodes:
            name = html.escape(str(node.get("name") or node.get("hostname") or node.get("id") or "未知节点"))
            online = bool(node.get("is_online"))
            cpu = _metric(node, "cpu", self._fractions)
            memory = _metric(node, "memory", self._fractions)
            disk = _metric(node, "disk", self._fractions)
            gpu = _metric(node, "gpu", self._fractions)
            network = node.get("network") if isinstance(node.get("network"), dict) else {}
            load = node.get("load") if isinstance(node.get("load"), dict) else {}
            def progress(label: str, value: Optional[float], color: str) -> str:
                shown = "-" if value is None else f"{value:.1f}%"
                width = 0 if value is None else min(max(value, 0), 100)
                return f'<div class="metric"><div><span>{label}</span><b>{shown}</b></div><i><em style="width:{width}%;background:{color}"></em></i></div>'
            rel = self._reltime(node.get("updated_at") or node.get("last_seen"))
            if online:
                updated = f"更新时间：{html.escape(rel)}"
            elif rel != "等待心跳":
                updated = f"最后在线：{html.escape(rel)}"
            else:
                updated = "状态：离线"
            gpu_row = progress('GPU', gpu, '#f2a33c') if gpu is not None else ""
            facts = [
                f"<span>上行 {html.escape(self._fmt_speed(network.get('up')))}</span>",
                f"<span>下行 {html.escape(self._fmt_speed(network.get('down')))}</span>",
                f"<span>负载 {html.escape(str(load.get('load1', '-')))}</span>",
                f"<span>运行 {html.escape(self._fmt_uptime(node.get('uptime')))}</span>",
            ]
            for extra in (self._fmt_traffic(node), self._fmt_expire(node), self._fmt_group(node)):
                if extra:
                    facts.append(f"<span>{html.escape(extra)}</span>")
            cards.append(f'''<section class="card"><div class="node-head"><div><span class="dot {'online' if online else 'offline'}"></span><strong>{name}</strong></div><small>{'在线' if online else '离线'}</small></div>
                {progress('CPU', cpu, '#ff6b9d')}{progress('内存', memory, '#8b7bff')}{progress('磁盘', disk, '#22b8cf')}{gpu_row}
                <div class="facts">{''.join(facts)}</div>
                <div class="updated">{updated}</div></section>''')
        body = "".join(cards) or '<div class="empty">Komari 没有返回节点数据</div>'
        total = len(nodes)
        online_count = sum(1 for node in nodes if node.get("is_online"))
        stats = (f'<span class="pill">共 {total} 节点</span><span class="pill ok">在线 {online_count}</span>'
                 f'<span class="pill bad">离线 {total - online_count}</span>') if nodes else ""
        return self._page_html("服务器运行状态", "实时资源概览 · 自动刷新由 AstrBot 监控任务负责", body, stats)

    @staticmethod
    def _mini_chart(label: str, values: list[Any], color: str, hours: int) -> str:
        width, height = 300, 72
        grid = "".join(f'<line x1="0" y1="{height - g / 100 * height:.1f}" x2="{width}" y2="{height - g / 100 * height:.1f}" stroke="#f1e5ea" stroke-width="1"/>' for g in (0, 50, 100))
        count = len(values)
        if count < 2:
            inner = f'<text x="{width / 2}" y="40" text-anchor="middle" font-size="12" fill="#927f8c">暂无数据</text>'
        else:
            prev = 0.0
            points = []
            for i, value in enumerate(values):
                val = _num(value)
                val = prev if val is None else min(max(val, 0), 100)
                prev = val
                points.append(f"{i / (count - 1) * width:.1f},{height - val / 100 * height:.1f}")
            inner = f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
        valid = [v for v in (_num(x) for x in values) if v is not None]
        suffix = f" · 当前 {valid[-1]:.1f}% · 峰值 {max(valid):.1f}%" if valid else ""
        return f'<div class="chartinfo">{label}（最近 {hours} 小时）{suffix}</div><svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart">{grid}{inner}</svg>'

    @classmethod
    def _traffic_chart(cls, series: list[dict[str, Any]], hours: int) -> str:
        width, height = 300, 72
        up = [v if (v := _num(p.get("net_out"))) is not None and v >= 0 else 0.0 for p in series]
        down = [v if (v := _num(p.get("net_in"))) is not None and v >= 0 else 0.0 for p in series]
        peak = max(up + down, default=0.0)
        scale = peak if peak > 0 else 1.0
        grid = "".join(f'<line x1="0" y1="{height - g / 100 * height:.1f}" x2="{width}" y2="{height - g / 100 * height:.1f}" stroke="#f1e5ea" stroke-width="1"/>' for g in (0, 50, 100))

        def poly(vals: list[float], color: str) -> str:
            points = " ".join(f"{i / (len(vals) - 1) * width:.1f},{height - 3 - min(v / scale, 1.0) * (height - 6):.1f}" for i, v in enumerate(vals))
            return f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'

        if len(up) < 2:
            inner = f'<text x="{width / 2}" y="40" text-anchor="middle" font-size="12" fill="#927f8c">暂无数据</text>'
        else:
            inner = poly(up, "#22b8cf") + poly(down, "#ff6b9d")
        info = ""
        if up:
            info = (f" · 当前 ↑{cls._fmt_speed(up[-1])} ↓{cls._fmt_speed(down[-1] if down else None)}"
                    f" · 峰值 ↑{cls._fmt_speed(max(up, default=0.0))} ↓{cls._fmt_speed(max(down, default=0.0))}")
        return f'<div class="chartinfo">流量（最近 {hours} 小时）{info}</div><svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" class="chart">{grid}{inner}</svg>'

    def _history_html(self, series_by_node: dict[str, Any], hours: int) -> str:
        cards: list[str] = []
        for entry in series_by_node.values():
            node = entry["node"]
            series = entry["series"]
            name = html.escape(str(node.get("name") or node.get("hostname") or node.get("id") or "未知节点"))
            online = bool(node.get("is_online"))
            valid_times = [t for t in (point.get("time") for point in series) if t]
            if valid_times:
                span = (f"{datetime.fromtimestamp(min(valid_times)).strftime('%m-%d %H:%M')}"
                        f" → {datetime.fromtimestamp(max(valid_times)).strftime('%m-%d %H:%M')}")
            else:
                span = f"最近 {hours} 小时"
            gpu_values = [p.get("gpu") for p in series]
            gpu_chart = self._mini_chart("GPU", gpu_values, "#f2a33c", hours) if any(v is not None for v in gpu_values) else ""
            charts = (
                self._mini_chart("CPU", [p.get("cpu") for p in series], "#ff6b9d", hours)
                + self._mini_chart("内存", [p.get("ram") for p in series], "#8b7bff", hours)
                + self._mini_chart("磁盘", [p.get("disk") for p in series], "#22b8cf", hours)
                + gpu_chart
                + self._traffic_chart(series, hours)
            )
            cards.append(f'<section class="card"><div class="node-head"><div><span class="dot {"online" if online else "offline"}"></span><strong>{name}</strong></div><small>{"在线" if online else "离线"} · {span}</small></div>{charts}</section>')
        body = "".join(cards) or '<div class="empty">没有可用的历史数据</div>'
        return self._page_html("历史资源趋势", f"CPU / 内存 / 磁盘 · 最近 {hours} 小时", body)

    def _history_text(self, series_by_node: dict[str, Any], hours: int) -> str:
        lines = [f"📊 Komari 历史（最近 {hours} 小时）"]
        for entry in series_by_node.values():
            node = entry["node"]
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            series = entry["series"]
            last = series[-1] if series else {}
            def fmt(v: Any) -> str:
                return "-" if v is None else f"{v:.1f}%"
            traffic = ""
            if last.get("net_in") is not None or last.get("net_out") is not None:
                traffic = f" / 流量 ↑{self._fmt_speed(last.get('net_out'))} ↓{self._fmt_speed(last.get('net_in'))}"
            gpu = "" if last.get("gpu") is None else f" / GPU {fmt(last.get('gpu'))}"
            lines.append(f"{name}：CPU {fmt(last.get('cpu'))} / 内存 {fmt(last.get('ram'))} / 磁盘 {fmt(last.get('disk'))}{gpu}{traffic}")
        return "\n".join(lines) or "没有可用的历史数据。"

    async def _chain_from_html(self, html_text: str, text_fallback: str) -> MessageChain:
        """Build an image Chain from HTML, falling back to text on render failure or image_output off."""
        if not self.config.image_output:
            return MessageChain().message(text_fallback)
        try:
            image_url = await self.html_render(html_text, {"content": html_text}, options={"type": "jpeg", "quality": 92, "full_page": True})
            if image_url:
                return MessageChain([Image.fromURL(image_url)])
        except Exception as exc:
            self.logger.warning("状态卡片渲染失败，回退文本：%s", exc)
        return MessageChain().message(text_fallback)

    async def _report_chain(self, nodes: list[dict[str, Any]]) -> MessageChain:
        return await self._chain_from_html(self._report_html(nodes), self._format_report(nodes))

    async def _report_result(self, event: AstrMessageEvent, nodes: list[dict[str, Any]]):
        if not nodes:
            return event.plain_result("Komari 没有返回节点。")
        return event.chain_result(await self._report_chain(nodes))

    # ---- 节点过滤 / 选择 ----

    @staticmethod
    def _node_key(node: dict[str, Any]) -> str:
        return str(node.get("uuid") or node.get("id") or "")

    @staticmethod
    def _node_idents(node: dict[str, Any]) -> list[str]:
        return [str(node.get(key) or "").lower() for key in ("name", "hostname", "id", "uuid") if node.get(key)]

    def _filter_tokens(self) -> list[str]:
        return [t.strip().lower() for t in (self.config.filter_nodes or "").split(",") if t.strip()]

    def _monitored(self, node: dict[str, Any]) -> bool:
        mode = self.config.filter_mode
        if mode == "none":
            return True
        idents = self._node_idents(node)
        hit = any(any(token in ident for ident in idents) for token in self._filter_tokens())
        return hit if mode == "allow" else (not hit)

    def _visible(self, nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [node for node in nodes if self._monitored(node)]

    @staticmethod
    def _selectors(args: tuple[Any, ...]) -> list[tuple[str, str]]:
        """把查询参数解析成 (类型, 值)：group:xx / tag:xx 走分组与标签，其余按节点名匹配。"""
        selectors: list[tuple[str, str]] = []
        for arg in args:
            token = str(arg).strip()
            if not token:
                continue
            lowered = token.lower()
            for prefix, kind in _SELECTOR_PREFIXES.items():
                if lowered.startswith(prefix):
                    value = lowered[len(prefix):].strip()
                    if value:
                        selectors.append((kind, value))
                    break
            else:
                selectors.append(("keyword", lowered))
        return selectors

    @staticmethod
    def _matches(node: dict[str, Any], kind: str, value: str) -> bool:
        if kind == "group":
            return value in str(node.get("group") or "").lower()
        if kind == "tag":
            tags = [tag.strip().lower() for tag in str(node.get("tags") or "").split(";") if tag.strip()]
            return any(value in tag for tag in tags)
        return any(value in ident for ident in KomariWatchPlugin._node_idents(node))

    def _select(self, nodes: list[dict[str, Any]], args: tuple[Any, ...]) -> list[dict[str, Any]]:
        selectors = self._selectors(args)
        if not selectors:
            return nodes
        return [node for node in nodes
                if any(self._matches(node, kind, value) for kind, value in selectors)]

    @staticmethod
    def _keyword_hint(args: tuple[Any, ...]) -> str:
        tokens = [str(arg).strip() for arg in args if str(arg).strip()]
        return "、".join(f"「{token}」" for token in tokens)

    def _warn_filter_misconfig(self) -> None:
        if self._filter_warned or self.config.filter_mode != "allow":
            return
        if not self._filter_tokens():
            self.logger.warning("filter_mode 为 allow 但 filter_nodes 为空：当前不会监控或推送任何节点。"
                                "请填写 filter_nodes，或将 filter_mode 改为 none / deny。")
            self._filter_warned = True

    def _can_alert(self, record: dict[str, Any], kind: str, now: float) -> bool:
        """Prevent repeated alerts when a node flaps around a threshold."""
        last = _num(record.get("sent", {}).get(kind))
        return last is None or self.config.alert_cooldown == 0 or now - last >= self.config.alert_cooldown

    def _check_expire(self, node: dict[str, Any], record: dict[str, Any], name: str, now: float) -> list[str]:
        """到期提醒：到期前 expire_remind_days 天起每日提醒一次；续费后重新计时。"""
        raw = node.get("expired_at")
        if record.get("expire_at") != raw:
            # 到期时间变化说明已续费，重置提醒计时。
            record["expire_at"] = raw
            record.pop("expire_last_remind", None)
        if self.config.expire_remind_days <= 0:
            return []
        days = self._expire_days(node)
        if days is None or days > self.config.expire_remind_days:
            return []
        last = _num(record.get("expire_last_remind"))
        if last is not None and now - last < 86400:
            return []
        record["expire_last_remind"] = now
        when = _parse_time(raw)
        stamp = when.strftime("%Y-%m-%d") if when else "-"
        left = f"已到期 {int(-days)} 天" if days < 0 else f"剩余 {int(days)} 天"
        return [f"⏳ Komari 节点即将到期\n节点：{name}\n{left}（到期时间 {stamp}）"]

    def _check_traffic(self, node: dict[str, Any], record: dict[str, Any], name: str,
                       now: float, is_online: bool) -> list[str]:
        """流量限额告警：使用率达到阈值时告警，回落到阈值以下时恢复。

        取不到累计流量计数器（面板不提供）时静默跳过，不做无依据的估算。
        """
        if self.config.traffic_alert_percent <= 0:
            return []
        used, limit = self._traffic(node)
        if used is None or not limit:
            return []
        percent = used / limit * 100
        threshold = self.config.traffic_alert_percent
        if (is_online and percent >= threshold and not record["active"].get("traffic")
                and self._can_alert(record, "traffic", now)):
            record["sent"]["traffic"] = now
            record["active"]["traffic"] = True
            return [f"⚠️ Komari 流量告警\n节点：{name}\n已用 {self._fmt_bytes(used)} / {self._fmt_bytes(limit)}"
                    f"（{percent:.0f}%），阈值 {threshold:g}%"]
        if record["active"].get("traffic") and is_online and percent < threshold:
            record["active"]["traffic"] = False
            if not self.config.notify_recovery:
                return []
            return [f"✅ Komari 流量恢复\n节点：{name}\n已用 {self._fmt_bytes(used)} / {self._fmt_bytes(limit)}"
                    f"（{percent:.0f}%），已低于阈值 {threshold:g}%"]
        return []

    # ---- 静默 ----

    def _muted(self, target: str) -> bool:
        muted = self.state.get("muted")
        if not isinstance(muted, dict):
            return False
        until = _num(muted.get(target))
        return until is not None and time.time() < until

    # ---- 告警历史 ----

    def _append_alert(self, text: str) -> None:
        history = self.state.setdefault("alert_history", [])
        history.append({"time": time.time(), "text": text})
        if len(history) > _ALERT_HISTORY_LIMIT:
            del history[: len(history) - _ALERT_HISTORY_LIMIT]

    async def _send(self, text: str) -> None:
        for target in self._targets():
            if self._muted(target):
                continue
            try:
                await self.context.send_message(target, MessageChain().message(text))
            except Exception as exc:
                self.logger.warning("向 %s 推送失败: %s", target, exc)

    async def _send_chain(self, chain: MessageChain) -> None:
        for target in self._targets():
            if self._muted(target):
                continue
            try:
                await self.context.send_message(target, chain)
            except Exception as exc:
                self.logger.warning("向 %s 推送失败: %s", target, exc)

    def _fixed_report_due(self, now: float) -> bool:
        """status_report_time（每天 HH:MM，本地时间）到期判定：当天到点后尚未推送过即触发。"""
        spec = (self.config.status_report_time or "").strip()
        if not spec:
            return False
        parts = spec.split(":")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit() or int(parts[0]) > 23 or int(parts[1]) > 59:
            if not self._time_report_warned:
                self.logger.warning("status_report_time 配置无效（%s），应为 HH:MM 格式，如 09:00。", spec)
                self._time_report_warned = True
            return False
        now_dt = datetime.fromtimestamp(now)
        trigger = now_dt.replace(hour=int(parts[0]), minute=int(parts[1]), second=0, microsecond=0)
        last = _num(self.state.get("last_status_report"))
        return now_dt >= trigger and (last is None or last < trigger.timestamp())

    async def _maybe_status_push(self, nodes: list[dict[str, Any]], now: float) -> None:
        interval = self.config.status_report_interval
        fixed_due = self._fixed_report_due(now)
        if interval <= 0 and not fixed_due:
            return
        if not fixed_due:
            last = _num(self.state.get("last_status_report"))
            if last is not None and now - last < interval * 3600:
                return
        visible = self._visible(nodes)
        if not visible:
            return
        chain = await self._report_chain(visible)
        await self._send_chain(chain)
        self.state["last_status_report"] = now
        self._save_state()

    def _prune_missing(self, known_keys: set[str]) -> None:
        nodes = self.state.get("nodes")
        if not isinstance(nodes, dict):
            return
        for key in list(nodes.keys()):
            record = nodes[key]
            if key in known_keys:
                record["missing"] = 0
                continue
            missing = record.get("missing", 0) + 1
            record["missing"] = missing
            if missing >= self.config.prune_missing_cycles:
                del nodes[key]

    def _prune_muted(self) -> None:
        muted = self.state.get("muted")
        if not isinstance(muted, dict):
            return
        now_ts = time.time()
        for key in [key for key, value in muted.items() if not isinstance(value, (int, float)) or value <= now_ts]:
            del muted[key]

    async def _check_once(self, track_failure: bool = True) -> tuple[bool, dict[str, Any]]:
        """Run one monitoring cycle; returns (failed, summary with online/offline/alert counts)."""
        self._warn_filter_misconfig()
        async with self._check_lock:
            nodes, error = await self._snapshot()
            if error:
                if track_failure:
                    self._failure_count += 1
                    if (self.config.panel_fail_cycles > 0
                            and self._failure_count >= self.config.panel_fail_cycles
                            and not self.state.get("panel_alert")):
                        self.state["panel_alert"] = True
                        message = f"⚠️ Komari 面板不可达\n连续 {self._failure_count} 次检查失败，离线与高负载告警暂停。\n{error}"
                        self._append_alert(message)
                        self._save_state()
                        await self._send(message)
                self.logger.warning(error)
                return True, {"error": error}
            self._failure_count = 0
            if self.state.get("panel_alert"):
                self.state["panel_alert"] = False
                message = "🟢 Komari 面板已恢复\n检查恢复正常，告警继续生效。"
                self._append_alert(message)
                self._save_state()
                await self._send(message)
            now = datetime.now(timezone.utc).timestamp()
            known_keys: set[str] = set()
            offline_alerted: list[str] = []
            offline_recovered: list[tuple[str, str]] = []
            high_alerted: list[str] = []
            high_recovered: list[tuple[str, str]] = []
            restarts: list[str] = []
            long_offline: list[str] = []
            expiring: list[str] = []
            traffic_alerts: list[str] = []
            online_count = 0
            offline_count = 0
            for node in nodes:
                key = str(node.get("uuid") or node.get("id") or node.get("name") or "unknown")
                known_keys.add(key)
                if not self._monitored(node):
                    continue
                record = self.state.setdefault("nodes", {}).setdefault(key, {"offline": 0, "high": 0, "sent": {}, "active": {}})
                record.setdefault("sent", {})
                record.setdefault("active", {})
                is_online = bool(node["is_online"])
                if is_online:
                    online_count += 1
                else:
                    offline_count += 1
                record["offline"] = record.get("offline", 0) + 1 if not is_online else 0
                # 记录"首次观察到离线"的时刻，离线时长从这里算起，而不是从告警发出时刻算起。
                if not is_online:
                    record.setdefault("offline_started", now)
                cpu = _metric(node, "cpu", self._fractions)
                mem = _metric(node, "memory", self._fractions)
                disk = _metric(node, "disk", self._fractions)
                # 离线节点的指标可能是陈旧历史值，跳过其高负载告警，避免死节点误报。
                high = is_online and ((cpu is not None and cpu >= self.config.cpu_threshold) or (mem is not None and mem >= self.config.memory_threshold) or (disk is not None and disk >= self.config.disk_threshold))
                record["high"] = record.get("high", 0) + 1 if high else 0
                if not is_online and record["active"].get("high"):
                    # 离线期间指标不再更新，清空高负载状态，避免节点重新上线后
                    # 误报"负载恢复"，也避免该状态长期残留。
                    record["active"]["high"] = False
                name = node.get("name") or key
                uptime = _num(node.get("uptime"))
                prev_uptime = _num(record.get("uptime"))
                if (self.config.notify_restart and is_online and uptime is not None
                        and prev_uptime is not None and prev_uptime - uptime > 30
                        and self._can_alert(record, "restart", now)):
                    record["sent"]["restart"] = now
                    restarts.append(f"🔄 Komari 节点重启\n节点：{name}\n此前已运行 {self._fmt_duration(prev_uptime)}，当前已运行 {self._fmt_duration(uptime)}")
                if uptime is not None:
                    record["uptime"] = uptime
                # 使用 >= 而非 ==：若触发告警时仍在冷却期内（_can_alert 为 False），
                # 计数器会继续累加，== 判断将永不再成立，导致本次宕机静默。
                if (not is_online
                        and record["offline"] >= self.config.offline_grace_cycles
                        and not record["active"].get("offline")
                        and self._can_alert(record, "offline", now)):
                    record["sent"]["offline"] = now
                    record["active"]["offline"] = True
                    offline_alerted.append(name)
                elif is_online and record["active"].get("offline"):
                    duration = self._fmt_duration(now - record.get("offline_started", now))
                    record["active"]["offline"] = False
                    record.pop("offline_started", None)
                    offline_recovered.append((name, duration))
                elif is_online:
                    record.pop("offline_started", None)
                if (not is_online and record["active"].get("offline")
                        and self.config.long_offline_remind_hours > 0):
                    offline_secs = now - record.get("offline_started", now)
                    last_remind = _num(record.get("offline_last_remind"))
                    if (offline_secs >= self.config.long_offline_remind_hours * 3600
                            and (last_remind is None or now - last_remind >= 86400)):
                        record["offline_last_remind"] = now
                        long_offline.append(f"🔴 Komari 节点仍离线\n节点：{name}\n已离线 {self._fmt_duration(offline_secs)}")
                if (record["high"] >= self.config.high_load_cycles
                        and not record["active"].get("high")
                        and self._can_alert(record, "high", now)):
                    details = ", ".join(f"{label} {value:.1f}%" for value, label, limit in (
                        (cpu, "CPU", self.config.cpu_threshold),
                        (mem, "内存", self.config.memory_threshold),
                        (disk, "磁盘", self.config.disk_threshold)) if value is not None and value >= limit)
                    record["sent"]["high"] = now
                    record["active"]["high"] = True
                    record["high_started"] = now
                    high_alerted.append(f"⚠️ Komari 高负载告警\n节点：{name}\n{details}")
                elif is_online and not high and record["active"].get("high"):
                    # 只有节点仍在在线时才报恢复，避免把"离线"误报成"负载恢复"。
                    duration = self._fmt_duration(now - record.get("high_started", now))
                    record["active"]["high"] = False
                    high_recovered.append((name, duration))
                expiring += self._check_expire(node, record, name, now)
                traffic_alerts += self._check_traffic(node, record, name, now, is_online)
            alerts: list[str] = []
            if offline_alerted:
                if len(offline_alerted) == 1:
                    alerts.append(f"🔴 Komari 离线告警\n节点：{offline_alerted[0]}\n连续 {self.config.offline_grace_cycles} 个周期未收到心跳。")
                else:
                    alerts.append(f"🔴 Komari 离线告警\n{len(offline_alerted)} 个节点连续 {self.config.offline_grace_cycles} 个周期未收到心跳：\n" + "\n".join(f"· {n}" for n in offline_alerted))
            if self.config.notify_recovery:
                if len(offline_recovered) == 1:
                    name, duration = offline_recovered[0]
                    alerts.append(f"🟢 Komari 节点恢复\n节点：{name}\n离线时长：{duration}")
                elif len(offline_recovered) > 1:
                    alerts.append(f"🟢 Komari 节点恢复\n{len(offline_recovered)} 个节点已恢复：\n" + "\n".join(f"· {n}（离线时长 {d}）" for n, d in offline_recovered))
                if len(high_recovered) == 1:
                    name, duration = high_recovered[0]
                    alerts.append(f"✅ Komari 负载恢复\n节点：{name}\n持续时长：{duration}")
                elif len(high_recovered) > 1:
                    alerts.append(f"✅ Komari 负载恢复\n{len(high_recovered)} 个节点已恢复：\n" + "\n".join(f"· {n}（持续时长 {d}）" for n, d in high_recovered))
            alerts.extend(restarts)
            alerts.extend(long_offline)
            alerts.extend(expiring)
            alerts.extend(high_alerted)
            alerts.extend(traffic_alerts)
            self._prune_missing(known_keys)
            self._prune_muted()
            for alert in alerts:
                self._append_alert(alert)
            self._save_state()
            if alerts:
                await self._send("\n\n".join(alerts))
            if self.config.status_report_interval > 0 or (self.config.status_report_time or "").strip():
                await self._maybe_status_push(nodes, now)
            return False, {"online": online_count, "offline": offline_count, "alerts": len(alerts)}

    def _poll_delay(self, failed: bool) -> float:
        if not failed:
            return float(self.config.poll_interval)
        factor = 2 ** min(max(self._failure_count - 1, 0), 10)
        return float(min(self.config.poll_interval * factor, 1800))

    async def _monitor_loop(self) -> None:
        try:
            while not self._stop.is_set():
                failed = False
                if self._targets() and self.config.komari_url:
                    try:
                        failed, _ = await self._check_once()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        # 单个周期内的意外异常不应终止整个监控循环，否则监控会静默失效。
                        failed = True
                        self.logger.exception("监控循环本轮异常，已跳过并继续下一轮：%s", exc)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_delay(failed))
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def _start_monitor(self) -> None:
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _snapshot(self) -> tuple[list[dict[str, Any]], Optional[str]]:
        static, error = await self._nodes()
        if error:
            return [], error
        live, ws_live = await self._realtime()
        if ws_live:
            stamp = datetime.now(timezone.utc).isoformat()
            for item in live:
                item.setdefault("updated_at", stamp)
            # 只为缺失内存/磁盘指标的在线节点拉历史记录补全展示，
            # 不再因个别节点缺指标而对全部节点发起 N 次请求。
            lacking = [item for item in live
                       if _metric(item, "memory", self._fractions) is None or _metric(item, "disk", self._fractions) is None]
            if lacking:
                history_by_key = {self._node_key(item): item for item in await self._history_realtime(lacking)}
                for item in live:
                    fallback = history_by_key.get(self._node_key(item))
                    if not fallback:
                        continue
                    for field in ("cpu_usage", "ram_usage", "disk_usage"):
                        if item.get(field) is None and fallback.get(field) is not None:
                            item[field] = fallback[field]
                    for section in ("cpu", "ram", "memory", "disk", "storage", "network", "load"):
                        if isinstance(fallback.get(section), dict):
                            current = item.get(section)
                            item[section] = {**fallback[section], **current} if isinstance(current, dict) else fallback[section]
        else:
            live = await self._history_realtime(static)
        merged = self._merge_nodes(static, live)
        live_keys = {self._node_key(item) for item in live}
        for node in merged:
            node["is_online"] = self._is_online(node, live_keys, ws_live)
        return merged, None

    @filter.command("komari_status", alias=["kstatus", "komari"])
    async def komari_status(self, event: AstrMessageEvent, *args):
        """查询所有 Komari 节点的状态与资源使用率；可加节点名（支持子串）只看指定节点。"""
        self._start_monitor()
        self._warn_filter_misconfig()
        async with self._check_lock:
            nodes, error = await self._snapshot()
        if error:
            yield event.plain_result(error)
            return
        selected = self._visible(self._select(nodes, args))
        if args and str(args[0]).strip() and nodes and not selected:
            yield event.plain_result(f"没有匹配{self._keyword_hint(args)}的节点，可发送 /komari_nodes 查看节点列表。")
            return
        yield await self._report_result(event, selected)

    @filter.command("komari_realtime", alias=["krealtime", "实时状态"])
    async def komari_realtime(self, event: AstrMessageEvent, *args):
        """查询 Komari WebSocket 实时数据（不经历史兜底）；WebSocket 不可用时提示改用状态命令。"""
        self._start_monitor()
        async with self._check_lock:
            live, ws_ok = await self._realtime()
            static: list[dict[str, Any]] = []
            static_error: Optional[str] = None
            if ws_ok:
                static, static_error = await self._nodes()
        if not ws_ok:
            yield event.plain_result("WebSocket 实时通道暂时不可用（可能被反代禁用），请改用 /komari_status 查看状态报告。")
            return
        if static_error:
            yield event.plain_result(static_error)
            return
        merged = self._merge_nodes(static, live)
        # 只有出现在 WebSocket 返回里的节点才是在线，掉线节点如实显示离线。
        live_keys = {self._node_key(item) for item in live}
        for node in merged:
            node["is_online"] = self._node_key(node) in live_keys
        selected = self._visible(self._select(merged, args))
        if args and str(args[0]).strip() and merged and not selected:
            yield event.plain_result(f"没有匹配{self._keyword_hint(args)}的节点，可发送 /komari_nodes 查看节点列表。")
            return
        yield await self._report_result(event, selected)

    @filter.command("komari_history", alias=["khistory", "历史"])
    async def komari_history(self, event: AstrMessageEvent, *args):
        """查询历史资源趋势；如 /komari_history 6 nodeA（小时数 1-24，可加节点名或 group:/tag: 过滤）。"""
        self._start_monitor()
        hours, tokens = 1, []
        for arg in args:
            text = str(arg).strip()
            if not text:
                continue
            if text.isdigit():
                hours = int(text)
            else:
                tokens.append(text)
        hours = max(1, min(hours, 24))
        async with self._check_lock:
            static, error = await self._snapshot()
        if error:
            yield event.plain_result(error)
            return
        static = self._visible(static)
        if not static:
            yield event.plain_result("Komari 没有返回节点。")
            return
        if tokens:
            matched = self._select(static, tuple(tokens))
            if not matched:
                yield event.plain_result(f"没有匹配{self._keyword_hint(tuple(tokens))}的节点。")
                return
            static = matched
        tasks = [self._history_by_node(node, hours) for node in static]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        series_by_node: dict[str, Any] = {}
        for result in results:
            if isinstance(result, BaseException) or not isinstance(result, tuple):
                continue
            node, series = result
            if series:
                series_by_node[str(node.get("uuid") or node.get("id") or node.get("name"))] = {"node": node, "series": series}
        if not series_by_node:
            yield event.plain_result("没有可用的历史数据。")
            return
        chain = await self._chain_from_html(self._history_html(series_by_node, hours), self._history_text(series_by_node, hours))
        yield event.chain_result(chain)

    @filter.command("komari_ping", alias=["kping", "延迟"])
    async def komari_ping(self, event: AstrMessageEvent, *args):
        """查看 Ping 任务的延迟与丢包；如 /komari_ping 4 nodeA（小时数 1-24，默认 1）。"""
        self._start_monitor()
        hours, tokens = 1, []
        for arg in args:
            text = str(arg).strip()
            if not text:
                continue
            if text.isdigit():
                hours = int(text)
            else:
                tokens.append(text)
        hours = max(1, min(hours, 24))
        async with self._check_lock:
            nodes, error = await self._snapshot()
        if error:
            yield event.plain_result(error)
            return
        selected = self._visible(self._select(nodes, tuple(tokens)))[:5]
        if not selected:
            if tokens:
                yield event.plain_result(f"没有匹配{self._keyword_hint(tuple(tokens))}的节点。")
            else:
                yield event.plain_result("Komari 没有返回节点。")
            return
        lines = [f"🏓 Komari 延迟（最近 {hours} 小时）"]
        empty = 0
        for node in selected:
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            grouped = self._ping_stats(await self._ping_records(node.get("uuid") or node.get("id"), hours))
            if not grouped:
                empty += 1
                lines.append(f"{name}：没有延迟记录")
                continue
            lines.append(name)
            lines.extend(self._ping_lines(grouped))
        if empty == len(selected):
            lines.append("面板未返回延迟数据：可能未配置 Ping 任务，或该接口被限制。")
        yield event.plain_result("\n".join(lines))

    @filter.command("komari_recent", alias=["krecent", "最近上报"])
    async def komari_recent(self, event: AstrMessageEvent, *args):
        """查看节点最近一条上报明细（温度、进程数、连接数、累计流量等）；如 /komari_recent nodeA。"""
        self._start_monitor()
        async with self._check_lock:
            nodes, error = await self._snapshot()
        if error:
            yield event.plain_result(error)
            return
        selected = self._visible(self._select(nodes, args))[:5]
        if not selected:
            if args and str(args[0]).strip():
                yield event.plain_result(f"没有匹配{self._keyword_hint(args)}的节点。")
            else:
                yield event.plain_result("Komari 没有返回节点。")
            return
        lines = ["🕒 Komari 最近上报"]
        for node in selected:
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            record, error = await self._recent_record(node)
            if not record:
                lines.append(f"{name}：{error}")
                continue
            lines.append(f"{name} · {self._reltime(record.get('time'))}")
            lines.append(self._recent_detail(record))
        yield event.plain_result("\n".join(lines))

    def _recent_detail(self, record: dict[str, Any]) -> str:
        parts: list[str] = []
        cpu = _num(record.get("cpu"))
        if cpu is not None:
            parts.append(f"CPU {cpu:.1f}%")
        gpu = _num(record.get("gpu"))
        if gpu is not None:
            parts.append(f"GPU {gpu:.1f}%")
        ram_used, ram_total = _num(record.get("ram")), _num(record.get("ram_total"))
        if ram_used is not None and ram_total:
            parts.append(f"内存 {self._fmt_bytes(ram_used)}/{self._fmt_bytes(ram_total)}（{ram_used / ram_total * 100:.0f}%）")
        disk_used, disk_total = _num(record.get("disk")), _num(record.get("disk_total"))
        if disk_used is not None and disk_total:
            parts.append(f"磁盘 {self._fmt_bytes(disk_used)}/{self._fmt_bytes(disk_total)}（{disk_used / disk_total * 100:.0f}%）")
        load = _num(record.get("load"))
        if load is not None:
            parts.append(f"负载 {load:.2f}")
        temp = _num(record.get("temp"))
        if temp is not None:
            parts.append(f"温度 {temp:.1f}℃")
        process = _num(record.get("process"))
        if process is not None:
            parts.append(f"进程 {int(process)}")
        connections = _num(record.get("connections"))
        if connections is not None:
            parts.append(f"连接 {int(connections)}")
        net_in, net_out = _num(record.get("net_in")), _num(record.get("net_out"))
        if net_in is not None or net_out is not None:
            parts.append(f"↑{self._fmt_speed(net_out)} ↓{self._fmt_speed(net_in)}")
        total_up, total_down = _num(record.get("net_total_up")), _num(record.get("net_total_down"))
        if total_up is not None or total_down is not None:
            parts.append(f"累计 ↑{self._fmt_bytes(total_up)} ↓{self._fmt_bytes(total_down)}")
        return " / ".join(parts) if parts else "无可用指标。"

    @filter.command("komari_help", alias=["khelp", "komari帮助"])
    async def komari_help(self, event: AstrMessageEvent):
        """查看 Komari 插件全部命令。"""
        lines = [
            "📖 Komari 监控命令",
            "/komari_status [节点|group:x|tag:x] - 状态卡片",
            "/komari_realtime [节点|group:x|tag:x] - WebSocket 实时数据",
            "/komari_history [小时] [节点] - 历史趋势（1-24 小时）",
            "/komari_ping [小时] [节点] - Ping 延迟与丢包（1-24 小时）",
            "/komari_recent [节点] - 最近一条上报明细",
            "/komari_nodes - 节点列表速查（含分组与标签）",
            "/komari_top [指标] [数量] - 资源占用 Top 榜（cpu/mem/disk/gpu/uptime）",
            "/komari_alerts - 最近告警记录",
            "/komari_public - 站点信息",
            "/komari_version - 服务端版本",
            "/komari_bind / /komari_unbind - 绑定/解绑告警推送",
            "/komari_mute [分钟] [all] - 静默当前会话（all 为全部会话）",
            "/komari_unmute [all] - 解除静默",
            "/komari_check - 立即检查一次并返回结果摘要",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("komari_nodes", alias=["knodes", "节点列表"])
    async def komari_nodes(self, event: AstrMessageEvent):
        """列出全部节点名称，便于填写 filter_nodes 或查询命令参数。"""
        static, error = await self._nodes()
        if error:
            yield event.plain_result(error)
            return
        if not static:
            yield event.plain_result("Komari 没有返回节点。")
            return
        lines = ["📋 Komari 节点列表"]
        excluded = 0
        for node in static:
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            group = self._fmt_group(node)
            suffix = f"（{group}）" if group else ""
            if self._monitored(node):
                lines.append(f"· {name}{suffix}")
            else:
                lines.append(f"· {name}{suffix}（已被 filter 配置排除）")
                excluded += 1
        summary = f"共 {len(static)} 个节点"
        if excluded:
            summary += f"，其中 {excluded} 个被过滤配置排除"
        lines.append(summary)
        lines.append("提示：查询命令可用 group:分组名 或 tag:标签 筛选节点。")
        yield event.plain_result("\n".join(lines))

    @filter.command("komari_top", alias=["ktop", "节点排行"])
    async def komari_top(self, event: AstrMessageEvent, *args):
        """查看资源占用 Top 榜；如 /komari_top mem 10（指标 cpu/mem/disk/gpu/uptime，默认 cpu 前 5，仅在线节点）。"""
        self._start_monitor()
        metric, count = "cpu", 5
        for arg in args:
            text = str(arg).strip().lower()
            if text in ("cpu", "c"):
                metric = "cpu"
            elif text in ("mem", "memory", "m", "内存"):
                metric = "memory"
            elif text in ("disk", "d", "磁盘"):
                metric = "disk"
            elif text in ("gpu", "g", "显卡"):
                metric = "gpu"
            elif text in ("uptime", "u", "运行时长"):
                metric = "uptime"
            elif text.isdigit():
                count = int(text)
        count = max(1, min(count, 20))
        async with self._check_lock:
            nodes, error = await self._snapshot()
        if error:
            yield event.plain_result(error)
            return
        if metric == "uptime":
            scored = [(_num(node.get("uptime")), node) for node in self._visible(nodes) if node.get("is_online")]
        else:
            scored = [(_metric(node, metric, self._fractions), node) for node in self._visible(nodes) if node.get("is_online")]
        scored = [(value, node) for value, node in scored if value is not None]
        if not scored:
            yield event.plain_result("没有可排序的在线节点。")
            return
        scored.sort(key=lambda pair: pair[0], reverse=True)
        label = {"cpu": "CPU", "memory": "内存", "disk": "磁盘", "gpu": "GPU", "uptime": "运行时长"}[metric]
        top = scored[:count]
        lines = [f"🏆 Komari {label} Top {len(top)}（在线节点）"]
        for rank, (value, node) in enumerate(top, 1):
            name = node.get("name") or node.get("hostname") or node.get("id") or "未知节点"
            shown = self._fmt_duration(value) if metric == "uptime" else f"{value:.1f}%"
            lines.append(f"{rank}. {name} · {shown}")
        yield event.plain_result("\n".join(lines))

    @filter.command("komari_public", alias=["kpublic", "站点信息"])
    async def komari_public(self, event: AstrMessageEvent):
        """查询 Komari 公开站点信息。"""
        payload, error = await self._get_json("/api/public")
        if error:
            yield event.plain_result(error)
            return
        data = payload.get("data", payload) if payload else {}
        if not isinstance(data, dict):
            yield event.plain_result("Komari 未返回公开站点信息。")
            return
        name = data.get("sitename") or data.get("name") or "未命名站点"
        description = data.get("description") or "无"
        yield event.plain_result(f"🌐 Komari 站点\n名称：{name}\n描述：{description}")

    @filter.command("komari_version", alias=["kversion", "版本"])
    async def komari_version(self, event: AstrMessageEvent):
        """查询 Komari 服务端版本。"""
        payload, error = await self._get_json("/api/version")
        if error:
            yield event.plain_result(error)
            return
        data = payload.get("data", payload) if payload else {}
        if not isinstance(data, dict):
            yield event.plain_result("Komari 未返回版本信息。")
            return
        version = data.get("version", "未知")
        commit = data.get("hash") or data.get("commit") or ""
        yield event.plain_result(f"Komari 版本：{version}{f' ({commit})' if commit else ''}")

    @filter.command("komari_bind")
    async def komari_bind(self, event: AstrMessageEvent):
        """绑定当前 OneBot 会话为告警接收目标。"""
        target = event.unified_msg_origin
        targets = self._targets()
        if target not in targets:
            targets.append(target)
            self.state["targets"] = targets
            self._save_state()
        self._start_monitor()
        yield event.plain_result("✅ 当前会话已绑定 Komari 告警推送；发送 /komari_unbind 可解除绑定。")

    @filter.command("komari_unbind")
    async def komari_unbind(self, event: AstrMessageEvent):
        """解除当前会话的告警推送。"""
        self.state["targets"] = [item for item in self._targets() if item != event.unified_msg_origin]
        self._save_state()
        yield event.plain_result("✅ 当前会话已解除绑定。")

    @filter.command("komari_mute")
    async def komari_mute(self, event: AstrMessageEvent, *args):
        """临时静默当前会话的告警推送，默认 30 分钟；加 all 静默全部绑定会话。"""
        minutes, scope_all = 30, False
        for arg in args:
            text = str(arg).strip().lower()
            if text.isdigit():
                minutes = int(text)
            elif text in ("all", "全部", "全局"):
                scope_all = True
        minutes = max(1, min(minutes, 1440))
        until = time.time() + minutes * 60
        muted = self.state.setdefault("muted", {})
        if scope_all:
            targets = self._targets()
            if not targets:
                yield event.plain_result("当前没有绑定任何会话，无静默对象；可先发送 /komari_bind 绑定。")
                return
            for target in targets:
                muted[target] = until
            scope_text = "全部绑定会话"
        else:
            muted[event.unified_msg_origin] = until
            scope_text = "当前会话"
        self._save_state()
        yield event.plain_result(f"🔇 已静默{scope_text} {minutes} 分钟，期间不推送告警。发送 /komari_unmute 可提前恢复。")

    @filter.command("komari_unmute")
    async def komari_unmute(self, event: AstrMessageEvent, *args):
        """解除静默；加 all 解除全部会话的静默。"""
        muted = self.state.get("muted")
        if not isinstance(muted, dict):
            muted = {}
        if any(str(arg).strip().lower() in ("all", "全部", "全局") for arg in args):
            muted.clear()
            scope_text = "全部会话"
        else:
            muted.pop(event.unified_msg_origin, None)
            scope_text = "当前会话"
        self.state["muted"] = muted
        self._save_state()
        yield event.plain_result(f"🔊 已解除{scope_text}的静默，恢复正常推送。")

    @filter.command("komari_alerts", alias=["kalerts", "告警历史"])
    async def komari_alerts(self, event: AstrMessageEvent):
        """查看最近的一批告警记录。"""
        history = [item for item in self.state.get("alert_history", []) if isinstance(item, dict)]
        if not history:
            yield event.plain_result("暂无告警记录。")
            return
        lines = ["📜 最近告警"]
        for item in history[-10:]:
            ts = _num(item.get("time"))
            stamp = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts else "--"
            lines.append(f"▶ {stamp}\n{item.get('text', '')}")
        yield event.plain_result("\n".join(lines))

    @filter.command("komari_check")
    async def komari_check(self, event: AstrMessageEvent):
        """立即执行一次检查；告警会发往已绑定会话，并返回结果摘要。"""
        failed, info = await self._check_once(track_failure=False)
        if failed:
            yield event.plain_result(f"❌ Komari 检查失败：{info.get('error', '未知错误')}\n后台稍后会自动重试。")
            return
        summary = f"✅ Komari 检查完成：在线 {info.get('online', 0)} / 离线 {info.get('offline', 0)}"
        alert_count = info.get("alerts", 0)
        summary += f"\n已推送 {alert_count} 条告警。" if alert_count else "\n未触发新告警。"
        yield event.plain_result(summary)

    async def terminate(self):
        self._stop.set()
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


__all__ = ["KomariWatchPlugin", "KomariWatchConfig"]