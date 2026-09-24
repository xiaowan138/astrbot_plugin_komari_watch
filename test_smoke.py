"""Smoke test with mocked astrbot modules.

Run: python test_smoke.py  (requires pydantic + aiohttp)
Covers the alert engine end-to-end (offline/high-load/recovery/restart/
long-offline/panel-failure), HTML rendering and command handlers.
"""
import asyncio
import logging
import shutil
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
shutil.rmtree(ROOT / "data", ignore_errors=True)


def _mock_astrbot():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event_mod = types.ModuleType("astrbot.api.event")
    comp_mod = types.ModuleType("astrbot.api.message_components")
    star_mod = types.ModuleType("astrbot.api.star")

    class AstrMessageEvent:
        pass

    class MessageChain:
        def __init__(self, items=None):
            self.items = list(items or [])

        def message(self, text):
            self.items.append(text)
            return self

    class _Filter:
        @staticmethod
        def command(*args, **kwargs):
            def deco(func):
                return func
            return deco

    class Star:
        def __init__(self, context=None):
            self.context = context

    class Context:
        pass

    class Image:
        @staticmethod
        def fromURL(url):
            return ("image", url)

    event_mod.AstrMessageEvent = AstrMessageEvent
    event_mod.MessageChain = MessageChain
    event_mod.filter = _Filter()
    comp_mod.Image = Image
    star_mod.Context = Context
    star_mod.Star = Star
    star_mod.register = lambda *a, **k: (lambda cls: cls)

    astrbot.api = api
    api.event = event_mod
    api.message_components = comp_mod
    api.star = star_mod
    sys.modules.update({
        "astrbot": astrbot, "astrbot.api": api,
        "astrbot.api.event": event_mod,
        "astrbot.api.message_components": comp_mod,
        "astrbot.api.star": star_mod,
    })


_mock_astrbot()
sys.path.insert(0, str(ROOT))
import main as m  # noqa: E402

PASS, FAIL = [], []
SENT: list[str] = []


def check(name, cond):
    (PASS if cond else FAIL).append(name)


class FakeEvent:
    unified_msg_origin = "test:origin"

    @staticmethod
    def plain_result(text):
        return ("plain", text)

    @staticmethod
    def chain_result(chain):
        return ("chain", chain)


async def run():
    cfg = m.KomariWatchConfig(komari_url="https://x.example.com", image_output=False,
                              long_offline_remind_hours=1, panel_fail_cycles=3)
    plugin = m.KomariWatchPlugin(None, cfg)

    # ---- pure helpers ----
    check("config defaults", cfg.cpu_threshold == 90 and cfg.notify_restart is True)
    check("parse_time numeric", m._parse_time(1757000000) is not None and m._parse_time(1757000000000) is not None)
    check("parse_time iso", m._parse_time("2026-09-05T08:00:00Z") is not None)
    check("parse_time junk", m._parse_time("abc") is None and m._parse_time(None) is None)
    ws = m.KomariWatchPlugin._parse_ws_clients({"data": {"data": {"u1": '{"cpu": 10}', "u2": {"ram": 20}}, "online": ["u1"]}})
    check("ws clients nested", len(ws) == 1 and ws[0]["uuid"] == "u1" and ws[0]["cpu"] == 10)
    check("ws clients list", m.KomariWatchPlugin._parse_ws_clients({"data": [{"cpu": 1}]}) == [{"cpu": 1}])
    check("ws clients flat", m.KomariWatchPlugin._parse_ws_clients({"u9": {"cpu": 1}})[0]["uuid"] == "u9")
    check("metric cpu", m._metric({"cpu_usage": 0.5}, "cpu") == 50.0)
    check("metric ram used/total", m._metric({"ram": {"used": 2, "total": 4}}, "memory") == 50.0)

    # ---- rendering ----
    nodes = [
        {"uuid": "u1", "name": "n1", "is_online": True, "cpu_usage": 95, "memory_usage": 50, "disk_usage": 40,
         "network": {"up": 1024, "down": 2048}, "load": {"load1": 0.5}, "uptime": 99999, "updated_at": "2026-09-05T07:59:00Z"},
        {"uuid": "u2", "name": "n2<script>", "is_online": False},
    ]
    report = plugin._report_html(nodes)
    check("report html stats", "共 2 节点" in report and "在线 1" in report and "离线 1" in report)
    check("report html offline label", "状态：离线" in report)
    check("report html escape", "<script>" not in report)
    series = [{"cpu": 10, "ram": 20, "disk": 30, "net_in": 1024, "net_out": 512}] * 3
    html_out = plugin._history_html({"u1": {"node": nodes[0], "series": series}}, 6)
    check("history html traffic", "流量" in html_out and "polyline" in html_out)
    check("history html offline dot", plugin._history_html({"u2": {"node": nodes[1], "series": series}}, 6).count("dot offline") == 1)
    chart = m.KomariWatchPlugin._mini_chart("CPU", [10, 80, 40], "#f00", 6)
    check("mini chart suffix", "当前 40.0%" in chart and "峰值 80.0%" in chart)
    traffic = m.KomariWatchPlugin._traffic_chart(series, 6)
    check("traffic chart values", "↑" in traffic and "峰值" in traffic)
    text = plugin._history_text({"u1": {"node": nodes[0], "series": series}}, 6)
    check("history text traffic", "流量 ↑" in text)

    # 1.5.0: history sorting and time span labels
    async def fake_get_json(endpoint):
        return {"data": {"records": [
            {"time": 1757000300, "cpu": 30, "ram_percent": 30, "disk_percent": 30},
            {"time": 1757000100, "cpu": 10, "ram_percent": 10, "disk_percent": 10},
            {"time": 1757000200, "cpu": 20, "ram_percent": 20, "disk_percent": 20},
        ]}}, None
    plugin._get_json = fake_get_json
    sorted_series = await plugin._history_series({"uuid": "u1"}, 1)
    check("history series sorted", [p["cpu"] for p in sorted_series] == [10.0, 20.0, 30.0])
    html_span = plugin._history_html({"u1": {"node": nodes[0], "series": sorted_series}}, 6)
    check("history html span", "→" in html_span)

    # ---- alert engine ----
    global SENT
    sent = SENT

    async def fake_send(text):
        sent.append(text)

    plugin._send = fake_send
    plugin._stop.set()
    plugin._start_monitor = lambda: None
    plugin.state["targets"] = ["test:origin"]

    live_nodes = [
        {"uuid": "u1", "name": "node1", "is_online": True, "cpu_usage": 95, "memory_usage": 50,
         "disk_usage": 40, "uptime": 99999, "updated_at": "2026-09-05T07:59:00Z"},
        {"uuid": "u2", "name": "node2", "is_online": False},
    ]

    async def ok_snapshot():
        return live_nodes, None

    async def err_snapshot():
        return [], "连接 Komari 失败：boom"

    plugin._snapshot = ok_snapshot
    await plugin._check_once()
    await plugin._check_once()
    check("offline+high alerts", any("离线告警" in s and "node2" in s for s in sent)
          and any("高负载告警" in s and "CPU 95.0%" in s for s in sent))
    check("high details only exceeded", all("内存" not in s for s in sent if "高负载" in s))

    # node restart
    live_nodes[0]["uptime"] = 120
    await plugin._check_once()
    check("restart alert", any("节点重启" in s and "此前已运行" in s for s in sent))

    # recovery
    live_nodes[1]["is_online"] = True
    await plugin._check_once()
    check("recovery alert", any("节点恢复" in s for s in sent))

    # long-offline daily reminder (alert re-trigger is blocked by cooldown,
    # so build the active state directly to exercise the reminder branch)
    live_nodes[1]["is_online"] = False
    await plugin._check_once()
    rec = plugin.state["nodes"]["u2"]
    rec["active"]["offline"] = True
    rec["offline_started"] -= 7200
    await plugin._check_once()
    check("long offline remind", any("仍离线" in s and "已离线" in s for s in sent))

    # panel failure and recovery
    plugin._snapshot = err_snapshot
    for _ in range(3):
        await plugin._check_once()
    check("panel down alert", any("面板不可达" in s for s in sent))
    plugin._snapshot = ok_snapshot
    await plugin._check_once()
    check("panel recovery alert", any("面板已恢复" in s for s in sent))

    # 1.5.0: _check_once returns a summary tuple
    failed, info = await plugin._check_once()
    check("check once summary", failed is False and info.get("online") == 1
          and info.get("offline") == 1 and "alerts" in info)

    # ---- commands ----
    plugin._snapshot = ok_snapshot
    results = [r async for r in plugin.komari_top(FakeEvent(), "mem", "3")]
    check("top command", results and results[0][1].startswith("🏆") and "内存" in results[0][1])
    results = [r async for r in plugin.komari_status(FakeEvent(), "不存在的节点")]
    check("status no match", any("没有匹配" in r[1] for r in results))
    results = [r async for r in plugin.komari_help(FakeEvent())]
    check("help lists top", any("/komari_top" in r[1] for r in results))
    results = [r async for r in plugin.komari_check(FakeEvent())]
    check("check command summary", any("检查完成" in r[1] for r in results))
    plugin._snapshot = err_snapshot
    results = [r async for r in plugin.komari_check(FakeEvent())]
    check("check command failure", any("检查失败" in r[1] for r in results))
    plugin._snapshot = ok_snapshot
    results = [r async for r in plugin.komari_top(FakeEvent(), "uptime")]
    check("top uptime", any("运行时长" in r[1] for r in results))
    sel = plugin._select(live_nodes, ("node1", "node2"))
    check("select multi keyword", len(sel) == 2)

    plugin.state["muted"] = {}
    plugin.state["targets"] = []
    results = [r async for r in plugin.komari_mute(FakeEvent(), "30", "all")]
    check("mute all no targets", any("没有绑定任何会话" in r[1] for r in results))
    plugin.state["targets"] = []
    sent.clear()
    await plugin._check_once()
    check("send respects empty targets", sent == [])
    plugin.state["muted"] = {"test:origin": 9999999999}
    sent.clear()
    await plugin._check_once()
    check("send respects mute", sent == [])

    # ---- 1.6.0: 指标换算、WS 帧判定、分组/标签筛选 ----
    check("metric fraction default", m._metric({"cpu_usage": 0.5}, "cpu") == 50.0)
    check("metric fraction disabled", m._metric({"cpu_usage": 0.5}, "cpu", frozenset()) == 0.5)
    check("metric gpu numeric", m._metric({"gpu": 42}, "gpu") == 42.0)
    check("metric gpu nested", m._metric({"gpu": {"usage": 30}}, "gpu") == 30.0)
    check("fraction set from config", plugin._fractions == frozenset({"cpu", "memory", "disk"}))
    check("fraction parse custom", m.KomariWatchPlugin._parse_fractions(" cpu , gpu ") == frozenset({"cpu", "gpu"}))
    check("fraction parse empty", m.KomariWatchPlugin._parse_fractions("") == frozenset())

    check("ws ack frame rejected", not m.KomariWatchPlugin._is_clients_payload({"status": "ok"}))
    check("ws clients frame accepted", m.KomariWatchPlugin._is_clients_payload({"data": {"online": [], "data": {}}}))
    check("ws list frame accepted", m.KomariWatchPlugin._is_clients_payload([{"uuid": "u1"}]))
    check("ws flat frame accepted", m.KomariWatchPlugin._is_clients_payload({"u1": {"cpu": 1}}))

    tagged = [{"uuid": "g1", "name": "tokyo-1", "group": "Tokyo", "tags": "production;ssd"}]
    check("select by group", plugin._select(tagged, ("group:tokyo",)) == tagged)
    check("select by cn group", plugin._select(tagged, ("分组:Tokyo",)) == tagged)
    check("select by tag", plugin._select(tagged, ("tag:ssd",)) == tagged)
    check("select group miss", plugin._select(tagged, ("group:osaka",)) == [])
    check("select plain keyword", plugin._select(tagged, ("tokyo-1",)) == tagged)
    hint = plugin._keyword_hint(("a", "b"))
    check("keyword hint lists all", "「a」" in hint and "「b」" in hint)

    # ---- 1.6.0: 流量与到期 ----
    check("traffic sum mode", m.KomariWatchPlugin._traffic({"traffic_limit": 1000, "net_total_up": 100, "net_total_down": 300}) == (400.0, 1000.0))
    check("traffic max mode", m.KomariWatchPlugin._traffic(
        {"traffic_limit": 1000, "traffic_limit_type": "max", "net_total_up": 100, "net_total_down": 300}) == (300.0, 1000.0))
    check("traffic explicit used", m.KomariWatchPlugin._traffic({"traffic_limit": 1000, "traffic_used": 250}) == (250.0, 1000.0))
    check("traffic no counters", m.KomariWatchPlugin._traffic({"traffic_limit": 1000}) == (None, 1000.0))
    check("traffic fmt", "流量" in plugin._fmt_traffic({"traffic_limit": 1000, "net_total_up": 100, "net_total_down": 300}))
    check("traffic fmt hidden without limit", plugin._fmt_traffic({"net_total_up": 1}) == "")
    check("expire fmt none", plugin._fmt_expire({"expired_at": None}) == "")
    # 取半天余量，避免系统时钟粒度让 2 天整落到 int() 边界上。
    soon = (datetime.now(timezone.utc) + timedelta(days=2, hours=12)).isoformat()
    check("expire fmt soon", "剩余 2 天" in plugin._fmt_expire({"expired_at": soon}))
    past = (datetime.now(timezone.utc) - timedelta(days=3, hours=12)).isoformat()
    check("expire fmt past", "已到期 3 天" in plugin._fmt_expire({"expired_at": past}))
    check("group fmt", plugin._fmt_group({"group": "Tokyo", "tags": "a;b"}) == "Tokyo / a,b")

    # ---- 1.6.0: GPU 展示 ----
    gpu_node = {"uuid": "u1", "name": "gpu1", "is_online": True, "gpu": 55, "cpu_usage": 10, "memory_usage": 20, "disk_usage": 30}
    check("report gpu row", "GPU" in plugin._report_html([gpu_node]) and "55.0%" in plugin._report_html([gpu_node]))
    check("report no gpu row", "GPU" not in plugin._report_html([{"uuid": "x", "name": "nogpu", "is_online": True, "cpu_usage": 10}]))
    gpu_series = [{"cpu": 10, "ram": 20, "disk": 30, "gpu": 40, "net_in": 1, "net_out": 2}] * 3
    check("history gpu chart", "GPU" in plugin._history_html({"u1": {"node": gpu_node, "series": gpu_series}}, 6))
    check("history gpu text", "GPU 40.0%" in plugin._history_text({"u1": {"node": gpu_node, "series": gpu_series}}, 6))

    # ---- 1.6.0: 记录接口回退 ----
    calls = []

    async def flaky_get_json(endpoint):
        calls.append(endpoint)
        if "load_type" in endpoint:
            return None, "Komari API 返回 HTTP 400"
        return {"data": {"records": [{"time": 1757000000, "cpu": 10}]}}, None

    plugin._get_json = flaky_get_json
    fallback_records = await plugin._records("u1", 1)
    check("records fallback retried", len(fallback_records) == 1 and len(calls) == 2 and "load_type" not in calls[-1])

    # ---- 1.6.0: ping 统计与命令 ----
    ping_records = [{"task_id": 1, "value": 20.0}, {"task_id": 1, "value": -1}, {"task_id": 1, "value": 40.0}, {"task_id": 2, "value": 5.0}]
    grouped = m.KomariWatchPlugin._ping_stats(ping_records)
    check("ping stats grouped", grouped["1"] == [20.0, -1.0, 40.0] and len(grouped["2"]) == 1)
    ping_lines = m.KomariWatchPlugin._ping_lines(grouped)
    check("ping lines loss", any("丢包 33.3%" in line for line in ping_lines))
    check("ping lines avg", any("平均 30.0ms" in line for line in ping_lines))

    async def fake_ping_records(uuid, hours):
        return ping_records if uuid == "u1" else []

    plugin._ping_records = fake_ping_records
    plugin._snapshot = ok_snapshot
    results = [r async for r in plugin.komari_ping(FakeEvent(), "1")]
    check("ping command output", bool(results) and "延迟" in results[0][1] and "丢包" in results[0][1])

    async def fake_recent(node):
        return ({"time": 1757000000, "cpu": 12.5, "ram": 2, "ram_total": 4, "disk": 1, "disk_total": 4,
                 "load": 0.5, "temp": 45.0, "process": 100, "connections": 20,
                 "net_in": 1024, "net_out": 2048, "net_total_up": 10000, "net_total_down": 20000}, None)

    plugin._recent_record = fake_recent
    results = [r async for r in plugin.komari_recent(FakeEvent(), "node1")]
    check("recent command detail", bool(results) and "最近上报" in results[0][1]
          and "温度 45.0℃" in results[0][1] and "累计 ↑" in results[0][1])

    # ---- 1.6.0: 到期与流量告警 ----
    plugin.config.expire_remind_days = 3
    plugin.config.traffic_alert_percent = 60
    plugin.state["muted"] = {}
    plugin.state["targets"] = ["test:origin"]
    plugin.state["nodes"] = {}
    limit_nodes = [{"uuid": "e1", "name": "exp1", "is_online": True, "expired_at": soon, "traffic_limit": 1000,
                    "traffic_limit_type": "sum", "net_total_up": 400, "net_total_down": 400}]

    async def limit_snapshot():
        return limit_nodes, None

    plugin._snapshot = limit_snapshot
    sent.clear()
    await plugin._check_once()
    check("expire alert", any("即将到期" in s for s in sent))
    check("traffic alert", any("流量告警" in s and "80%" in s for s in sent))
    sent.clear()
    await plugin._check_once()
    check("expire alert once per day", not any("即将到期" in s for s in sent))
    limit_nodes[0]["net_total_up"] = 0
    limit_nodes[0]["net_total_down"] = 0
    sent.clear()
    await plugin._check_once()
    check("traffic recovery", any("流量恢复" in s for s in sent))
    plugin.config.traffic_alert_percent = 0
    plugin.config.expire_remind_days = 0

    # ---- 1.6.0: 离线时长起点与高负载状态清理 ----
    plugin.state["nodes"] = {}
    plugin.config.alert_cooldown = 0
    down_nodes = [{"uuid": "d1", "name": "down", "is_online": False}]

    async def down_snapshot():
        return down_nodes, None

    plugin._snapshot = down_snapshot
    sent.clear()
    await plugin._check_once()
    started = plugin.state["nodes"]["d1"]["offline_started"]
    await plugin._check_once()
    check("offline start kept", plugin.state["nodes"]["d1"]["offline_started"] == started)
    check("offline alert fired", any("离线告警" in s for s in sent))
    plugin.state["nodes"]["d1"]["offline_started"] = started - 3600
    down_nodes[0]["is_online"] = True
    sent.clear()
    await plugin._check_once()
    check("recovery duration from offline start", any("离线时长：1时" in s for s in sent))
    check("offline start cleared", "offline_started" not in plugin.state["nodes"]["d1"])

    plugin.state["nodes"] = {}
    hot_nodes = [{"uuid": "h1", "name": "hot", "is_online": True, "cpu_usage": 99}]

    async def hot_snapshot():
        return hot_nodes, None

    plugin._snapshot = hot_snapshot
    plugin.config.high_load_cycles = 1
    sent.clear()
    await plugin._check_once()
    check("high state active", plugin.state["nodes"]["h1"]["active"]["high"] is True)
    hot_nodes[0]["is_online"] = False
    await plugin._check_once()
    check("high state reset on offline", plugin.state["nodes"]["h1"]["active"]["high"] is False)
    hot_nodes[0]["is_online"] = True
    hot_nodes[0]["cpu_usage"] = 5
    sent.clear()
    await plugin._check_once()
    check("no false load recovery", not any("负载恢复" in s for s in sent))
    plugin.config.high_load_cycles = 2

    # ---- 1.6.0: 监控循环异常后仍存活 ----
    logging.disable(logging.CRITICAL)
    plugin._stop.clear()
    plugin.state["targets"] = ["test:origin"]
    boom = {"count": 0}

    async def boom_check(track_failure=True):
        boom["count"] += 1
        raise RuntimeError("boom")

    plugin._check_once = boom_check
    plugin._monitor_task = None
    # 前面为隔离告警引擎把 _start_monitor 换成了空函数，这里恢复真实实现。
    plugin.__dict__.pop("_start_monitor", None)
    plugin._start_monitor()
    await asyncio.sleep(0.05)
    check("monitor loop survives error", boom["count"] >= 1 and plugin._monitor_task is not None
          and not plugin._monitor_task.done())
    plugin._stop.set()
    plugin._monitor_task.cancel()
    try:
        await plugin._monitor_task
    except asyncio.CancelledError:
        pass
    logging.disable(logging.NOTSET)

    # ---- session creation (regression for the name-collision bug) ----
    session = await plugin._get_session()
    check("get session callable", session is not None and not session.closed)
    await session.close()
    check("no method/attr collision", plugin._session is None or isinstance(plugin._session, object))


asyncio.run(run())
print(f"PASS {len(PASS)} / FAIL {len(FAIL)}")
if FAIL:
    print("FAILED:", FAIL)
    print("SENT MESSAGES:")
    for s in SENT:
        print("  --", s.replace("\n", " | "))
    sys.exit(1)
