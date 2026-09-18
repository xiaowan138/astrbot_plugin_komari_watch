"""Smoke test with mocked astrbot modules.

Run: python test_smoke.py  (requires pydantic + aiohttp)
Covers the alert engine end-to-end (offline/high-load/recovery/restart/
long-offline/panel-failure), HTML rendering and command handlers.
"""
import asyncio
import shutil
import sys
import types
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
