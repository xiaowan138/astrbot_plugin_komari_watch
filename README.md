# Komari 监控推送插件

英文名称：**Komari Watch**（插件标识：`astrbot_plugin_komari_watch`）  
创建者：xiaowan

这是一个独立实现的 AstrBot 插件，借鉴了社区 `astrbot_plugin_komari_status` 的查询思路，但没有复用其代码。除节点查询外，本插件增加了适合长期运行的离线确认、高负载确认、告警冷却和恢复通知。

## 功能

- `/komari_status`（别名 `/kstatus`、`/komari`）：生成状态图片，展示节点在线状态、CPU、内存、磁盘、网络速率、负载与运行时间；可加一个或多个节点名只看指定节点，如 `/komari_status node01 node02`。卡片头部附"共/在线/离线"统计。
- `/komari_realtime`、`/komari_public`、`/komari_version`：查询实时数据（不经历史兜底）、公开站点信息和服务端版本。
- `/komari_history`（别名 `/khistory`、`/历史`）：查询历史资源趋势曲线（CPU / 内存 / 磁盘 / 上下行流量，均标注当前值与峰值，并标注数据起止时间），如 `/komari_history 6 nodeA`（小时数 1-24，可加节点名过滤）。
- `/komari_nodes`（别名 `/knodes`）：列出全部节点名称及过滤排除情况，便于填写 `filter_nodes` 与查询参数。
- `/komari_top [指标] [数量]`（别名 `/ktop`）：资源占用 Top 榜，如 `/komari_top mem 10`（指标 cpu/mem/disk/uptime，默认 cpu 前 5，仅统计在线节点）。
- `/komari_alerts`（别名 `/kalerts`）：查看最近的一批告警记录。
- `/komari_mute <分钟> [all]`：临时静默告警（默认 30 分钟；默认只静默当前会话，加 `all` 静默全部绑定会话）；`/komari_unmute [all]` 提前恢复。
- `/komari_help`（别名 `/khelp`）：全部命令总览。
- `/komari_bind`：把当前 OneBot 私聊或群聊绑定为告警接收目标。
- `/komari_unbind`：解除当前会话绑定。
- `/komari_check`：立即执行一次检查，返回在线/离线数量与告警摘要；检查失败时会明确提示原因。
- 后台轮询 `/api/nodes`，优先从 `/api/clients` WebSocket 读取实时指标；WebSocket 被反代禁用时自动使用最近一条负载记录兜底。
- 节点连续多个周期无心跳才告警；高负载连续多个周期超过阈值才告警；同类告警支持冷却和恢复通知。
- 同一周期内多个节点离线/恢复会合并成一条告警；Komari 整体不可达时检查会自动指数退避，降低无效重试与日志噪音。
- 面板连续多次检查失败会推送"不可达"告警，恢复时自动通知；节点重启（运行时间回退）会推送提醒；节点长期离线可配置每日提醒。

## 安装配置

在 AstrBot 插件配置页面填写 Komari 地址，私有站点再填写 Token。按需调整轮询间隔、离线确认周期、CPU/内存/磁盘阈值等。启动后在目标 OneBot 群里发送 `/komari_bind` 即可接收推送；绑定信息保存于 AstrBot 的 `data/plugin_data/astrbot_plugin_komari_watch/state.json`。

支持的可选配置：
- `filter_mode` / `filter_nodes`：节点过滤。`filter_mode` 为 `none`（默认，不过滤）、`allow`（只监控列表中节点）或 `deny`（排除列表中节点）；`filter_nodes` 填写节点名，多个用英文逗号分隔，支持子串匹配（匹配名称、主机名、id、uuid）。
- `status_report_interval`：定时状态推送间隔（小时），大于 0 时后台监控会按该间隔向绑定会话推送状态卡片，0 表示关闭。
- `status_report_time`：每天固定时刻（本地时间 `HH:MM`，如 `09:00`）推送状态卡片，留空不启用；可与间隔推送共存，先到先推。
- `prune_missing_cycles`：节点从服务器消失多少周期后清理其监控状态，防止 `state.json` 无限增长。
- `panel_fail_cycles`：面板连续多少次检查失败后推送"面板不可达"告警（恢复时自动通知），0 表示关闭。
- `notify_restart`：检测到节点运行时间回退（重启）时推送通知。
- `long_offline_remind_hours`：节点离线超过该小时数后每日提醒一次，0 表示关闭。
- `notify_recovery`：关闭后不再推送恢复通知（其余保持不变）。

建议先用 `/komari_check` 验证 API 与权限，再开启较短的轮询周期。Token 只保存在 AstrBot 配置中，不会写入日志。

## 开源说明

本项目遵循 MIT License，欢迎提交 Issue 和 Pull Request。仓库地址：<https://github.com/xiaowan138/astrbot_plugin_komari_watch>
