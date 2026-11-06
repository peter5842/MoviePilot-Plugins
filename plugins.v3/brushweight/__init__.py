"""
刷流动态配额：按各站分享率调整「站点刷流」(BrushFlow) 每个站点任务的保种配额和同时下载数。

- 分享率取 MoviePilot 站点数据里最新的 上传/下载；有上传没下载的站点按「无下载时分享率」计。
- 权重 = 1 / 分享率（分享率先限制在上下限之间）：分享率越低，配额越多。
- 配额 = 权重占比 × 刷流全局保种体积 × 超配倍数，取整到 10 GB，且不低于最小配额。
  配额之和有意超过全局上限，忙的站点可以用闲置站点的空间；总量仍受刷流插件的全局上限和动态删除约束。
- 同时下载数随占比：≥20% → 4，≥8% → 3，其余 2。
- 停用的任务、站点数据过期/报错/无流量的站点保持原设置，它们的配额先从总池里扣掉。
- 配额变化达到阈值或下载数变化才改写任务，避免日常小波动反复改配置。

改写通过刷流插件自己的任务接口完成，插件会立即生效并做校验。
"""
import datetime
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.plugins import _PluginBase
from app.schemas.types import MessageType
from app.sdk.config import settings
from app.sdk.logging import logger

lock = threading.Lock()


class BrushWeight(_PluginBase):
    """按分享率给站点刷流的各站任务分配保种配额。"""

    plugin_name = "刷流动态配额"
    plugin_desc = "按各站分享率动态调整站点刷流每个站点的保种配额和同时下载数，分享率越低配额越多。"
    plugin_icon = "brush-flow.png"
    plugin_version = "1.0.1"
    plugin_author = "peter5842"
    author_url = "https://github.com/peter5842"
    plugin_config_prefix = "brushweight_"
    plugin_order = 61
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _dry_run = True
    _notify = False
    _cron = "35 */6 * * *"
    _overcommit = 1.5
    _ratio_min = 1.0
    _ratio_max = 20.0
    _no_download_ratio = 3.0
    _min_quota = 300
    _min_change = 5.0
    _stale_days = 3
    _scheduler: Optional[BackgroundScheduler] = None

    # ------------------------------------------------------------------ 配置
    def init_plugin(self, config: Optional[dict] = None):
        self.stop_service()
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._dry_run = bool(config.get("dry_run"))
        self._notify = bool(config.get("notify"))
        self._cron = (config.get("cron") or "35 */6 * * *").strip()
        self._overcommit = self.__to_float(config.get("overcommit"), 1.5, 1.0)
        self._ratio_min = self.__to_float(config.get("ratio_min"), 1.0, 0.01)
        self._ratio_max = max(self.__to_float(config.get("ratio_max"), 20.0, 0.01), self._ratio_min)
        self._no_download_ratio = self.__to_float(config.get("no_download_ratio"), 3.0, 0.01)
        self._min_quota = int(self.__to_float(config.get("min_quota"), 300, 0))
        self._min_change = self.__to_float(config.get("min_change"), 5.0, 0)
        self._stale_days = int(self.__to_float(config.get("stale_days"), 3, 0))

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.run,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                name=self.plugin_name,
            )
            self._onlyonce = False
            config["onlyonce"] = False
            self.update_config(config)
            self._scheduler.start()

    @staticmethod
    def __to_float(value: Any, default: float, minimum: float) -> float:
        try:
            return max(float(value), minimum)
        except (TypeError, ValueError):
            return default

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except (TypeError, ValueError) as err:
            logger.error(f"{self.plugin_name} 执行周期无效：{err}")
            return []
        return [{
            "id": "BrushWeight",
            "name": self.plugin_name,
            "trigger": trigger,
            "func": self.run,
            "kwargs": {},
        }]

    def stop_service(self):
        scheduler, self._scheduler = self._scheduler, None
        try:
            if scheduler:
                scheduler.remove_all_jobs()
                if scheduler.running:
                    scheduler.shutdown(wait=False)
        except Exception as err:
            logger.warning(f"停止 {self.plugin_name} 一次性任务失败：{err}")

    # ------------------------------------------------------------------ 页面
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        def col(md: int, component: str, **props) -> dict:
            return {"component": "VCol", "props": {"cols": 12, "md": md},
                    "content": [{"component": component, "props": props}]}

        def num(md: int, model: str, label: str, hint: str) -> dict:
            return col(md, "VTextField", model=model, label=label, type="number",
                       hint=hint, **{"persistent-hint": True})

        return [{
            "component": "VForm",
            "content": [
                {"component": "VRow", "content": [
                    col(3, "VSwitch", model="enabled", label="启用插件"),
                    col(3, "VSwitch", model="dry_run", label="仅预览（不改刷流配置）"),
                    col(3, "VSwitch", model="notify", label="有调整时发送通知"),
                    col(3, "VSwitch", model="onlyonce", label="立即运行一次"),
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{
                        "component": "VCronField",
                        "props": {"model": "cron", "label": "执行周期", "placeholder": "35 */6 * * *"},
                    }]},
                    num(2, "overcommit", "超配倍数", "配额总和 = 全局保种体积 × 倍数"),
                    num(2, "min_quota", "最小配额 (GB)", "每站配额下限"),
                    num(2, "min_change", "改写阈值 (%)", "配额变化达到此比例才改写"),
                    num(2, "stale_days", "数据过期天数", "站点数据超过此天数未更新则不调整"),
                ]},
                {"component": "VRow", "content": [
                    num(4, "ratio_min", "分享率下限", "低于此值按此值计"),
                    num(4, "ratio_max", "分享率上限", "高于此值按此值计"),
                    num(4, "no_download_ratio", "无下载时分享率", "有上传但下载为 0 的站点按此计"),
                ]},
                {"component": "VRow", "content": [{
                    "component": "VCol", "props": {"cols": 12},
                    "content": [{"component": "VAlert", "props": {
                        "type": "info", "variant": "tonal",
                        "text": "需要已安装并启用「站点刷流」插件且设置了全局保种体积。只改写各站任务的「保种体积」和「同时下载数」，"
                                "停用的任务和站点数据异常的站点保持原样。",
                    }}],
                }]},
            ],
        }], {
            "enabled": False,
            "dry_run": True,
            "notify": False,
            "onlyonce": False,
            "cron": "35 */6 * * *",
            "overcommit": 1.5,
            "min_quota": 300,
            "min_change": 5,
            "stale_days": 3,
            "ratio_min": 1,
            "ratio_max": 20,
            "no_download_ratio": 3,
        }

    def get_page(self) -> List[dict]:
        last = self.get_data("last_run") or {}
        if not last:
            return [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "尚未运行。"}}]
        rows = last.get("rows") or []
        return [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": last.get("summary", "")}},
            {"component": "VTable", "props": {"hover": True, "density": "compact", "class": "mt-4"}, "content": [
                {"component": "thead", "content": [{"component": "tr", "content": [
                    {"component": "th", "text": h} for h in
                    ("站点", "分享率", "权重", "配额 (GB)", "同时下载", "结果")]}]},
                {"component": "tbody", "content": [{"component": "tr", "content": [
                    {"component": "td", "text": str(v)} for v in (
                        r["name"], r["ratio"], r["weight"], r["quota"], r["dl"], r["result"])]} for r in rows]},
            ]},
        ]

    # ------------------------------------------------------------------ 主流程
    def run(self):
        if not lock.acquire(blocking=False):
            logger.warning(f"{self.plugin_name} 上一次运行尚未结束，跳过")
            return
        try:
            self.__run()
        except Exception as err:
            logger.error(f"{self.plugin_name} 运行出错：{err}", exc_info=True)
        finally:
            lock.release()

    def __run(self):
        status = self.__brush("/status")
        global_size = float(status.get("global_disksize") or 0)
        pool = global_size * self._overcommit
        if not pool:
            logger.error(f"{self.plugin_name}：刷流插件未设置全局保种体积，无法分配")
            return

        userdata = self.__mp_api("GET", "/site/userdata/latest", {"page": 1, "count": 200})
        if not isinstance(userdata, list):
            raise RuntimeError(f"读取站点数据失败：{userdata}")
        latest = {row["domain"]: row for row in userdata if isinstance(row, dict) and row.get("domain")}

        tasks = []
        for item in status.get("tasks") or []:
            task = (self.__brush(f"/tasks/{item['id']}") or {}).get("task")
            if not task:
                continue
            if task.get("enabled"):
                ratio, note = self.__site_ratio(task.get("site_id"), latest)
            else:
                ratio, note = None, "任务停用"
            tasks.append({"task": task, "ratio": ratio, "note": note})

        fixed = sum(float(t["task"].get("disksize") or 0) for t in tasks if t["ratio"] is None)
        weights = {t["task"]["id"]: 1 / t["ratio"] for t in tasks if t["ratio"] is not None}
        total_weight = sum(weights.values())
        free_pool = max(pool - fixed, 0)

        mode = "预览" if self._dry_run else "执行"
        summary = (f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}（{mode}）：全局 {global_size:.0f} GB × {self._overcommit} "
                   f"= {pool:.0f} GB，未参与加权的任务占 {fixed:.0f} GB")
        logger.info(f"[{mode}] {summary}")
        rows, changes = [], []
        for t in tasks:
            task = t["task"]
            old_quota = float(task.get("disksize") or 0)
            old_dl = task.get("maxdlcount")
            if t["ratio"] is None:
                rows.append({"name": task.get("name"), "ratio": t["note"], "weight": "—",
                             "quota": f"{old_quota:.0f}", "dl": old_dl, "result": "保持"})
                logger.info(f"  {task.get('name')} 保持 {old_quota:.0f} GB / 下载 {old_dl}（{t['note']}）")
                continue
            share = weights[task["id"]] / total_weight
            quota = max(round(share * free_pool / 10) * 10, self._min_quota)
            dl = 4 if quota / pool >= 0.20 else 3 if quota / pool >= 0.08 else 2
            moved = abs(quota - old_quota) / old_quota if old_quota else 1
            update = moved * 100 >= self._min_change or dl != old_dl
            result = "不变"
            if update:
                if self._dry_run:
                    result = "需调整"
                else:
                    task.update({"disksize": quota, "maxdlcount": dl})
                    self.__brush(f"/tasks/{task['id']}", "PUT", task)
                    result = "已调整"
                    changes.append(f"{task.get('name')}：{old_quota:.0f}→{quota} GB，下载 {old_dl}→{dl}")
            rows.append({"name": task.get("name"), "ratio": t["note"], "weight": f"{share * 100:.1f}%",
                         "quota": f"{old_quota:.0f} → {quota}", "dl": f"{old_dl} → {dl}", "result": result})
            logger.info(f"  {task.get('name')} 分享率 {t['note']} 权重 {share * 100:.1f}% "
                        f"配额 {old_quota:.0f} → {quota} GB 下载 {old_dl} → {dl} {result}")

        summary += f"；本次调整 {len(changes)} 个任务" if not self._dry_run else "；预览模式未改写"
        logger.info(f"[{mode}] 本次调整 {len(changes)} 个任务")
        self.save_data("last_run", {"summary": summary, "rows": rows})
        if changes and self._notify:
            self.post_message(mtype=MessageType.Plugin, title="【刷流动态配额】", text="\n".join(changes))

    def __site_ratio(self, site_id: Any, latest: Dict[str, dict]) -> Tuple[Optional[float], str]:
        """返回 (用于加权的分享率 或 None, 说明)。"""
        site = self.__mp_api("GET", f"/site/{site_id}") if site_id else None
        domain = (site or {}).get("domain")
        if not domain:
            return None, "站点不存在"
        stat = latest.get(domain)
        if not stat:
            return None, "无站点数据"
        if stat.get("err_msg"):
            return None, f"数据报错：{str(stat['err_msg'])[:30]}"
        day = stat.get("updated_day")
        if not day or day < (datetime.date.today() - datetime.timedelta(days=self._stale_days)).isoformat():
            return None, f"数据过期（{day}）"
        upload, download = float(stat.get("upload") or 0), float(stat.get("download") or 0)
        if not upload and not download:
            return None, "无流量"
        if not download:
            return self._no_download_ratio, "无下载"
        ratio = upload / download
        return min(max(ratio, self._ratio_min), self._ratio_max), f"{ratio:.2f}"

    # ------------------------------------------------------------------ 接口
    def __brush(self, path: str, method: str = "GET", body: Any = None) -> Any:
        resp = self.__mp_api(method, f"/plugin/BrushFlow{path}", body=body)
        if not isinstance(resp, dict) or not resp.get("success"):
            raise RuntimeError(f"刷流插件接口 {method} {path} 失败：{(resp or {}).get('message') if isinstance(resp, dict) else resp}")
        return resp.get("data")

    @staticmethod
    def __mp_api(method: str, path: str, query: Optional[dict] = None, body: Any = None) -> Any:
        """调用 MoviePilot 自己的接口（本机回环，使用 API 令牌）。"""
        url = f"http://127.0.0.1:{settings.PORT}/api/v1{path}"
        if query:
            url += f"?{urllib.parse.urlencode(query)}"
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "X-API-KEY": str(settings.API_TOKEN), "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read() or b"null")
        except urllib.error.HTTPError as err:
            return {"success": False, "message": f"HTTP {err.code} {err.read()[:200]!r}"}
