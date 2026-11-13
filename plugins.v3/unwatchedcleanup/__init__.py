"""
媒体库无人观看清理：删除 Plex 中长期无人观看的电影和剧集。

新命中的条目先发通知预告，宽限期内仍无人观看才删除；期间播放其中任意内容即可保留。
有整理记录的文件走 MoviePilot 自己的删除（媒体库文件 + 源文件 + 下载器任务），
其余文件直接删除，并一并删除硬链接到这些文件的 qBittorrent 任务。
"""
import datetime
import fcntl
import uuid
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.plugins import _PluginBase
from app.schemas.types import MessageType
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.queries import list_subscriptions, list_transfer_history
from app.sdk.services import DownloaderHelper, MediaServerHelper

from .orphan_files import scan as scan_orphan_files, purge_source

VIDEO_EXT = {".mkv", ".mp4", ".ts", ".m2ts", ".avi", ".mov", ".wmv", ".iso", ".rmvb", ".flv", ".webm"}
HISTORY_KEEP = 300

lock = threading.Lock()


def human(n: float) -> str:
    """把字节数格式化为带单位的字符串。"""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def fmt_date(ts: Optional[float]) -> str:
    """时间戳转日期字符串，缺失时返回空串。"""
    return datetime.date.fromtimestamp(ts).isoformat() if ts else ""


class UnwatchedCleanup(_PluginBase):
    """按 Plex 观看记录清理长期无人观看的媒体库内容。"""

    plugin_name = "媒体库无人观看清理"
    plugin_desc = "按观看记录清理媒体库，并自动清理媒体库和下载器均无引用的下载残留。"
    plugin_icon = "clean.png"
    plugin_version = "1.1.0"
    plugin_author = "peter5842"
    author_url = "https://github.com/peter5842"
    plugin_config_prefix = "unwatchedcleanup_"
    plugin_order = 60
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _dry_run = True
    _notify = True
    _cron = "30 6 * * *"
    _days = 180
    _grace_days = 7
    _limit = 0
    _library_dirs: List[str] = []
    _plex_path = "/data/"
    _local_path = "/media/"
    _skip_category = "刷流"
    _mediaservers: List[str] = []
    _scheduler: Optional[BackgroundScheduler] = None
    _orphan_enabled = False
    _orphan_dry_run = True
    _orphan_cron = "0 5 * * *"
    _orphan_download_dirs = ""
    _orphan_min_age_days = 1
    _orphan_max_actions = 10

    # ------------------------------------------------------------------ 配置
    def init_plugin(self, config: Optional[dict] = None):
        """应用配置；“立即运行一次”用插件自己的一次性调度器，周期任务交给宿主。"""
        self.stop_service()
        self._stop = threading.Event()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._dry_run = config.get("dry_run", True) is not False
        self._notify = bool(config.get("notify"))
        self._cron = (config.get("cron") or "30 6 * * *").strip()
        self._days = self.__to_int(config.get("days"), 180, minimum=1)
        self._grace_days = self.__to_int(config.get("grace_days"), 7, minimum=0)
        self._limit = self.__to_int(config.get("limit"), 0, minimum=0)
        self._library_dirs = [
            d.strip().rstrip("/") + "/"
            for d in str(config.get("library_dirs") or "").replace(",", "\n").splitlines()
            if d.strip()
        ]
        self._plex_path = (config.get("plex_path") or "").strip()
        self._local_path = (config.get("local_path") or "").strip()
        self._skip_category = (config.get("skip_category") or "").strip()
        self._mediaservers = config.get("mediaservers") or []
        self._orphan_enabled = config.get("orphan_enabled") is True
        self._orphan_dry_run = config.get("orphan_dry_run", True) is not False
        self._orphan_cron = str(config.get("orphan_cron") or "0 5 * * *").strip()
        self._orphan_download_dirs = str(config.get("orphan_download_dirs") or "").strip()
        self._orphan_min_age_days = self.__to_int(config.get("orphan_min_age_days"), 1, minimum=1)
        self._orphan_max_actions = min(100, self.__to_int(config.get("orphan_max_actions"), 10, minimum=1))

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
    def __to_int(value: Any, default: int, minimum: int = 0) -> int:
        try:
            return max(int(value), minimum)
        except (TypeError, ValueError):
            return default

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """孤立文件状态和只读预览；执行入口使用宿主后台服务。"""
        return [
            {"path": "/orphans/status", "endpoint": self.orphan_status, "methods": ["GET"], "auth": "apikey", "summary": "孤立下载清理状态"},
            {"path": "/orphans/preview", "endpoint": self.preview_orphans, "methods": ["POST"], "auth": "apikey", "summary": "只读预览孤立下载文件"},
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """两类清理分别调度，均由 MoviePilot 宿主管理。"""
        if not self._enabled:
            return []
        services = []
        entries = [("UnwatchedCleanup", self.plugin_name, self._cron, self.run)]
        if self._orphan_enabled:
            entries.append(("OrphanDownloads", "孤立下载文件清理", self._orphan_cron, self.run_orphans))
        for service_id, name, cron, func in entries:
            try:
                trigger = CronTrigger.from_crontab(cron, timezone=settings.TZ)
            except (TypeError, ValueError):
                logger.error(f"{name} 执行周期无效，未添加定时任务")
                continue
            services.append({"id": service_id, "name": name, "trigger": trigger, "func": func,
                             "kwargs": {"max_instances": 1, "coalesce": True}})
        return services

    def stop_service(self):
        if hasattr(self, "_stop"):
            self._stop.set()
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

        try:
            plex_items = [{"title": c.name, "value": c.name}
                          for c in MediaServerHelper().get_configs().values() if c.type == "plex"]
        except Exception:
            plex_items = []

        return [{
            "component": "VForm",
            "content": [
                {"component": "VRow", "content": [
                    col(3, "VSwitch", model="enabled", label="启用插件"),
                    col(3, "VSwitch", model="dry_run", label="仅预览（不预告不删除）",
                        hint="只列出命中的条目，不发通知、不删除", **{"persistent-hint": True}),
                    col(3, "VSwitch", model="notify", label="发送通知",
                        hint="预告与删除结果以插件消息发送", **{"persistent-hint": True}),
                    col(3, "VSwitch", model="onlyonce", label="立即运行一次"),
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{
                        "component": "VCronField",
                        "props": {"model": "cron", "label": "执行周期", "placeholder": "30 6 * * *"},
                    }]},
                    col(2, "VTextField", model="days", label="无人观看天数", type="number"),
                    col(2, "VTextField", model="grace_days", label="宽限天数", type="number",
                        hint="预告后多少天仍无人看才删除", **{"persistent-hint": True}),
                    col(2, "VTextField", model="limit", label="单次最多删除", type="number",
                        hint="0 表示不限", **{"persistent-hint": True}),
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{
                        "component": "VSelect",
                        "props": {"model": "mediaservers", "label": "Plex 服务器", "multiple": True,
                                  "chips": True, "clearable": True, "items": plex_items,
                                  "hint": "留空用第一个已连接的 Plex", "persistent-hint": True},
                    }]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{
                        "component": "VTextarea",
                        "props": {"model": "library_dirs", "label": "媒体库目录（每行一个）", "rows": 2,
                                  "hint": "只清理这些目录下的文件（MoviePilot 容器内路径）",
                                  "persistent-hint": True},
                    }]},
                    col(2, "VTextField", model="plex_path", label="Plex 路径前缀",
                        hint="Plex 看到的媒体根目录", **{"persistent-hint": True}),
                    col(2, "VTextField", model="local_path", label="本地路径前缀",
                        hint="同一目录在 MoviePilot 中的路径", **{"persistent-hint": True}),
                    col(4, "VTextField", model="skip_category", label="跳过的下载器分类",
                        hint="匹配硬链接种子时跳过这个 qB 分类（如刷流）", **{"persistent-hint": True}),
                ]},
                {"component": "VRow", "content": [
                    col(3, "VSwitch", model="orphan_enabled", label="清理孤立下载文件"),
                    col(3, "VSwitch", model="orphan_dry_run", label="孤立文件仅预览",
                        hint="全局仅预览开关也会阻止孤立文件删除", **{"persistent-hint": True}),
                    col(3, "VCronField", model="orphan_cron", label="孤立文件清理周期"),
                    col(3, "VTextField", model="orphan_min_age_days", label="源文件静置天数", type="number", min=1),
                ]},
                {"component": "VRow", "content": [
                    col(9, "VTextarea", model="orphan_download_dirs", label="下载目录映射", rows=3,
                        hint="每行 MP目录:下载器目录1,下载器目录2；任一下载/做种任务和媒体库硬链接引用都会保留文件", **{"persistent-hint": True}),
                    col(3, "VTextField", model="orphan_max_actions", label="孤立文件单次上限", type="number", min=1, max=100),
                ]},
                {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text":
                    "孤立文件按静置时间判断，独立于 180 天观看规则。扫描所有已配置下载器（含停用配置），无法完整核对时停止；执行前再次核对引用和文件身份。删除后实际释放空间，不进入回收区。"}},
                {"component": "VRow", "content": [{
                    "component": "VCol", "props": {"cols": 12},
                    "content": [{"component": "VAlert", "props": {
                        "type": "warning", "variant": "tonal",
                        "text": "规则：电影逐部判断，剧集按整部判断。最新文件入库超过设定天数、且这段时间内没有任何 Plex 账号播放过，"
                                "并且没有在 MoviePilot 订阅中，才会命中。新命中的条目先预告，宽限期后仍无人看才删除，删除不可恢复。",
                    }}],
                }]},
            ],
        }], {
            "enabled": False,
            "dry_run": True,
            "notify": True,
            "onlyonce": False,
            "cron": "30 6 * * *",
            "days": 180,
            "grace_days": 7,
            "limit": 0,
            "library_dirs": "/media/电影/\n/media/电视剧/",
            "plex_path": "/data/",
            "local_path": "/media/",
            "skip_category": "刷流",
            "mediaservers": [],
            "orphan_enabled": False,
            "orphan_dry_run": True,
            "orphan_cron": "0 5 * * *",
            "orphan_download_dirs": "",
            "orphan_min_age_days": 1,
            "orphan_max_actions": 10,
        }

    def get_page(self) -> List[dict]:
        last = self.get_data("last_scan") or {}
        pending = self.get_data("pending") or {}
        history = self.get_data("history") or []
        orphan = self.get_data("orphan_last_scan") or {}
        orphan_history = self.get_data("orphan_history") or []

        def table(headers: List[str], rows: List[List[Any]]) -> dict:
            return {"component": "VTable", "props": {"hover": True, "density": "compact"}, "content": [
                {"component": "thead", "content": [{"component": "tr", "content": [
                    {"component": "th", "text": h} for h in headers]}]},
                {"component": "tbody", "content": [{"component": "tr", "content": [
                    {"component": "td", "text": str(v)} for v in row]} for row in rows]},
            ]}

        grace = self._grace_days * 86400
        pending_rows = sorted(
            ([v.get("title", k), v.get("year") or "", "剧集" if k.startswith("tv:") else "电影",
              human(v["size"]) if v.get("size") else "", fmt_date(v.get("first_seen")),
              fmt_date((v.get("first_seen") or 0) + grace)] for k, v in pending.items()),
            key=lambda r: r[5])
        history_rows = [[h.get("deleted_at", ""), h.get("title", ""), h.get("year") or "",
                         "剧集" if h.get("kind") == "tv" else "电影", human(h.get("size") or 0)]
                        for h in reversed(history[-100:])]
        summary = (f"上次运行 {last.get('time', '—')}（{last.get('mode', '—')}）："
                   f"命中 {last.get('candidates', 0)} 项，共 {last.get('size', '0B')}；"
                   f"本次删除 {last.get('deleted', 0)} 项。") if last else "尚未运行。"

        return [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": summary}},
            {"component": "div", "props": {"class": "text-h6 mt-4 mb-2"}, "text": "孤立下载文件"},
            {"component": "VAlert", "props": {"type": "error" if orphan.get("error") else "info", "variant": "tonal",
                "text": (f"上次运行 {orphan.get('time', '—')}（{orphan.get('mode', '—')}）：候选 {orphan.get('candidates', 0)} 项，"
                         f"删除 {orphan.get('deleted', 0)} 项；{orphan.get('error') or '仅处理配置的下载目录。'}")}},
            table(["候选路径", "大小", "状态"], [[r["path"], human(r["size"]), r["status"]] for r in orphan.get("rows", [])]),
            {"component": "div", "text": "；".join(orphan.get("warnings", []))},
            table(["操作时间", "文件", "大小", "结果"], [[h.get("time", ""), h.get("path", ""), human(h.get("size", 0)), h.get("message", h.get("status", ""))] for h in reversed(orphan_history[-100:])]),
            {"component": "div", "props": {"class": "text-h6 mt-4 mb-2"}, "text": f"宽限期内（{len(pending)}）"},
            table(["标题", "年份", "类型", "大小", "预告日期", "预计删除"], pending_rows)
            if pending_rows else {"component": "div", "text": "无"},
            {"component": "div", "props": {"class": "text-h6 mt-4 mb-2"}, "text": f"最近删除（{len(history)}）"},
            table(["删除时间", "标题", "年份", "类型", "大小"], history_rows)
            if history_rows else {"component": "div", "text": "无"},
        ]

    # ------------------------------------------------------------------ 孤立下载
    def orphan_status(self) -> dict:
        """返回孤立文件扫描结果和操作记录，不包含下载器凭据。"""
        return {"success": True, "enabled": self._orphan_enabled,
                "preview": self._dry_run or self._orphan_dry_run, "cron": self._orphan_cron,
                "last_scan": self.get_data("orphan_last_scan") or {},
                "history": self.get_data("orphan_history") or []}

    def preview_orphans(self) -> dict:
        """接口始终只预览，即使配置已启用实际删除。"""
        return self.run_orphans(preview=True)

    def run_orphans(self, preview: bool = False) -> dict:
        """与观看清理互斥运行，删除前重新读取下载器和媒体库引用。"""
        if not preview and not (self._enabled and self._orphan_enabled):
            return {"success": False, "message": "孤立下载清理未启用"}
        if not lock.acquire(blocking=False):
            return {"success": False, "message": "已有媒体清理任务正在运行"}
        stop = self._stop
        config = {"download_dirs": self._orphan_download_dirs,
                  "library_dirs": [d.rstrip("/") for d in self._library_dirs],
                  "min_age_days": self._orphan_min_age_days}
        dry_run = preview or self._dry_run or self._orphan_dry_run
        last = {"time": datetime.datetime.now().isoformat(timespec="seconds"), "mode": "预览" if dry_run else "执行",
                "candidates": 0, "deleted": 0, "rows": [], "error": ""}
        try:
            directory = self.get_data_path()
            directory.mkdir(parents=True, exist_ok=True)
            with (directory / "orphans.lock").open("a") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                report = scan_orphan_files(config, DownloaderHelper(), time.time(), stop.is_set)
                last.update(report, candidates=len(report["rows"]))
                self.save_data("orphan_last_scan", last)
                if dry_run:
                    return {"success": True, "data": last}
                for row in report["rows"][:self._orphan_max_actions]:
                    if stop.is_set():
                        raise ValueError("配置已变更，停止本轮清理")
                    fresh = scan_orphan_files(config, DownloaderHelper(), time.time(), stop.is_set)
                    current = next((item for item in fresh["rows"] if item["path"] == row["path"]), None)
                    if not current or current["signature"] != row["signature"]:
                        row["status"] = "已变化，保留"
                        continue
                    event = {"id": uuid.uuid4().hex, "time": datetime.datetime.now().isoformat(timespec="seconds"),
                             "path": row["path"], "size": row["size"], "status": "intent", "message": "删除结果待核对"}
                    history = self.get_data("orphan_history") or []
                    history.append(event)
                    self.save_data("orphan_history", history[-HISTORY_KEEP:])
                    try:
                        purge_source(current, stopped=stop.is_set)
                    except Exception as error:
                        reason = str(error) if isinstance(error, ValueError) else "剩余文件保留，等待下轮核对"
                        event.update(status="uncertain", message=f"未完成删除：{reason}")
                        row["status"] = event["message"]
                    else:
                        event.update(status="success", message="已删除")
                        row["status"] = "已删除"
                        last["deleted"] += 1
                        logger.info(f"孤立下载清理：已删除 {row['path']}，{human(row['size'])}")
                    self.save_data("orphan_history", history[-HISTORY_KEEP:])
                    self.save_data("orphan_last_scan", last)
                return {"success": not any(row["status"].startswith("未完成") for row in last["rows"]), "data": last}
        except Exception:
            # 异常中的下载器连接信息可能含密钥，不写入日志或页面。
            last["error"] = "本轮未能完整核对下载器、目录或操作状态，已停止清理。"
            logger.warning(f"孤立下载清理：{last['error']}")
            return {"success": False, "message": last["error"]}
        finally:
            try:
                self.save_data("orphan_last_scan", last)
            finally:
                lock.release()

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
        if not self._library_dirs:
            logger.error(f"{self.plugin_name} 未配置媒体库目录，停止")
            return
        plex = self.__get_plex()
        if not plex:
            return
        now = time.time()
        cutoff = now - self._days * 86400
        played = self.__history_since(plex, cutoff)
        subs = self.__subscribed_tmdb_ids()
        roots = tuple(self._library_dirs)

        state: Dict[str, dict] = self.get_data("pending") or {}
        collected = self.__collect(plex)
        current_ids = {self.__item_id(item) for item in collected}
        # 文件已部分删除时仍保留原清理计划，避免下一轮因 Plex 条目消失而漏掉重试。
        retry_items = [dict(entry["cleanup_item"]) for key, entry in state.items()
                       if entry.get("cleanup_item") and key not in current_ids]
        candidates = []
        for item in collected + retry_items:
            pending_item = state.get(self.__item_id(item), {}).get("cleanup_item")
            item["keys"] = set(item["keys"])

            files = [f for f in item["files"] if f.startswith(roots) and os.path.exists(f)]
            if not files and not pending_item:
                continue
            if item["newest_added"] > cutoff or item["last_viewed"] > cutoff:
                continue
            if item["keys"] & played:
                continue
            if item["tmdb"] and item["tmdb"] in subs:
                continue
            if pending_item:
                item["files"] = pending_item["files"]
                item["records"] = pending_item["records"]
                item["fallback_inodes"] = pending_item["fallback_inodes"]
                item["size"] = pending_item["size"]
            else:
                item["files"] = files
                item["size"] = sum(os.path.getsize(f) for f in files)
            candidates.append(item)
        candidates.sort(key=lambda i: i["newest_added"])
        total = sum(i["size"] for i in candidates)

        mode = "预览" if self._dry_run else "执行"
        logger.info(f"[{mode}] {len(candidates)} 项超过 {self._days} 天无人观看，共 {human(total)}")
        for i in candidates:
            logger.info(f"  {i['kind']:5} {i['title']} ({i['year']}) 文件 {len(i['files'])} 个 "
                        f"{human(i['size'])} 最新入库 {fmt_date(i['newest_added'])}")
        last = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "mode": mode,
                "candidates": len(candidates), "size": human(total), "deleted": 0}
        if self._dry_run:
            self.save_data("last_scan", last)
            return

        current = {self.__item_id(i): i for i in candidates}
        rescued = [v.get("title", k) for k, v in state.items() if k not in current]
        state = {k: v for k, v in state.items() if k in current}
        new = [i for k, i in current.items() if k not in state]
        for i in new:
            state[self.__item_id(i)] = {"title": i["title"], "year": i["year"], "size": i["size"], "first_seen": now}
        for k, i in current.items():  # 迁移来的旧记录补上年份和大小，方便页面展示
            state[k].setdefault("year", i["year"])
            state[k]["size"] = i["size"]
        due = [i for k, i in current.items() if now - state[k]["first_seen"] >= self._grace_days * 86400]
        self.save_data("pending", state)

        if rescued:
            logger.info(f"  不再命中（已观看或有变化）：{', '.join(rescued)}")
        if new:
            delete_on = fmt_date(now + self._grace_days * 86400)
            lines = [f"- {i['title']} ({i['year']}) {human(i['size'])}" for i in new]
            self.__notify("媒体库清理预告",
                          f"以下 {len(new)} 项已 {self._days} 天无人观看，共 {human(sum(i['size'] for i in new))}，"
                          f"将于 {delete_on} 删除。期间在 Plex 播放其中任意内容即可保留。\n" + "\n".join(lines))
            logger.info(f"  预告 {len(new)} 项，将于 {delete_on} 删除")

        deleted = []
        for item in due:
            if self._limit and len(deleted) >= self._limit:
                break
            try:
                if "records" not in item:
                    item["records"] = {f: self.__transfer_record(f) for f in item["files"]}
                    item["fallback_inodes"] = []
                    for f in item["files"]:
                        if not item["records"][f] and os.path.exists(f):
                            stat = os.stat(f)
                            if stat.st_nlink > 1:
                                item["fallback_inodes"].append([stat.st_dev, stat.st_ino])
                state[self.__item_id(item)]["cleanup_item"] = dict(item, keys=sorted(key for key in item["keys"] if key))
                self.save_data("pending", state)
                self.__delete_item(item)
            except Exception as err:
                logger.error(f"  删除 {item['title']} ({item['year']}) 失败：{err}", exc_info=True)
                continue
            logger.info(f"  已删除 {item['kind']} {item['title']} ({item['year']}) {human(item['size'])}")
            deleted.append(item)
            state.pop(self.__item_id(item), None)
            self.save_data("pending", state)

        if deleted:
            for sec in {i["section"] for i in deleted}:
                try:
                    plex.query(f"/library/sections/{sec}/refresh")
                except Exception as err:
                    logger.warning(f"  通知 Plex 刷新媒体库 {sec} 失败：{err}")
            self.__notify("媒体库清理",
                          f"已删除 {len(deleted)} 项，释放约 {human(sum(i['size'] for i in deleted))}。\n"
                          + "\n".join(f"- {i['title']} ({i['year']})" for i in deleted))
            history = self.get_data("history") or []
            stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            history += [{"title": i["title"], "year": i["year"], "kind": i["kind"], "size": i["size"],
                         "deleted_at": stamp} for i in deleted]
            self.save_data("history", history[-HISTORY_KEEP:])

        last["deleted"] = len(deleted)
        self.save_data("last_scan", last)
        logger.info(f"[执行] 本次删除 {len(deleted)} 项，{len(state)} 项待清理（含宽限期和失败重试）")

    @staticmethod
    def __item_id(item: dict) -> str:
        return f"{item['kind']}:{item['rating_key']}"

    def __notify(self, title: str, text: str):
        if self._notify:
            self.post_message(mtype=MessageType.Plugin, title=f"【{title}】", text=text)

    # ------------------------------------------------------------------ Plex
    def __get_plex(self):
        services = MediaServerHelper().get_services(type_filter="plex", name_filters=self._mediaservers or None)
        for name, service in (services or {}).items():
            if not service.instance or service.instance.is_inactive():
                logger.warning(f"Plex 服务器 {name} 未连接")
                continue
            plex = service.instance.get_plex()
            if plex:
                return plex
        logger.error(f"{self.plugin_name} 没有可用的 Plex 服务器")
        return None

    @staticmethod
    def __query(plex, path: str, **params):
        key = f"{path}?{urllib.parse.urlencode(params)}" if params else path
        return plex.query(key)

    def __history_since(self, plex, ts: float) -> Set[str]:
        """自 ts 起任何账号播放过的 ratingKey（含所在季、剧）。"""
        keys, start = set(), 0
        while True:
            mc = self.__query(plex, "/status/sessions/history/all", **{
                "viewedAt>": int(ts), "X-Plex-Container-Start": start, "X-Plex-Container-Size": 500})
            items = list(mc) if mc is not None else []
            for v in items:
                for k in ("ratingKey", "parentRatingKey", "grandparentRatingKey"):
                    if v.get(k):
                        keys.add(v.get(k))
            start += len(items)
            if not items or start >= int(mc.get("totalSize", start)):
                return keys

    @staticmethod
    def __tmdb_id(el) -> Optional[str]:
        if el is None:
            return None
        for g in el.findall("Guid"):
            gid = g.get("id", "")
            if gid.startswith("tmdb://"):
                return gid[7:]
        return None

    def __to_local(self, plex_file: str) -> str:
        if self._plex_path and plex_file.startswith(self._plex_path):
            return self._local_path + plex_file[len(self._plex_path):]
        return plex_file

    def __collect(self, plex) -> List[dict]:
        """列出 Plex 电影（逐部）和剧集（整部）及其文件、最新入库时间和最近观看时间。"""
        out = []
        for sec in self.__query(plex, "/library/sections"):
            stype, sid = sec.get("type"), sec.get("key")
            if stype == "movie":
                for v in self.__query(plex, f"/library/sections/{sid}/all", type=1, includeGuids=1):
                    out.append({
                        "kind": "movie", "title": v.get("title"), "year": v.get("year"),
                        "tmdb": self.__tmdb_id(v),
                        "files": [self.__to_local(p.get("file")) for m in v.findall("Media") for p in m.findall("Part")],
                        "newest_added": int(v.get("addedAt", "0")),
                        "last_viewed": int(v.get("lastViewedAt", "0")),
                        "keys": {v.get("ratingKey")}, "rating_key": v.get("ratingKey"), "section": sid,
                    })
            elif stype == "show":
                shows = {s.get("ratingKey"): s for s in
                         self.__query(plex, f"/library/sections/{sid}/all", type=2, includeGuids=1)}
                grouped: Dict[str, dict] = {}
                for e in self.__query(plex, f"/library/sections/{sid}/all", type=4):
                    g = grouped.setdefault(e.get("grandparentRatingKey"), {
                        "files": [], "newest_added": 0, "last_viewed": 0, "keys": set()})
                    g["files"] += [self.__to_local(p.get("file")) for m in e.findall("Media") for p in m.findall("Part")]
                    g["newest_added"] = max(g["newest_added"], int(e.get("addedAt", "0")))
                    g["last_viewed"] = max(g["last_viewed"], int(e.get("lastViewedAt", "0")))
                    g["keys"] |= {e.get("ratingKey"), e.get("parentRatingKey")}
                for skey, g in grouped.items():
                    s = shows.get(skey)
                    out.append({
                        "kind": "tv", "title": s.get("title") if s is not None else skey,
                        "year": s.get("year") if s is not None else None,
                        "tmdb": self.__tmdb_id(s),
                        "keys": g["keys"] | {skey}, "rating_key": skey, "section": sid,
                        "files": g["files"], "newest_added": g["newest_added"], "last_viewed": g["last_viewed"],
                    })
        return out

    # ------------------------------------------------------------------ MoviePilot
    @staticmethod
    def __subscribed_tmdb_ids() -> Set[str]:
        ids, page = set(), 1
        while True:
            result = list_subscriptions(None, {"page": page, "count": 200})
            for s in result.items:
                source = getattr(s.media_source, "value", s.media_source)
                if source == "themoviedb" and s.media_id:
                    ids.add(str(s.media_id))
            if not result.items or page * 200 >= result.total:
                return ids
            page += 1

    @staticmethod
    def __transfer_record(dest: str) -> Optional[dict]:
        """该媒体库文件对应的成功整理记录。"""
        result = list_transfer_history({"dest": dest, "status": True}, {"page": 1, "count": 20})
        for rec in result.items:
            if rec.dest == dest:
                return rec.model_dump(mode="json")
        return None

    @staticmethod
    def __mp_api(method: str, path: str, query: Optional[dict] = None, body: Any = None) -> dict:
        """调用 MoviePilot 自己的接口，与在界面上删除整理记录走同一条路径。"""
        url = f"http://127.0.0.1:{settings.PORT}/api/v1{path}"
        if query:
            url += f"?{urllib.parse.urlencode(query)}"
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "X-API-KEY": str(settings.API_TOKEN), "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as err:
            return {"success": False, "message": f"HTTP {err.code} {err.read()[:200]!r}"}

    # ------------------------------------------------------------------ 删除
    def __delete_item(self, item: dict):
        """完成源文件和媒体文件清理，部分失败保留计划供下一次运行重试。"""
        records = item["records"]
        for f, rec in records.items():
            if not rec:
                continue
            # 上一次已成功删除此记录时，允许重试同一作品的其余记录。
            source = rec.get("src") or ""
            local_source = rec.get("src_storage") in (None, "local")
            if local_source and source and not os.path.exists(source) and not os.path.exists(f):
                continue
            res = self.__mp_api("DELETE", "/history/transfer", {"deletesrc": "true", "deletedest": "true"}, rec)
            if not res.get("success"):
                raise RuntimeError(f"MoviePilot 删除 {f} 未完成：{res.get('message')}")
            if os.path.exists(f) or (local_source and source and os.path.exists(source)):
                raise RuntimeError(f"MoviePilot 返回成功，但源文件或媒体文件仍存在：{f}")

        inodes = {tuple(value) for value in item["fallback_inodes"]}
        if inodes:
            for name in self.__torrents_by_inode(inodes):
                logger.info(f"    删除下载器任务：{name}")
        for f, rec in records.items():
            if not rec and os.path.exists(f):
                os.remove(f)
        self.__tidy(item["files"])

    def __torrents_by_inode(self, inodes: Set[Tuple[int, int]]) -> Dict[str, List[str]]:
        """删除文件硬链接到 inodes 的 qB 任务（跳过指定分类），返回 名称->哈希。"""
        removed: Dict[str, List[str]] = {}
        services = DownloaderHelper().get_services(type_filter="qbittorrent")
        for service_name, service in (services or {}).items():
            qb = service.instance
            if not qb or (hasattr(qb, "is_inactive") and qb.is_inactive()):
                raise RuntimeError(f"下载器 {service_name} 未连接，保留清理计划")
            torrents, error = qb.get_torrents()
            if error:
                raise RuntimeError(f"读取 {service_name} 任务列表失败，保留清理计划")
            hits = {}
            for t in torrents:
                save_path = t.get("save_path") or ""
                if (self._skip_category and t.get("category") == self._skip_category) \
                        or not save_path.startswith(self._local_path or "/"):
                    continue
                for tf in qb.get_files(t.get("hash")) or []:
                    try:
                        stat = os.stat(os.path.join(save_path, tf.get("name")))
                        if (stat.st_dev, stat.st_ino) in inodes:
                            hits[t.get("hash")] = t.get("name")
                            break
                    except OSError:
                        continue
            if hits:
                if not qb.delete_torrents(delete_file=True, ids=list(hits)):
                    raise RuntimeError(f"下载器 {service_name} 删除任务失败，保留清理计划")
                for h, n in hits.items():
                    removed.setdefault(n, []).append(h)
        return removed

    def __tidy(self, files: List[str]):
        """删掉条目所在目录里残留的刮削文件（nfo/jpg），前提是目录里已没有视频。"""
        roots = set(self._library_dirs)
        top = os.path.commonpath(files)
        while os.path.dirname(top).rstrip("/") + "/" not in roots and os.path.dirname(top) != top:
            top = os.path.dirname(top)
        if top.rstrip("/") + "/" in roots or not os.path.isdir(top):
            return
        if any(os.path.splitext(n)[1].lower() in VIDEO_EXT for _, _, ns in os.walk(top) for n in ns):
            return
        for root, _, names in os.walk(top, topdown=False):
            for n in names:
                os.remove(os.path.join(root, n))
            os.rmdir(root)
