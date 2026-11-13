"""IYUU query client: bounded retries and explicit errors, without downloader writes.

Derived from jxxghp/CKun's IYUUAutoSeed helper (GPL-3.0); maintained by peter5842.
"""
import hashlib
import json
import re
import time
from threading import Event
from typing import Optional, Tuple

from requests.exceptions import RequestException

from app.sdk.logging import logger
from app.sdk.network import RequestUtils


class IyuuHelper:
    _version = "8.2.0"
    _api_base = "https://2025.iyuu.cn"
    _attempts = 3
    _timeout = 45
    _retry_statuses = {408, 425, 429, 500, 502, 503, 504}

    def __init__(self, token: str, stop_event: Optional[Event] = None):
        self._token = token
        self._stop = stop_event if stop_event is not None else Event()
        self._sites = {}
        self._sid_sha1 = None
        self.last_error = ""

    def _safe_message(self, message) -> str:
        text = str(message or "")
        if self._token:
            text = text.replace(self._token, "[hidden]")
        text = re.sub(r"https?://\S+", "[URL]", text)
        return text.replace("\n", " ").replace("\r", " ")[:160]

    def __request_iyuu(self, url: str, method: str = "get", params: dict = None) -> Tuple[Optional[dict], str]:
        """Retry query/handshake requests only; never retry an authentication rejection."""
        if not self._token:
            return None, "请求IYUU失败，未配置Token"
        error = "请求IYUU失败"
        for attempt in range(1, self._attempts + 1):
            if self._stop.is_set():
                return None, "IYUU查询已停止"
            delay = 2 ** attempt
            retryable = True
            try:
                client = RequestUtils(accept_type="application/json", headers={"token": self._token},
                                      timeout=self._timeout)
                if method == "post":
                    response = client.post_res(self._api_base + url, json=params, raise_exception=True)
                else:
                    response = client.get_res(self._api_base + url, params=params, raise_exception=True)
                if response is None:
                    error = "网络请求未返回响应"
                elif response.status_code != 200:
                    error = f"HTTP {response.status_code}"
                    retryable = response.status_code in self._retry_statuses
                    retry_after = response.headers.get("Retry-After", "")
                    if str(retry_after).isdigit():
                        if int(retry_after) > 30:
                            return None, f"请求IYUU失败，{error}，服务端要求等待{retry_after}秒，本轮停止重试"
                        delay = max(delay, int(retry_after))
                else:
                    try:
                        result = response.json()
                    except ValueError:
                        result = None
                    if not isinstance(result, dict) or "code" not in result:
                        error = "响应不是有效的IYUU JSON对象"
                    elif str(result["code"]) != "0":
                        return None, (f"请求IYUU失败，业务码 {self._safe_message(result['code'])}："
                                      f"{self._safe_message(result.get('msg'))}")
                    elif not isinstance(result.get("data"), dict):
                        error = "成功响应缺少有效data对象"
                    else:
                        return result["data"], ""
            except RequestException as exc:
                # Exception text and response bodies can contain credentials; retain only the type.
                error = f"网络异常 {type(exc).__name__}"
            if not retryable or attempt == self._attempts:
                return None, f"请求IYUU失败，{error}（已尝试{attempt}次）"
            logger.warning(f"IYUU {url}：{error}，{delay}秒后重试（{attempt}/{self._attempts}）")
            if self._stop.wait(delay):
                return None, "IYUU查询已停止"
        return None, error

    def __get_sites(self) -> Tuple[dict, str]:
        data, error = self.__request_iyuu("/reseed/sites/index")
        if error:
            return {}, error
        sites = data.get("sites")
        if not isinstance(sites, list) or not sites:
            return {}, "IYUU站点列表为空或格式错误"
        if any(not isinstance(site, dict) or not site.get("id") for site in sites):
            return {}, "IYUU站点列表包含无效条目"
        return {str(site["id"]): site for site in sites}, ""

    def get_torrent_url(self, sid: str) -> Tuple[Optional[str], Optional[str]]:
        if not sid:
            return None, None
        if not self._sites:
            self._sites, self.last_error = self.__get_sites()
        site = self._sites.get(str(sid)) or {}
        return site.get("base_url"), site.get("download_page")

    def __report_existing(self) -> Tuple[Optional[str], str]:
        if not self._sites:
            self._sites, error = self.__get_sites()
            if error:
                return None, error
        data, error = self.__request_iyuu("/reseed/sites/reportExisting", "post",
                                        {"sid_list": [site["id"] for site in self._sites.values()]})
        if error:
            return None, error
        sid_sha1 = data.get("sid_sha1")
        if not isinstance(sid_sha1, str) or not sid_sha1:
            return None, "IYUU站点握手未返回有效sid_sha1"
        return sid_sha1, ""

    def __reseed_index(self, payload: str) -> Tuple[Optional[dict], str]:
        return self.__request_iyuu("/reseed/index/index", "post", {
            "hash": payload, "sha1": self.get_sha1(payload), "sid_sha1": self._sid_sha1,
            "timestamp": int(time.time()), "version": self._version,
        })

    def get_seed_info(self, info_hashs: list) -> Tuple[Optional[dict], str]:
        if not info_hashs:
            return {}, ""
        if not self._sid_sha1:
            self._sid_sha1, error = self.__report_existing()
            if error:
                self.last_error = error
                return None, error
        payload = json.dumps(sorted(info_hashs), separators=(",", ":"), ensure_ascii=False)
        data, error = self.__reseed_index(payload)
        if "站点哈希值 require" in error:
            self._sid_sha1, error = self.__report_existing()
            if not error:
                data, error = self.__reseed_index(payload)
        self.last_error = error
        return data, error

    @staticmethod
    def get_sha1(json_str: str) -> str:
        return hashlib.sha1(json_str.encode("utf-8")).hexdigest()
