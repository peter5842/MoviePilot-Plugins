"""Exercise real query and batch control flow with offline services and injected failures."""
import ast
from datetime import datetime, timedelta
import hashlib
import json
import importlib.util
import logging
import os
from pathlib import Path
import re
from threading import Event, Lock
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock

import pytest


ROOT = Path(__file__).resolve().parents[1] / "plugins.v3/iyuuautoseed"
logger = logging.getLogger("iyuu-test")
spec = importlib.util.spec_from_file_location("iyuu_source_guard", ROOT / "source_guard.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
check_source = guard.check_source


class RequestException(Exception):
    pass


class ReadTimeout(RequestException):
    pass


def load_class(file, name, extra):
    node = next(n for n in ast.parse(file.read_text()).body if isinstance(n, ast.ClassDef) and n.name == name)
    namespace = dict(globals(), **extra)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(file), "exec"), namespace)
    return namespace[name], namespace


Helper, helper_ns = load_class(ROOT / "iyuu_helper.py", "IyuuHelper", {"RequestUtils": Mock()})
Plugin, plugin_ns = load_class(ROOT / "__init__.py", "IYUUAutoSeed", {
    "_PluginBase": object, "ServiceInfo": SimpleNamespace, "DownloaderHelper": Mock(), "CommentedMap": dict,
    "eventmanager": SimpleNamespace(register=lambda _: lambda f: f),
    "EventType": SimpleNamespace(SiteDeleted=1), "NotificationType": SimpleNamespace(SiteMessage=1),
})


def response(data=None, status=200, code=0, **kwargs):
    obj = Mock(status_code=status, headers=kwargs.get("headers", {}))
    obj.json.return_value = {"code": code, "data": {} if data is None else data, "msg": kwargs.get("msg", "")}
    return obj


@pytest.fixture
def transport(monkeypatch):
    client = Mock()
    factory = Mock(return_value=client)
    monkeypatch.setitem(helper_ns, "RequestUtils", factory)
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.return_value = False
    helper = Helper("private-test-token", stop)
    helper._sid_sha1 = "existing-site-digest"
    return helper, client, factory, stop


def test_transient_timeout_and_gateway_failure_recover(transport):
    helper, client, factory, stop = transport
    client.post_res.side_effect = [ReadTimeout("contains private-test-token"), response(status=502), response({"source": {}})]
    data, error = helper.get_seed_info(["source"])
    assert data == {"source": {}} and not error
    assert client.post_res.call_count == 3
    assert [x.args[0] for x in stop.wait.call_args_list] == [2, 4]
    assert all(call.kwargs["timeout"] == 45 for call in factory.call_args_list)


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_non_transient_http_errors_are_not_retried(transport, status):
    helper, client, _, stop = transport
    client.post_res.return_value = response(status=status)
    data, error = helper.get_seed_info(["source"])
    assert data is None and f"HTTP {status}" in error
    assert client.post_res.call_count == 1
    stop.wait.assert_not_called()


def test_rate_limit_respects_bounded_retry_after(transport):
    helper, client, _, stop = transport
    client.post_res.side_effect = [response(status=429, headers={"Retry-After": "20"}), response()]
    assert helper.get_seed_info(["source"]) == ({}, "")
    stop.wait.assert_called_once_with(20)


def test_long_server_backoff_defers_to_next_run(transport):
    helper, client, _, stop = transport
    client.post_res.return_value = response(status=429, headers={"Retry-After": "120"})
    assert "本轮停止重试" in helper.get_seed_info(["source"])[1]
    assert client.post_res.call_count == 1
    stop.wait.assert_not_called()
    assert "冷却中" in helper.get_seed_info(["next-source"])[1]
    assert client.post_res.call_count == 1


@pytest.mark.parametrize("limited", [response(status=429), response(code=400, msg="访问频率过快")])
def test_rate_limit_stops_following_batches_until_cooldown_expires(transport, monkeypatch, limited):
    helper, client, _, stop = transport
    clock = Mock(return_value=1000)
    monkeypatch.setitem(helper_ns, "time", SimpleNamespace(monotonic=clock, time=lambda: 1000))
    client.post_res.side_effect = [limited, response()]
    assert "60秒" in helper.get_seed_info(["source"])[1]
    clock.return_value = 1059
    assert "冷却中" in helper.get_seed_info(["next-source"])[1]
    assert client.post_res.call_count == 1
    stop.wait.assert_not_called()
    clock.return_value = 1061
    assert helper.get_seed_info(["next-source"]) == ({}, "")
    assert client.post_res.call_count == 2


def test_handshake_rejection_is_not_repeated_for_each_batch(transport):
    helper, client, factory, _ = transport
    helper._sid_sha1 = None
    client.get_res.return_value = response({"sites": [{"id": 1}]})
    client.post_res.return_value = response(code=400, msg="缺少sid_list")
    assert "/reseed/sites/reportExisting" in helper.get_seed_info(["source"])[1]
    assert "冷却中" in helper.get_seed_info(["next-source"])[1]
    assert client.get_res.call_count == client.post_res.call_count == 1
    assert client.post_res.call_args.kwargs["json"] == {"sid_list": [1]}
    assert factory.call_args.kwargs["headers"]["Content-Type"] == "application/json"


def test_query_retry_exhaustion_is_bounded_and_does_not_leak_response(transport):
    helper, client, _, _ = transport
    bad = response()
    bad.json.side_effect = ValueError("private-test-token, secret body")
    bad.text = "private-test-token https://example.invalid/?passkey=secret"
    client.post_res.return_value = bad
    data, error = helper.get_seed_info(["source"])
    assert data is None and "3次" in error
    assert "private-test-token" not in error and "secret" not in error
    assert client.post_res.call_count == 3


def test_authentication_business_error_is_not_retried_and_is_redacted(transport):
    helper, client, _, stop = transport
    client.post_res.return_value = response(code=401, msg="private-test-token https://example.invalid/?token=secret")
    data, error = helper.get_seed_info(["source"])
    assert data is None and "401" in error
    assert "private-test-token" not in error and "secret" not in error
    assert client.post_res.call_count == 1
    stop.wait.assert_not_called()


def test_stop_interrupts_backoff(transport):
    helper, client, _, stop = transport
    client.post_res.return_value = None
    stop.wait.return_value = True
    assert helper.get_seed_info(["source"]) == (None, "IYUU查询已停止")
    assert client.post_res.call_count == 1


def test_stop_prevents_first_request(transport):
    helper, client, _, stop = transport
    stop.is_set.return_value = True
    assert helper.get_seed_info(["source"])[0] is None
    client.post_res.assert_not_called()


def test_failed_site_discovery_never_queries_with_missing_handshake(transport):
    helper, client, _, _ = transport
    helper._sid_sha1 = None
    client.get_res.return_value = response(status=503)
    data, error = helper.get_seed_info(["source"])
    assert data is None and "503" in error
    assert client.get_res.call_count == 3
    client.post_res.assert_not_called()


def test_missing_handshake_digest_stops_before_seed_lookup(transport):
    helper, client, _, _ = transport
    helper._sid_sha1 = None
    client.get_res.return_value = response({"sites": [{"id": 1}]})
    client.post_res.return_value = response({})
    assert "sid_sha1" in helper.get_seed_info(["source"])[1]
    assert client.post_res.call_count == 1
    assert client.post_res.call_args.args[0].endswith("reportExisting")


def test_full_handshake_empty_matches_and_input_order_are_valid(transport):
    helper, client, _, _ = transport
    helper._sid_sha1 = None
    client.get_res.return_value = response({"sites": [{"id": 1, "base_url": "example.invalid", "download_page": "download.php"}]})
    client.post_res.side_effect = [response({"sid_sha1": "new-digest"}), response()]
    hashes = ["b", "a"]
    assert helper.get_seed_info(hashes) == ({}, "")
    assert hashes == ["b", "a"]
    assert helper.get_torrent_url(1) == ("example.invalid", "download.php")
    assert client.post_res.call_args.kwargs["json"]["sid_sha1"] == "new-digest"


def test_invalidated_handshake_is_refreshed_once(transport):
    helper, client, _, _ = transport
    helper._sites = {"1": {"id": 1}}
    client.post_res.side_effect = [response(code=1, msg="站点哈希值 require"), response({"sid_sha1": "new-digest"}), response()]
    assert helper.get_seed_info(["source"]) == ({}, "")
    assert client.post_res.call_count == 3
    assert helper._sid_sha1 == "new-digest"


def test_helpers_do_not_share_site_state():
    first, second = Helper("one"), Helper("two")
    first._sites["1"] = {"id": 1}
    assert not second._sites


class Torrent(dict):
    __getattr__ = dict.__getitem__


@pytest.fixture
def run(monkeypatch):
    plugin = Plugin()
    plugin._event, plugin._seed_lock = Event(), Lock()
    plugin._downloaders = ["qB"]
    plugin._error_caches, plugin._permanent_error_caches, plugin._success_caches = [], [], []
    plugin._notify = False
    plugin._size = 0
    plugin._reuse_save_path = True
    plugin.iyuu_helper = Mock()
    plugin.iyuu_helper.get_seed_info.return_value = ({}, "")
    plugin.update_config = Mock()
    plugin.check_recheck = Mock()
    plugin.post_message = Mock()
    saved = {}
    plugin.save_data = lambda key, value: saved.update({key: json.loads(json.dumps(value))})
    plugin.get_data = lambda key: saved.get(key)
    downloader = Mock()
    downloader.is_inactive.return_value = False
    downloader.get_completed_torrents.return_value = [Torrent(hash=f"source-{i}", save_path="/source", tags="", total_size=1,
                                                            completed=1, availability=-1) for i in range(401)]
    service = SimpleNamespace(name="qB", type="qbittorrent", instance=downloader)
    services = Mock()
    services.get_services.return_value = {"qB": service}
    monkeypatch.setitem(plugin_ns, "DownloaderHelper", lambda: services)
    return plugin, saved, downloader


def test_all_queries_failed_marks_scheduler_and_persisted_result_failed(run, caplog):
    plugin, saved, _ = run
    plugin.iyuu_helper.get_seed_info.return_value = (None, "HTTP 502，已尝试3次")
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError, match="查询失败 3 批"):
        plugin.auto_seed()
    assert saved["last_run"]["status"] == "failed"
    assert saved["last_run"]["failed_sources"] == 401
    assert plugin._error_caches == plugin._permanent_error_caches == []
    assert "全部查询成功" not in caplog.text and "辅种全部完成" not in caplog.text
    assert not plugin._seed_lock.locked()


def test_partial_query_failure_keeps_other_batches_and_can_retry_next_run(run):
    plugin, saved, _ = run
    plugin.iyuu_helper.get_seed_info.side_effect = [(None, "network"), ({}, ""), ({}, "")]
    with pytest.raises(RuntimeError):
        plugin.auto_seed()
    assert saved["last_run"]["status"] == "partial"
    assert saved["last_run"]["query_ok"] == 2 and saved["last_run"]["query_failed"] == 1
    plugin.iyuu_helper.get_seed_info.side_effect = None
    plugin.auto_seed()
    assert saved["last_run"]["status"] == "completed" and saved["last_run"]["query_ok"] == 3
    assert plugin.iyuu_helper.get_seed_info.call_count == 6


def test_empty_lookup_results_are_completed_without_downloader_writes(run):
    plugin, saved, downloader = run
    plugin.auto_seed()
    assert saved["last_run"]["status"] == "completed"
    downloader.add_torrent.assert_not_called()
    plugin.post_message.assert_not_called()


def test_offline_downloader_is_not_reported_as_empty_success(run):
    plugin, saved, downloader = run
    downloader.get_completed_torrents.return_value = None
    with pytest.raises(RuntimeError, match="读取已完成种子失败"):
        plugin.auto_seed()
    assert saved["last_run"]["status"] == "failed"
    plugin.iyuu_helper.get_seed_info.assert_not_called()


def test_duplicate_run_does_not_touch_current_result(run):
    plugin, saved, _ = run
    plugin._seed_lock.acquire()
    with pytest.raises(RuntimeError, match="已在运行"):
        plugin.auto_seed()
    assert not saved
    plugin._seed_lock.release()


def test_persistence_failure_releases_execution_lock(run):
    plugin, _, _ = run
    plugin.save_data = Mock(side_effect=OSError("disk full"))
    with pytest.raises(OSError):
        plugin.auto_seed()
    assert not plugin._seed_lock.locked()


def test_stop_does_not_become_success(run):
    plugin, saved, _ = run
    plugin._event.set()
    plugin.auto_seed()
    assert saved["last_run"]["status"] == "stopped"
    plugin.iyuu_helper.get_seed_info.assert_not_called()


@pytest.mark.parametrize("download_ok", [True, False])
def test_matched_seed_result_is_reflected_in_task_outcome(run, download_ok):
    plugin, saved, downloader = run
    downloader.get_completed_torrents.return_value = downloader.get_completed_torrents.return_value[:1]
    plugin.iyuu_helper.get_seed_info.return_value = ({"source-0": {"torrent": [{"sid": 1, "info_hash": "new-seed"}]}}, "")
    def add(**kwargs):
        assert kwargs["save_path"] == "/source" and kwargs["seed"]["info_hash"] == "new-seed"
        if download_ok:
            plugin.success += 1
        else:
            plugin.fail += 1
        return download_ok
    plugin._IYUUAutoSeed__download_torrent = Mock(side_effect=add)
    if download_ok:
        plugin.auto_seed()
        assert saved["source-0"][0]["torrents"] == ["new-seed"]
        assert saved["last_run"]["status"] == "completed" and saved["last_run"]["seed_success"] == 1
    else:
        with pytest.raises(RuntimeError, match="辅种失败 1 个"):
            plugin.auto_seed()
        assert saved["last_run"]["status"] == "partial" and saved["last_run"]["seed_failed"] == 1
    plugin._IYUUAutoSeed__download_torrent.assert_called_once()


def test_query_only_probe_never_adds_starts_or_notifies(run):
    plugin, saved, downloader = run
    plugin._notify = True
    plugin.iyuu_helper.get_seed_info.return_value = ({"source-0": {"torrent": [{"sid": 1, "info_hash": "new"}]}}, "")
    plugin._IYUUAutoSeed__download_torrent = Mock()
    result = plugin.probe_queries()
    assert result["query_only"] and result["query_ok"] == 3
    assert saved["last_probe"]["status"] == "completed" and "last_run" not in saved
    plugin._IYUUAutoSeed__download_torrent.assert_not_called()
    plugin.check_recheck.assert_not_called()
    plugin.update_config.assert_not_called()
    plugin.post_message.assert_not_called()


@pytest.mark.parametrize("case", ["valid", "missing_root", "missing_file", "wrong_size", "unmapped", "escape", "symlink", "empty_list"])
def test_source_guard_checks_real_files(tmp_path, case):
    root = tmp_path / "source"
    folder = root / "film"
    folder.mkdir(parents=True)
    file = folder / "video.mkv"
    file.write_bytes(b"source")
    torrent = {"hash": "source", "save_path": "/downloads", "content_path": "/downloads/film"}
    files = [{"name": "film/video.mkv", "size": 6}]
    mappings = f"{root}:/downloads"
    if case == "missing_root":
        file.unlink(); folder.rmdir()
    elif case == "missing_file":
        file.unlink()
    elif case == "wrong_size":
        file.write_bytes(b"partial")
    elif case == "unmapped":
        torrent["save_path"] = "/elsewhere"
    elif case == "escape":
        files[0]["name"] = "../outside"
    elif case == "symlink":
        outside = tmp_path / "outside"
        outside.write_bytes(b"source")
        file.unlink(); file.symlink_to(outside)
    elif case == "empty_list":
        files = []
    get_files = Mock(return_value=files)
    ready, reason = check_source(torrent, get_files, mappings)
    assert ready == (case == "valid")
    if case != "valid":assert reason
    if case == "missing_root":get_files.assert_not_called()


def test_missing_source_is_excluded_before_iyuu_query(run, tmp_path):
    plugin, saved, downloader = run
    plugin._source_path_mappings = f"{tmp_path}:/source"
    plugin.auto_seed()
    assert saved["last_run"]["source_skipped"] == 401
    plugin.iyuu_helper.get_seed_info.assert_not_called()


@pytest.mark.parametrize("reason,permanent", [("种子数据有误，请确认链接是否正确", False), ("无法打开链接", False), ("触发站点流控", False), ("下载种子出错，状态码：403", False), ("下载种子出错，状态码：404", True), ("下载种子出错，状态码：410", True)])
def test_download_errors_only_cache_explicit_removal(run, monkeypatch, reason, permanent):
    plugin, saved, downloader = run
    plugin.total = plugin.realtotal = plugin.fail = plugin.cached = 0
    plugin._run_summary = {"errors": []}
    plugin._deferred_hashes = set()
    plugin.iyuu_helper.get_torrent_url.return_value = ("https://site.invalid", "download")
    site = {"id": 1, "name": "site", "url": "https://site.invalid"}
    sites = Mock()
    sites.get_indexer.return_value = site
    sites.check.return_value = (False, "")
    monkeypatch.setitem(plugin_ns, "SitesHelper", lambda: sites)
    monkeypatch.setitem(plugin_ns, "StringUtils", SimpleNamespace(get_url_domain=lambda _: "site.invalid"))
    monkeypatch.setitem(plugin_ns, "settings", SimpleNamespace(USER_AGENT="test"))
    helper = Mock()
    helper.download_torrent.return_value = (None, None, "", [], reason)
    monkeypatch.setitem(plugin_ns, "TorrentHelper", lambda: helper)
    downloader.get_torrents.return_value = ([], None)
    plugin._IYUUAutoSeed__get_download_url = Mock(return_value="https://site.invalid/download?token=private")
    service = SimpleNamespace(name="qB", type="qbittorrent", instance=downloader)
    assert not plugin._IYUUAutoSeed__download_torrent({"sid": 1, "info_hash": "candidate"}, service, "/source", None)
    assert ("candidate" in plugin._permanent_error_caches) == permanent
    assert ("candidate" in plugin._deferred_hashes) == (not permanent)
    assert not plugin._error_caches
    assert helper.download_torrent.call_args.kwargs["cache_invalid"] is False
    assert helper.download_torrent.call_args.kwargs["site_id"] == 1


def test_recheck_queue_persists_and_releases_busy_flag_on_failure(run):
    plugin, saved, _ = run
    plugin._recheck_torrents = {"qB": ["new-seed"]}
    plugin._auto_rechecked = {"new-seed", "finished-seed"}
    plugin._is_recheck_running = False
    plugin.check_recheck_service = Mock(side_effect=RuntimeError("offline"))
    with pytest.raises(RuntimeError):
        Plugin.check_recheck(plugin)
    assert not plugin._is_recheck_running
    assert saved["pending_rechecks"] == {"qB": ["new-seed"]}
    assert saved["rechecked_once"] == ["new-seed"]


def test_explicit_queue_migration_only_tracks_requested_pending_tasks(run):
    plugin, saved, downloader = run
    plugin._recheck_torrents, plugin._auto_rechecked = {}, set()
    pending, active, unrelated = "a" * 40, "b" * 40, "c" * 40
    downloader.get_torrents.return_value = ([{"hash": pending, "state": "checkingDL"}, {"hash": active, "state": "uploading"}, {"hash": unrelated, "state": "stoppedUP"}], None)
    assert plugin.restore_checks({"downloaders": {"qB": [pending, active]}}) == {"restored": 1}
    assert saved["pending_rechecks"] == {"qB": [pending]}
    downloader.start_torrents.assert_not_called()
    downloader.recheck_torrents.assert_not_called()


@pytest.mark.parametrize("payload", [{}, {"downloaders": {"other": ["a" * 40]}}, {"downloaders": {"qB": ["not-a-hash"]}}])
def test_queue_migration_rejects_ambiguous_requests(run, payload):
    plugin, saved, downloader = run
    with pytest.raises(ValueError):plugin.restore_checks(payload)
    assert "pending_rechecks" not in saved
    downloader.get_torrents.assert_not_called()


def test_failed_start_keeps_pending_check_but_removed_task_is_pruned(run):
    plugin, saved, downloader = run
    plugin._recheck_torrents = {"qB": ["pending", "removed"]}
    plugin._auto_rechecked = set()
    plugin._is_recheck_running = False
    downloader.get_torrents.return_value = ([{"hash": "pending", "state": "stoppedUP", "progress": 1}], None)
    downloader.start_torrents.return_value = False
    Plugin.check_recheck(plugin)
    assert saved["pending_rechecks"] == {"qB": ["pending"]}
