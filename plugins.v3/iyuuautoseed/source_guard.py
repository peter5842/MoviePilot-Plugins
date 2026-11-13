"""Read-only source verification across MoviePilot/downloader path mappings."""
from pathlib import Path, PurePosixPath


def check_source(torrent, get_files, mappings: str):
    """Return (ready, reason); configured mappings fail closed if data cannot be verified."""
    if not mappings.strip():
        return True, ""
    try:
        roots = []
        for line in mappings.splitlines():
            if not line.strip():
                continue
            local, remote = line.strip().split(":", 1)
            if not Path(local).is_absolute() or not PurePosixPath(remote).is_absolute():
                return False, "源路径映射必须使用绝对路径"
            roots.append((Path(local).resolve(), PurePosixPath(remote)))
        save = PurePosixPath(torrent.get("save_path") or "")
        matches = [(local, remote) for local, remote in roots if save.is_relative_to(remote)]
        if not matches:
            return False, "源路径未匹配配置的映射"
        local, remote = max(matches, key=lambda pair: len(pair[1].parts))
        base = (local / str(save.relative_to(remote))).resolve()
        if not base.is_relative_to(local) or not base.is_dir():
            return False, "源下载目录不存在或越过映射边界"
        content = PurePosixPath(torrent.get("content_path") or "")
        if not content.is_relative_to(remote):
            return False, "源内容路径未匹配配置的映射"
        content_path = (local / str(content.relative_to(remote))).resolve()
        if not content_path.is_relative_to(local) or not content_path.exists():
            return False, "源内容已不存在，下载器完成状态可能过期"
        files = get_files(torrent.get("hash"))
        if not files:
            return False, "无法读取源种子的文件清单"
        for item in files:
            name = PurePosixPath(item.get("name") or "")
            if not name.parts or name.is_absolute() or ".." in name.parts:
                return False, "源文件清单包含无效路径"
            path = (base / str(name)).resolve()
            if not path.is_relative_to(local) or not path.is_file():
                return False, "源文件缺失或越过映射边界"
            if path.stat().st_size != int(item["size"]):
                return False, "源文件大小与种子记录不一致"
        return True, ""
    except (OSError, ValueError, TypeError, KeyError):
        return False, "源文件或路径映射无法核实"
