"""
孤立下载文件检查与删除；仅由 UnwatchedCleanup 插件调用。

扫描所有已配置下载器，媒体库文件按设备号和 inode 保护，执行前重新核对。
"""
import hashlib
import json
import os
import stat
from pathlib import Path, PurePosixPath


def digest(value):
    """生成计划和文件身份的稳定摘要。"""
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def overlaps(left, right):
    """判断两个绝对路径是否相同或互相包含。"""
    a, b = PurePosixPath(left), PurePosixPath(right)
    return a == b or a in b.parents or b in a.parents


def parse_mappings(text):
    """解析显式下载目录映射，拒绝根目录、上级路径及交叉映射。"""
    mappings = []
    for line in str(text or "").splitlines():
        if not line.strip():
            continue
        root, separator, aliases = line.partition(":")
        paths = [root.strip(), *[x.strip() for x in aliases.split(",")]]
        if not separator or any(not p or not PurePosixPath(p).is_absolute() or ".." in PurePosixPath(p).parts or len(PurePosixPath(p).parts) < 3 for p in paths):
            raise ValueError("下载目录映射需为具体绝对路径：MP目录:下载器目录1,下载器目录2")
        mapping = {"root": str(PurePosixPath(paths[0])), "aliases": list(dict.fromkeys(str(PurePosixPath(p)) for p in paths[1:]))}
        for other in mappings:
            if overlaps(mapping["root"], other["root"]) or any(overlaps(a, b) for a in mapping["aliases"] for b in other["aliases"]):
                raise ValueError("下载目录映射不能重复或互相嵌套")
        mappings.append(mapping)
    if not mappings:
        raise ValueError("未配置孤立下载文件目录")
    return mappings


def _value(item, name, default=None):
    """兼容下载器返回的字典及对象。"""
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def inventory(helper, stopped=lambda: False):
    """读取全部下载器任务路径；包括停用配置，任一无法核对就停止。"""
    configs = helper.get_configs(include_disabled=True)
    services = helper.get_services()
    names = set(configs) | set(services)
    if not names:
        raise ValueError("没有可核对的下载器配置，停止孤立文件清理")
    paths, counts = [], {}
    for name in sorted(names):
        if stopped():
            raise ValueError("配置已变更，停止清理")
        service = services.get(name)
        if not service or service.type not in ("qbittorrent", "transmission"):
            raise ValueError(f"下载器 {name} 无法完整核对，停止清理")
        try:
            client = service.instance
            if service.type == "transmission":
                arguments = list(dict.fromkeys([*client._trarg, "files"]))
                torrents = client.trc.get_torrents(arguments=arguments)
            else:
                torrents, error = client.get_torrents()
                if error:
                    raise ValueError()
            if torrents is None:
                raise ValueError()
            for torrent in torrents:
                if stopped():
                    raise ValueError()
                if service.type == "qbittorrent":
                    current = [_value(torrent, "content_path")]
                else:
                    directory = _value(torrent, "download_dir")
                    files = torrent.get_files()
                    if not directory or not files:
                        raise ValueError()
                    current = []
                    for entry in files:
                        filename = _value(entry, "name")
                        if not filename or PurePosixPath(filename).is_absolute() or ".." in PurePosixPath(filename).parts:
                            raise ValueError()
                        current.append(str(PurePosixPath(directory) / filename))
                if not current or any(not path or not PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts for path in current):
                    raise ValueError()
                paths.extend(current)
            counts[name] = len(torrents)
        except Exception:
            # 下载器异常可能包含凭据，持久记录只保存可公开的说明。
            raise ValueError(f"下载器 {name} 清单读取不完整，本轮不清理") from None
    return paths, counts


def scan(config, helper, now, stopped=lambda: False):
    """在完整引用清单和静置时间保护下，生成直接子目录或单文件候选。"""
    mappings = parse_mappings(config["download_dirs"])
    libraries = config["library_dirs"]
    if not libraries:
        raise ValueError("未配置媒体库保护目录，停止孤立文件清理")
    paths, services = inventory(helper, stopped)
    protected = library_identities(libraries, stopped)
    rows, warnings = [], []
    for mapping in mappings:
        root = Path(mapping["root"])
        if any(overlaps(root, library) for library in libraries):
            raise ValueError("下载源目录与媒体库目录重叠，停止清理")
        info = root_stat(root)
        references = []
        for source in paths:
            path = PurePosixPath(source)
            for alias in [mapping["root"], *mapping["aliases"]]:
                base = PurePosixPath(alias)
                if path == base or base in path.parents:
                    references.append(str(PurePosixPath(root) / path.relative_to(base)))
                elif path in base.parents:
                    references.append(str(root))
        for path in sorted(root.iterdir()):
            if stopped():
                raise ValueError("配置已变更，停止清理")
            if path.name.startswith(".") or any(overlaps(path, reference) for reference in references):
                continue
            try:
                fp = fingerprint(path, info.st_dev, protected=protected, stopped=stopped, allow_partial=True)
                if now - fp["newest"] < config["min_age_days"] * 86400:
                    continue
                rows.append({"id": digest(str(path)), "name": path.name, "path": str(path), "root": str(root),
                             "device": info.st_dev, "root_inode": info.st_ino, "status": "candidate", **fp})
            except (OSError, ValueError) as error:
                if isinstance(error, ValueError) and str(error) == "文件被媒体库引用":
                    continue
                warnings.append(f"保留 {path.name}：文件内容无法完整核对")
            if len(rows) > 1000:
                raise ValueError("候选超过 1000 项，请缩小下载目录范围")
    return {"rows": rows, "warnings": warnings[:30], "services": services}


PARTIAL = (".!qb", ".part", ".partial", ".tmp", ".!ut")

def root_stat(root):
    """检查根目录存在且未经过软链接。"""
    path = Path(root)
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError("源目录不存在、不是目录或包含软链接")
    return path.stat()


def fingerprint(path, device, *, limit=50000, stopped=lambda: False, protected=None, include_records=False, allow_partial=False):
    """记录完整子树的身份和最新写入时间，供删除前重新核对。"""
    records, size, newest = [], 0, 0
    stack = [Path(path)]
    while stack:
        if stopped():
            raise ValueError("检测或操作已停止")
        entry = stack.pop()
        info = entry.lstat()
        if info.st_dev != device or stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise ValueError("包含软链接、挂载点或特殊文件，保留源文件")
        if os.path.ismount(entry):
            raise ValueError("包含挂载点，保留源文件")
        if not allow_partial and entry.name.casefold().endswith(PARTIAL):
            raise ValueError("包含未完成下载文件，保留源文件")
        records.append((str(entry.relative_to(path)), info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode))
        newest = max(newest, info.st_mtime, info.st_ctime)
        if stat.S_ISREG(info.st_mode):
            if protected is not None and (info.st_dev, info.st_ino) in protected:
                raise ValueError("文件被媒体库引用")
            size += info.st_size
        else:
            stack.extend(entry.iterdir())
        if len(records) > limit:
            raise ValueError("目录条目过多，请单独检查")
    result = {"signature": digest(sorted(records)), "size": size, "newest": newest,
              "inode": Path(path).lstat().st_ino, "entries": len(records)}
    if include_records:
        result["records"] = records
    return result


def library_identities(directories, stopped=lambda: False):
    """读取媒体库的设备号及 inode，引用清单不可读时停止清理。"""
    identities = set()
    def failed(error):
        raise ValueError("媒体库目录不可完整读取，停止源文件清理") from error
    for directory in directories:
        root_stat(directory)
        for parent, children, names in os.walk(directory, followlinks=False, onerror=failed):
            if stopped():
                raise ValueError("检测已停止")
            if any((Path(parent) / child).is_symlink() for child in children):
                raise ValueError("媒体库含软链接目录，无法完整核对，停止清理")
            for name in names:
                info = os.stat(Path(parent) / name)
                if stat.S_ISREG(info.st_mode):
                    identities.add((info.st_dev, info.st_ino))
    return identities


def purge_source(row, *, stopped=lambda: False):
    """仅删除二次核对过的原始条目，使用目录描述符防止路径替换。"""
    root = Path(row["root"])
    info = root_stat(root)
    if (info.st_dev, info.st_ino) != (row["device"], row["root_inode"]):
        raise ValueError("源目录身份已改变")
    path = Path(row["path"])
    if path.parent != root or path.name.startswith("."):
        raise ValueError("候选路径无效")
    snapshot = fingerprint(path, info.st_dev, stopped=stopped, include_records=True, allow_partial=True)
    if snapshot["signature"] != row["signature"]:
        raise ValueError("文件已变化，等待重新检测")
    # 下载完成后可能刚建立新的媒体库硬链接，最后一步也保留多链接文件。
    for relative, inode, size, mtime, ctime, mode in snapshot["records"]:
        if stat.S_ISREG(mode) and (path / relative).stat().st_nlink > 1:
            raise ValueError("文件已有新的硬链接引用，保留")
    expected = {relative: inode for relative, inode, *_ in snapshot["records"]}
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root_fd = os.open(root, flags)
    try:
        if (os.fstat(root_fd).st_dev, os.fstat(root_fd).st_ino) != (row["device"], row["root_inode"]):
            raise ValueError("源目录已替换")
        records = sorted(snapshot["records"], key=lambda item: len(Path(item[0]).parts), reverse=True)
        for relative, inode, size, mtime, ctime, mode in records:
            if stopped():
                raise ValueError("操作已停止，剩余文件保留")
            parts = (path.name, *Path(relative).parts)
            parent_fd = os.dup(root_fd)
            try:
                for index, name in enumerate(parts[:-1]):
                    child_fd = os.open(name, flags, dir_fd=parent_fd)
                    os.close(parent_fd)
                    parent_fd = child_fd
                    parent_relative = str(Path(*parts[1:index + 1])) if index else "."
                    directory = os.fstat(parent_fd)
                    if directory.st_dev != row["device"] or directory.st_ino != expected[parent_relative]:
                        raise ValueError("父目录已替换")
                current = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
                if (current.st_dev, current.st_ino, current.st_mode) != (row["device"], inode, mode):
                    raise ValueError("待删条目已替换")
                if stat.S_ISDIR(mode):
                    os.rmdir(parts[-1], dir_fd=parent_fd)
                else:
                    if current.st_nlink != 1 or (current.st_size, current.st_mtime_ns, current.st_ctime_ns) != (size, mtime, ctime):
                        raise ValueError("文件已变化或新增硬链接，保留")
                    os.unlink(parts[-1], dir_fd=parent_fd)
            finally:
                os.close(parent_fd)
        os.fsync(root_fd)
    finally:
        os.close(root_fd)
