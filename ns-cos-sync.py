#!/usr/bin/env python3
# Copyright (c) 2026 Lionel Guo
# Email: lionelliguo@gmail.com

"""Synchronize objects between Akamai NetStorage and Tencent Cloud COS.

See README.md for configuration, usage, path mapping, ZIP handling, and
incremental-state behavior.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests import Response

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = SCRIPT_DIR / "ns-cos-config.json"
CHUNK_SIZE = 1024 * 1024


# Basic utilities

def info(msg: str) -> None:
    print(msg, flush=True)


def log(args: argparse.Namespace, msg: str) -> None:
    if not getattr(args, "quiet", False):
        info(msg)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_atomic(path: str | Path, data: Dict[str, Any]) -> None:
    p = Path(path)
    if p.parent:
        p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def default_config() -> Dict[str, Any]:
    return {
        "netstorage": {
            "host": "edgeone-nsu.akamaihd.net",
            "cp_code": "1990447",
            "key_name": "edgeone",
            "key_secret": "",
        },
        "cos": {
            "secret_id": "",
            "secret_key": "",
            "region": "ap-singapore",
            "bucket": "netstorage",
            "appid": "1312684008",
            "max_keys": 1000,
            "expire": 7200,
        },
        "sync": {
            "strategy": "incremental",
            "detect": "etag",
            "overwrite": True,
            "verify_target_exists": True,
            "copy_dir_markers": True,
            "prune_state": True,
            "fail_log": "failed_sync.log",
            "success_log": "success-files.log",
            "failed_files_log": "failed-files.log",
            "failed_dirs_log": "failed-dirs.log",
            "skip_success_log": True,
            "copy_file_attempts": 3,
            "read_list_cache": False,
            "write_list_cache": True,
            "list_cache_file": "list-dir-cache-{cp_code}-{source_hash}.json",
            "prefer_list_metadata": True,
            "stat_if_list_metadata_incomplete": False,
            "extract_zip_on_ns_to_cos": False,
            "zip_extract_to_folder": True,
            "zip_upload_original": False,
            "add_cp_code_to_cos_path": False,
            "strip_redundant_zip_root": True,
        },
        "state": {
            "state_mode": "sharded",
            "state_dir": "state_shards",
            "state_meta_file": "state-meta.json",
            "state_file": "state.json",
        },
        "schedule": {
            "enabled": False,
            "interval_seconds": 300,
        },
        "retry": {
            "max_attempts": 3,
            "timeout_seconds": 120,
            "base_sleep_seconds": 1.0,
            "max_sleep_seconds": 30.0,
        },
        "transfer": {
            "workers": 2,
            "temp_dir": None,
            "keep_temp": False,
            "skip_tls_verify": False,
            "curl_log": None,
            "quiet": False,
            "continue_on_error": True,
            "content_type": None,
            "ns_list_retries": 3,
            "ns_list_retry_sleep": 3.0,
            "ns_list_max_retry_sleep": 30.0,
            "skip_failed_dirs": False,
            "list_progress_every_dirs": 1000,
            "select_progress_every_files": 5000,
            "copy_progress_every_files": 100,
            "progress_interval_seconds": 30.0,
        },
    }


def load_config(path: str | Path) -> Dict[str, Any]:
    cfg = default_config()
    p = Path(path)
    if p.exists():
        cfg = deep_merge(cfg, load_json(p))
    return cfg


def append_fail_log(path: str, src: str, dst: str, err: Exception) -> None:
    try:
        p = Path(path)
        if p.parent:
            p.parent.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {src} -> {dst} | {repr(err)}\n")
    except Exception:
        pass


def append_file_log(path: str, src: str, dst: str, status: str = "OK", detail: str = "") -> None:
    """Append a compact per-file success/failure record.

    The existing fail_log keeps detailed traceback/error information. These
    success/failed files logs are intentionally simple so they can be counted
    with wc -l and reviewed with tail/grep.
    """
    try:
        if not path:
            return
        p = Path(path)
        if p.parent:
            p.parent.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        extra = f" | {detail}" if detail else ""
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {status} | {src} -> {dst}{extra}\n")
    except Exception:
        pass


def append_success_file_log(args: argparse.Namespace, src: str, dst: str) -> None:
    append_file_log(getattr(args, "success_log", "success-files.log"), src, dst, "OK")


def append_failed_file_log(args: argparse.Namespace, src: str, dst: str, err: Exception) -> None:
    append_file_log(getattr(args, "failed_files_log", "failed-files.log"), src, dst, "FAILED", repr(err))


def append_failed_dir_log(args: argparse.Namespace, src_dir: str, err: Exception) -> None:
    append_file_log(getattr(args, "failed_dirs_log", "failed-dirs.log"), src_dir, "LIST_DIR", "FAILED_DIR", repr(err))


def file_log_key(src: str, dst: str) -> str:
    return f"{src} -> {dst}"


def load_success_keys(path: str) -> Set[str]:
    """Load compact success log keys so already copied files can be skipped."""
    keys: Set[str] = set()
    try:
        if not path or not Path(path).exists():
            return keys
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if " | OK | " not in line or " -> " not in line:
                    continue
                keys.add(line.split(" | OK | ", 1)[1])
    except Exception:
        pass
    return keys


def fmt_bytes(n: int) -> str:
    try:
        n = int(n or 0)
    except Exception:
        n = 0
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    v = float(n)
    for u in units:
        if v < 1024 or u == units[-1]:
            return f"{v:.2f}{u}" if u != "B" else f"{int(v)}B"
        v /= 1024.0
    return f"{n}B"


def fmt_eta(seconds: float) -> str:
    if seconds < 0 or seconds == float("inf"):
        return "unknown"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{sec:02d}s"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


def touch_log_file(path: str) -> None:
    """Create an empty log file if it does not exist. Do not truncate existing logs."""
    try:
        if not path:
            return
        p = Path(path)
        if p.parent:
            p.parent.mkdir(parents=True, exist_ok=True)
        p.touch(exist_ok=True)
    except Exception:
        pass


def init_file_logs(args: argparse.Namespace) -> None:
    """Ensure success/failed compact file logs always exist at startup."""
    touch_log_file(getattr(args, "success_log", "success-files.log"))
    touch_log_file(getattr(args, "failed_files_log", "failed-files.log"))
    touch_log_file(getattr(args, "failed_dirs_log", "failed-dirs.log"))



def safe_cache_source_hash(source: str) -> str:
    """Short stable hash for using source path in cache filenames."""
    return hashlib.sha1((source or "").encode("utf-8")).hexdigest()[:12]


def resolved_list_cache_file(args: argparse.Namespace) -> str:
    """Resolve list cache file path.

    Supports placeholders in config:
      {cp_code}, {source}, {source_hash}, {command}
    Example: list-dir-cache-{cp_code}-{source_hash}.json
    """
    tpl = getattr(args, "list_cache_file", "list-dir-cache-{cp_code}-{source_hash}.json") or "list-dir-cache-{cp_code}-{source_hash}.json"
    source = getattr(args, "ns_source", getattr(args, "cos_source", "")) or ""
    source_clean = normalize_ns_path(source).replace("/", "_") or "root"
    return tpl.format(
        cp_code=str(getattr(args, "ns_cp_code", "") or "unknown"),
        source=source_clean,
        source_hash=safe_cache_source_hash(source),
        command=str(getattr(args, "command", "") or "sync"),
    )


def save_list_cache(args: argparse.Namespace, source_dir: str, files: List[Dict[str, Any]], dirs: List[str]) -> None:
    """Persist NetStorage dir-list result so later runs can skip expensive listing."""
    try:
        path = resolved_list_cache_file(args)
        p = Path(path)
        if p.parent:
            p.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": 1,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "created_epoch": int(time.time()),
            "command": getattr(args, "command", ""),
            "ns_host": getattr(args, "ns_host", ""),
            "ns_cp_code": str(getattr(args, "ns_cp_code", "")),
            "source_dir": source_dir,
            "file_count": len(files),
            "dir_count": len(dirs),
            "files": files,
            "dirs": dirs,
        }
        save_json_atomic(p, data)
        info(f"[LIST CACHE WRITE] file={path} dirs={len(dirs)} files={len(files)}")
    except Exception as exc:
        info(f"[WARN] failed to write list cache: {exc}")


def load_list_cache(args: argparse.Namespace, source_dir: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Load NetStorage dir-list result from cache file."""
    path = resolved_list_cache_file(args)
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"list cache not found: {path}")
    data = load_json(p)
    cached_cp = str(data.get("ns_cp_code", ""))
    cached_source = str(data.get("source_dir", ""))
    if cached_cp and cached_cp != str(getattr(args, "ns_cp_code", "")):
        raise ValueError(f"list cache cp_code mismatch: cache={cached_cp}, current={getattr(args, 'ns_cp_code', '')}")
    if cached_source and ensure_dir_prefix(cached_source) != ensure_dir_prefix(source_dir):
        raise ValueError(f"list cache source mismatch: cache={cached_source}, current={source_dir}")
    files = data.get("files", [])
    dirs = data.get("dirs", [])
    if not isinstance(files, list) or not isinstance(dirs, list):
        raise ValueError("invalid list cache format: files/dirs must be lists")
    info(
        f"[LIST CACHE READ] file={path} dirs={len(dirs)} files={len(files)} "
        f"created_at={data.get('created_at', '-') }"
    )
    # Normalize to tolerate older or manually edited caches.
    norm_files = []
    for f in files:
        if isinstance(f, dict) and f.get("path"):
            ff = dict(f)
            ff["path"] = normalize_ns_path(str(ff["path"]))
            ff["size"] = int(ff.get("size") or 0)
            ff["etag"] = (ff.get("etag") or "").strip('"')
            ff["last_modified_raw"] = ff.get("last_modified_raw") or ""
            norm_files.append(ff)
    norm_dirs = sorted({ensure_dir_prefix(str(d)) for d in dirs if str(d).strip()})
    return sorted(norm_files, key=lambda x: x["path"]), norm_dirs

def progress_bar(done: int, total: int, width: int = 30) -> str:
    """Return a compact ASCII progress bar for known-total phases."""
    if total <= 0:
        return "[" + "-" * width + "]"
    done = max(0, min(done, total))
    filled = int(width * done / total)
    if done > 0 and filled == 0:
        filled = 1
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def utc_http_date() -> str:
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())


def parse_http_date_to_epoch(v: str) -> int:
    if v is None:
        return 0

    v = str(v).strip()
    if not v:
        return 0

    # NetStorage directory/stat metadata may return Unix epoch seconds,
    # for example: "1777979747". Treat it as seconds directly.
    if re.fullmatch(r"\d{10}", v):
        return int(v)

    # Also tolerate Unix epoch milliseconds, for example: "1777979747000".
    if re.fullmatch(r"\d{13}", v):
        return int(int(v) / 1000)

    for fmt in ("%a, %d %b %Y %H:%M:%S GMT", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            if fmt.endswith("GMT"):
                # HTTP date is GMT/UTC. Use calendar.timegm to avoid local timezone shifts.
                import calendar
                return int(calendar.timegm(time.strptime(v, fmt)))
            dt = datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
    try:
        s = v.replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


def quote(s: Any) -> str:
    return urllib.parse.quote(str(s), safe="-_.~")


def quote_path(path: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="-_.~") for part in path.split("/"))


def qs_for_url(params: Dict[str, Any]) -> str:
    items = sorted((k, v) for k, v in params.items() if v is not None)
    return "&".join(f"{quote(k)}={quote(v)}" for k, v in items)


def normalize_ns_path(path: str) -> str:
    return (path or "").strip().lstrip("/")


def normalize_cos_key(key: str) -> str:
    return (key or "").strip().lstrip("/")


def ensure_dir_prefix(path: str) -> str:
    p = (path or "").strip().lstrip("/")
    return p if p.endswith("/") else p + "/"


def join_key(prefix: str, name: str) -> str:
    """Join COS key prefix and object name without creating a leading slash.

    COS object keys should not start with '/'. This also makes root destination
    prefixes work correctly, for example join_key('/', 'uat/a.png') returns
    'uat/a.png', not '/uat/a.png'.
    """
    p = normalize_cos_key(prefix).strip("/")
    n = normalize_cos_key(name).strip("/")
    if p and n:
        return p + "/" + n
    if p:
        return p
    return n


def join_ns_path(prefix: str, name: str) -> str:
    """Join NetStorage path prefix and name with exactly one leading slash."""
    p = normalize_ns_path(prefix).strip("/")
    n = normalize_ns_path(name).strip("/")
    if p and n:
        return "/" + p + "/" + n
    if p:
        return "/" + p
    if n:
        return "/" + n
    return "/"


def relative_under_prefix(path: str, prefix: str) -> str:
    """Return path relative to prefix while correctly handling root '/'.

    Examples:
      path=/uat/images/a.png, prefix=/             -> uat/images/a.png
      path=uat/images/a.png,  prefix=/             -> uat/images/a.png
      path=/uat/images/a.png, prefix=/uat/         -> images/a.png
      path=/uat/images/a.png, prefix=/uat/images/  -> a.png
    """
    p = normalize_ns_path(path)
    base = normalize_ns_path(prefix).rstrip("/")

    if not base:
        return p
    if p == base:
        return ""
    if p.startswith(base + "/"):
        return p[len(base) + 1:]
    return p


def is_recursive(source: str, recursive_flag: bool) -> bool:
    return recursive_flag or str(source).endswith("/")


def bool_value(cli_val: Optional[bool], default: bool) -> bool:
    if cli_val is None:
        return default
    return bool(cli_val)


def require(v: Any, name: str) -> Any:
    if v is None or v == "":
        raise ValueError(f"Missing required config/argument: {name}")
    return v


# Retry helpers

def backoff_delay_seconds(base: float, max_sleep: float, attempt: int) -> float:
    """Return exponential backoff delay with jitter.

    attempt is 1-based. With base=2, delays are roughly:
      attempt 1 -> 2s +/- jitter
      attempt 2 -> 4s +/- jitter
      attempt 3 -> 8s +/- jitter
    The value is capped by max_sleep.
    """
    delay = min(max_sleep, base * (2 ** max(0, attempt - 1)))
    delay *= 0.7 + random.random() * 0.6
    return delay


def sleep_with_backoff(base: float, max_sleep: float, attempt: int) -> float:
    delay = backoff_delay_seconds(base, max_sleep, attempt)
    time.sleep(delay)
    return delay


def request_with_retry(
    args: argparse.Namespace,
    method: str,
    url: str,
    headers: Dict[str, str],
    label: str,
    *,
    data: Any = None,
    stream: bool = False,
    attempts: Optional[int] = None,
) -> Response:
    max_attempts = int(attempts if attempts is not None else args.retries)
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                data=data,
                timeout=float(args.timeout),
                verify=not bool(args.skip_tls_verify),
                stream=stream,
            )
            if resp.status_code in (500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
            else:
                return resp
        except Exception as exc:
            last_err = exc
        if attempt < max_attempts:
            delay = backoff_delay_seconds(float(args.retry_sleep), float(args.retry_max_sleep), attempt)
            log(args, f"{label} error on attempt {attempt}/{max_attempts}: {last_err}; exponential_backoff_sleep={delay:.1f}s")
            time.sleep(delay)
    raise last_err if last_err else RuntimeError(f"{label} failed")


def ensure_success(resp: Response, label: str) -> None:
    if 200 <= resp.status_code < 300:
        return
    body = ""
    try:
        body = resp.text[:2000]
    except Exception:
        pass
    raise RuntimeError(f"{label} failed: HTTP {resp.status_code}; body={body}")


# Akamai NetStorage operations

def ns_request_path(args: argparse.Namespace, remote: str) -> str:
    return f"/{args.ns_cp_code}/{quote_path(normalize_ns_path(remote))}"


def ns_auth_headers(args: argparse.Namespace, request_path: str, action: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    ts = int(time.time())
    unique_id = random.randint(100000000, 999999999)
    auth_data = f"5, 0.0.0.0, 0.0.0.0, {ts}, {unique_id}, {args.ns_key_name}"
    sign_string = f"{auth_data}{request_path}\nx-akamai-acs-action:{action}\n"
    auth_sign = base64.b64encode(
        hmac.new(args.ns_key_secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    headers = {
        "X-Akamai-ACS-Action": action,
        "X-Akamai-ACS-Auth-Data": auth_data,
        "X-Akamai-ACS-Auth-Sign": auth_sign,
        "Accept-Encoding": "identity",
        "Connection": "close",
    }
    if extra:
        headers.update(extra)
    return headers


def ns_get(args: argparse.Namespace, remote: str, action: str, label: str, *, stream: bool = False, attempts: Optional[int] = None) -> Response:
    path = ns_request_path(args, remote)
    url = f"https://{args.ns_host}{path}"
    log(args, f"{label}: {url}")
    headers = ns_auth_headers(args, path, action)
    return request_with_retry(args, "GET", url, headers, label, stream=stream, attempts=attempts)


def ns_download_to_file(args: argparse.Namespace, remote_file: str, local_file: Path) -> Response:
    resp = ns_get(args, remote_file, "version=1&action=download", "NetStorage download", stream=True)
    ensure_success(resp, "NetStorage download")
    ensure_parent(local_file)
    with open(local_file, "wb") as f:
        for chunk in resp.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)
    return resp


def ns_upload_from_file(args: argparse.Namespace, local_file: Path, remote_file: str) -> Response:
    path = ns_request_path(args, remote_file)
    url = f"https://{args.ns_host}{path}"
    log(args, f"NetStorage upload: {url}")
    headers = ns_auth_headers(args, path, "version=1&action=upload", {"Content-Length": str(local_file.stat().st_size)})
    with open(local_file, "rb") as f:
        resp = request_with_retry(args, "PUT", url, headers, "NetStorage upload", data=f)
    return resp


def ns_mkdir(args: argparse.Namespace, remote_dir: str) -> bool:
    path = ns_request_path(args, ensure_dir_prefix(remote_dir))
    url = f"https://{args.ns_host}{path}"
    log(args, f"NetStorage mkdir: {url}")
    headers = ns_auth_headers(args, path, "version=1&action=mkdir")
    resp = request_with_retry(args, "PUT", url, headers, "NetStorage mkdir")
    if resp.status_code in (200, 201, 204, 409):
        return True
    ensure_success(resp, "NetStorage mkdir")
    return True


def text_of_first(elem: ET.Element, names: Iterable[str]) -> str:
    for n in names:
        for child in elem.iter():
            tag = child.tag.split("}")[-1].lower()
            if tag == n.lower() and child.text:
                return child.text.strip()
    return ""


def ns_entry_path(raw: str, current_dir: str) -> str:
    raw = urllib.parse.unquote((raw or "").strip())
    if not raw:
        return ""
    raw = raw.replace("\\", "/")
    # Remove full URL if any
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = urllib.parse.urlparse(raw).path
    # Remove cp code prefix if present
    m = re.match(r"^/?\d+/(.*)$", raw)
    if m:
        raw = m.group(1)
    raw = raw.lstrip("/")
    cur = ensure_dir_prefix(current_dir)
    if "/" not in raw and not raw.startswith(cur):
        raw = cur + raw
    return raw


def parse_ns_dir(args: argparse.Namespace, xml_bytes: bytes, current_dir: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    files: Dict[str, Dict[str, Any]] = {}
    dirs: Set[str] = set()
    if not xml_bytes.strip():
        return [], []
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse NetStorage dir XML: {exc}; body={xml_bytes[:500]!r}")

    for e in root.iter():
        tag = e.tag.split("}")[-1].lower()
        attrs = {k.lower(): v for k, v in e.attrib.items()}
        typ = (attrs.get("type") or attrs.get("filetype") or attrs.get("kind") or tag).lower()
        raw_name = (
            attrs.get("name") or attrs.get("filename") or attrs.get("file") or attrs.get("path") or attrs.get("target") or attrs.get("url") or
            text_of_first(e, ["name", "filename", "file", "path", "target", "url"])
        )
        if not raw_name:
            continue
        p = ns_entry_path(raw_name, current_dir)
        if not p:
            continue
        is_dir = typ in {"dir", "directory", "folder"} or p.endswith("/")
        is_file = typ in {"file", "contents", "object"} or (not is_dir and tag in {"file", "contents", "object"})
        if is_dir:
            dirs.add(ensure_dir_prefix(p))
        elif is_file:
            size_s = attrs.get("size") or text_of_first(e, ["size", "bytes", "filesize"])
            lm_s = attrs.get("mtime") or attrs.get("last-modified") or attrs.get("lastmodified") or text_of_first(e, ["mtime", "last-modified", "lastmodified", "last_modified"])
            etag = attrs.get("etag") or text_of_first(e, ["etag", "md5", "hash"])
            try:
                size = int(size_s) if size_s else 0
            except Exception:
                size = 0
            files[p] = {"path": p, "size": size, "last_modified_raw": lm_s or "", "etag": (etag or "").strip('"')}

    return sorted(files.values(), key=lambda x: x["path"]), sorted(dirs)


def ns_list_dir(args: argparse.Namespace, remote_dir: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    current = ensure_dir_prefix(remote_dir)
    resp = ns_get(args, current, "version=1&action=dir&format=xml", "NetStorage list dir", attempts=int(args.ns_list_retries))
    ensure_success(resp, "NetStorage list dir")
    return parse_ns_dir(args, resp.content, current)


def ns_walk(args: argparse.Namespace, source_dir: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    root = ensure_dir_prefix(source_dir)
    stack = [root]
    seen_dirs: Set[str] = set()
    all_dirs: Set[str] = {root}
    all_files: Dict[str, Dict[str, Any]] = {}

    started = time.time()
    last_report = started
    dirs_scanned = 0
    failed_dirs = 0
    progress_every = max(1, int(getattr(args, "list_progress_every_dirs", 1000) or 1000))
    progress_interval = max(1.0, float(getattr(args, "progress_interval_seconds", 30.0) or 30.0))

    info(f"[LIST START] root=/{root}")

    def report(force: bool = False) -> None:
        nonlocal last_report
        now = time.time()
        if not force and dirs_scanned % progress_every != 0 and (now - last_report) < progress_interval:
            return
        elapsed = max(0.001, now - started)
        info(
            "[LIST PROGRESS] "
            "bar=[scanning] "
            f"dirs_scanned={dirs_scanned} "
            f"dirs_pending={len(stack)} "
            f"dirs_found={len(all_dirs)} "
            f"files_found={len(all_files)} "
            f"failed_dirs={failed_dirs} "
            f"elapsed_sec={elapsed:.1f} "
            f"dirs_per_sec={dirs_scanned / elapsed:.2f}"
        )
        last_report = now

    while stack:
        cur = stack.pop()
        if cur in seen_dirs:
            continue
        seen_dirs.add(cur)
        dirs_scanned += 1
        try:
            files, dirs = ns_list_dir(args, cur)
        except Exception as exc:
            failed_dirs += 1
            info(f"FAILED: list NetStorage dir /{cur}: {exc}")
            append_fail_log(args.fail_log, cur, "LIST_DIR", exc)
            append_failed_dir_log(args, cur, exc)
            report(force=True)
            if args.skip_failed_dirs or args.continue_on_error:
                continue
            raise
        for f in files:
            all_files[f["path"]] = f
        for d in dirs:
            d = ensure_dir_prefix(d)
            if d not in seen_dirs:
                stack.append(d)
            all_dirs.add(d)
        report()

    elapsed = max(0.001, time.time() - started)
    info(
        "[LIST DONE] "
        f"dirs_scanned={dirs_scanned} "
        f"dirs_found={len(all_dirs)} "
        f"files_found={len(all_files)} "
        f"failed_dirs={failed_dirs} "
        f"elapsed_sec={elapsed:.1f} "
        f"dirs_per_sec={dirs_scanned / elapsed:.2f}"
    )
    return sorted(all_files.values(), key=lambda x: x["path"]), sorted(all_dirs)


def ns_stat_metadata(args: argparse.Namespace, remote_file: str, fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    fallback = fallback or {}
    try:
        resp = ns_get(args, remote_file, "version=1&action=stat&format=xml", "NetStorage stat")
        ensure_success(resp, "NetStorage stat")
        etag = (resp.headers.get("ETag") or fallback.get("etag") or "").strip('"')
        size = int(resp.headers.get("Content-Length") or fallback.get("size") or 0)
        lm_raw = resp.headers.get("Last-Modified") or fallback.get("last_modified_raw") or ""
        lm = parse_http_date_to_epoch(lm_raw)
        return {"etag": etag, "size": size, "last_modified": lm, "last_modified_raw": lm_raw}
    except Exception as exc:
        info(f"[WARN] NetStorage stat failed for {remote_file}: {exc}; using directory-list metadata fallback")
        etag = (fallback.get("etag") or "").strip('"')
        size = int(fallback.get("size") or 0)
        lm_raw = fallback.get("last_modified_raw") or ""
        return {"etag": etag, "size": size, "last_modified": parse_http_date_to_epoch(lm_raw), "last_modified_raw": lm_raw}


def metadata_from_list_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Build metadata from a NetStorage directory-list entry."""
    etag = (entry.get("etag") or "").strip('"')
    size = int(entry.get("size") or 0)
    lm_raw = entry.get("last_modified_raw") or ""
    return {
        "etag": etag,
        "size": size,
        "last_modified": parse_http_date_to_epoch(lm_raw),
        "last_modified_raw": lm_raw,
    }


def metadata_is_usable(meta: Dict[str, Any], detect: str = "etag") -> bool:
    """Return whether list metadata is good enough for incremental comparison."""
    if detect == "etag" and (meta.get("etag") or "").strip():
        return True
    # LastModified+Size fallback. Size can legitimately be 0 for empty files, so
    # LastModified is the main completeness signal.
    return bool(int(meta.get("last_modified", 0) or 0))


def choose_ns_metadata(args: argparse.Namespace, ns_remote: str, list_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Choose metadata for one NS file without always doing per-file stat.

    prefer_list_metadata=true:
      Use directory-list metadata first. Only call stat when
      stat_if_list_metadata_incomplete=true and the list metadata is not usable.

    prefer_list_metadata=false:
      Preserve the original behavior and call NetStorage stat.
    """
    list_meta = metadata_from_list_entry(list_entry)
    if getattr(args, "prefer_list_metadata", True):
        if getattr(args, "stat_if_list_metadata_incomplete", False) and not metadata_is_usable(list_meta, getattr(args, "detect", "etag")):
            return ns_stat_metadata(args, ns_remote, list_entry)
        return list_meta
    return ns_stat_metadata(args, ns_remote, list_entry)


# Tencent COS operations

def cos_host(args: argparse.Namespace) -> str:
    return f"{args.cos_bucket}-{args.cos_appid}.cos.{args.cos_region}.myqcloud.com"


def build_qsign_sha1(secret_id: str, secret_key: str, method: str, path: str, params: Dict[str, str], headers: Dict[str, str], expire: int) -> str:
    canon_headers = {quote(k).lower(): quote(v) for k, v in headers.items()}
    canon_params = {quote(k).lower(): quote(v) for k, v in params.items()}
    format_str = (
        f"{method.lower()}\n"
        f"{path}\n"
        f"{'&'.join(f'{k}={v}' for k, v in sorted(canon_params.items()))}\n"
        f"{'&'.join(f'{k}={v}' for k, v in sorted(canon_headers.items()))}\n"
    )
    now = int(time.time())
    sign_time = f"{now};{now + int(expire)}"
    sha1_hex = hashlib.sha1(format_str.encode("utf-8")).hexdigest()
    string_to_sign = f"sha1\n{sign_time}\n{sha1_hex}\n"
    sign_key = hmac.new(secret_key.encode("utf-8"), sign_time.encode("utf-8"), hashlib.sha1).hexdigest()
    signature = hmac.new(sign_key.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1).hexdigest()
    return (
        "q-sign-algorithm=sha1"
        f"&q-ak={secret_id}"
        f"&q-sign-time={sign_time}"
        f"&q-key-time={sign_time}"
        f"&q-header-list={';'.join(sorted(canon_headers.keys()))}"
        f"&q-url-param-list={';'.join(sorted(canon_params.keys()))}"
        f"&q-signature={signature}"
    )


def cos_signed_headers(args: argparse.Namespace, method: str, key: str = "", params: Optional[Dict[str, str]] = None, extra: Optional[Dict[str, str]] = None) -> Tuple[str, Dict[str, str]]:
    params = params or {}
    host = cos_host(args)
    path = "/" + normalize_cos_key(key) if key else "/"
    sign_path = path
    url_path = "/" + quote_path(normalize_cos_key(key)) if key else "/"
    url = f"https://{host}{url_path}"
    if params:
        url += "?" + qs_for_url(params)
    date = utc_http_date()
    headers_to_sign = {"date": date, "host": host}
    auth = build_qsign_sha1(
        args.cos_secret_id,
        args.cos_secret_key,
        method,
        sign_path,
        {k: str(v) for k, v in params.items() if v is not None},
        headers_to_sign,
        int(args.cos_expire),
    )
    headers = {"Host": host, "Date": date, "Authorization": auth, "Connection": "close"}
    if extra:
        headers.update(extra)
    return url, headers


def cos_upload_from_file(args: argparse.Namespace, key: str, local_file: Path, content_type: Optional[str] = None) -> Response:
    size = local_file.stat().st_size

    # Special handling for real 0-byte objects.  Some servers/proxies may close
    # a streamed PUT with an empty file object unexpectedly.  For size=0, upload
    # an explicit empty bytes payload instead, and DO NOT add a trailing slash to
    # the key. This preserves file objects such as ".../Shipping_files/0".
    if size == 0:
        info(f"[ZERO BYTE] COS upload empty object: {key}")
        return cos_upload_bytes(args, key, b"", content_type=content_type)

    extra = {"Content-Length": str(size)}
    if content_type or args.content_type:
        extra["Content-Type"] = content_type or args.content_type
    url, headers = cos_signed_headers(args, "PUT", key, extra=extra)
    log(args, f"COS upload: {url}")
    with open(local_file, "rb") as f:
        return request_with_retry(args, "PUT", url, headers, "COS upload", data=f)


def cos_upload_bytes(args: argparse.Namespace, key: str, data_bytes: bytes, content_type: Optional[str] = None) -> Response:
    extra = {"Content-Length": str(len(data_bytes))}
    if content_type or args.content_type:
        extra["Content-Type"] = content_type or args.content_type
    url, headers = cos_signed_headers(args, "PUT", key, extra=extra)
    log(args, f"COS upload: {url}")
    return request_with_retry(args, "PUT", url, headers, "COS upload", data=data_bytes)


def cos_put_empty_object(args: argparse.Namespace, key: str) -> bool:
    resp = cos_upload_bytes(args, ensure_dir_prefix(key), b"")
    ensure_success(resp, "COS put empty object")
    return True


def cos_download_to_file(args: argparse.Namespace, key: str, local_file: Path) -> Response:
    url, headers = cos_signed_headers(args, "GET", key)
    log(args, f"COS download: {url}")
    resp = request_with_retry(args, "GET", url, headers, "COS download", stream=True)
    ensure_success(resp, "COS download")
    ensure_parent(local_file)
    with open(local_file, "wb") as f:
        for chunk in resp.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)
    return resp


def cos_head(args: argparse.Namespace, key: str) -> Optional[Dict[str, Any]]:
    url, headers = cos_signed_headers(args, "HEAD", key)
    try:
        resp = request_with_retry(args, "HEAD", url, headers, "COS head", attempts=2)
        if resp.status_code == 404:
            return None
        ensure_success(resp, "COS head")
        return {
            "etag": (resp.headers.get("ETag") or "").strip('"'),
            "size": int(resp.headers.get("Content-Length") or 0),
            "last_modified": parse_http_date_to_epoch(resp.headers.get("Last-Modified") or ""),
            "last_modified_raw": resp.headers.get("Last-Modified") or "",
        }
    except Exception as exc:
        info(f"[WARN] COS head failed for {key}: {exc}")
        raise


def cos_exists(args: argparse.Namespace, key: str) -> bool:
    return cos_head(args, key) is not None


def parse_cos_list(xml_bytes: bytes) -> Tuple[List[Dict[str, Any]], bool, str]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse COS list XML: {exc}; body={xml_bytes[:500]!r}")
    def t(tag: str) -> str:
        n = root.find(f".//{{*}}{tag}")
        return (n.text or "") if n is not None else ""
    objs: List[Dict[str, Any]] = []
    for c in root.findall(".//{*}Contents"):
        key = c.findtext("{*}Key", "") or ""
        size = int(c.findtext("{*}Size", "0") or 0)
        lm = c.findtext("{*}LastModified", "") or ""
        etag = (c.findtext("{*}ETag", "") or "").strip('"')
        objs.append({"key": key, "size": size, "last_modified_raw": lm, "last_modified": parse_http_date_to_epoch(lm), "etag": etag})
    return objs, t("IsTruncated").lower() == "true", t("NextContinuationToken")


def cos_list_prefix(args: argparse.Namespace, prefix: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    token = None
    out: List[Dict[str, Any]] = []
    while True:
        params = {"list-type": "2", "max-keys": str(args.cos_max_keys), "prefix": normalize_cos_key(prefix), "continuation-token": token}
        params = {k: v for k, v in params.items() if v is not None}
        url, headers = cos_signed_headers(args, "GET", "", params=params)
        log(args, f"COS list prefix: {url}")
        resp = request_with_retry(args, "GET", url, headers, "COS list prefix")
        ensure_success(resp, "COS list prefix")
        objs, truncated, next_token = parse_cos_list(resp.content)
        out.extend(objs)
        if not truncated:
            break
        if not next_token:
            raise RuntimeError("COS ListObjectsV2 returned IsTruncated=true but missing NextContinuationToken")
        token = next_token
    dirs = sorted({o["key"] for o in out if o["key"].endswith("/") and int(o.get("size", 0)) == 0})
    return out, dirs


# Incremental state

def safe_shard_name(name: str) -> str:
    """Return a filesystem-safe shard name."""
    name = (name or "").strip().strip("/")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = name.strip("._-")
    if not name:
        return "_root"
    # Keep file names readable while avoiding extremely long shard file names.
    return name[:120]


def first_level_shard(source_id: str, base_prefix: str = "") -> str:
    """
    Shard state by the first-level path under the configured source prefix.

    Examples when base_prefix is /test/:
      /test/images/a.png -> images
      /test/css/main.css -> css
      /test/a.txt        -> _root
    """
    src = normalize_ns_path(source_id)
    base = normalize_ns_path(base_prefix)

    if base and src == base:
        rel = ""
    elif base and src.startswith(base.rstrip("/") + "/"):
        rel = src[len(base.rstrip("/")) + 1:]
    else:
        rel = src

    parts = [x for x in rel.split("/") if x]
    if len(parts) >= 2:
        return safe_shard_name(parts[0])
    return "_root"


def state_namespace(args: argparse.Namespace) -> str:
    cp_code = str(getattr(args, "ns_cp_code", "") or "")
    return (
        f"{args.command}:cp={cp_code}:"
        f"{normalize_ns_path(getattr(args, 'ns_source', getattr(args, 'ns_dest', '')))}:"
        f"{normalize_cos_key(getattr(args, 'cos_dest', getattr(args, 'cos_source', '')))}"
    )


def state_object_id(args: argparse.Namespace, source_id: str) -> str:
    return f"{state_namespace(args)}::{source_id}"


class StateStore:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.mode = args.state_mode
        self.meta: Dict[str, Any] = {}
        self.single: Dict[str, Any] = {}
        self.single_objects: Dict[str, Any] = {}
        self.shards: Dict[str, Dict[str, Any]] = {}
        self.dirty: Set[str] = set()
        self.seen: Set[str] = set()
        if args.strategy != "incremental":
            return
        if self.mode == "sharded":
            p = Path(args.state_meta_file)
            if p.exists():
                try:
                    self.meta = load_json(p)
                except Exception as exc:
                    info(f"[WARN] failed to load state meta {p}: {exc}; starting with empty meta")
                    self.meta = {}
        else:
            p = Path(args.state_file)
            if p.exists():
                try:
                    self.single = load_json(p)
                    self.single_objects = self.single.get("objects", {}) if isinstance(self.single.get("objects"), dict) else {}
                except Exception as exc:
                    info(f"[WARN] failed to load state file {p}: {exc}; starting with empty state")
                    self.single = {}
                    self.single_objects = {}

    def shard_id(self, key: str) -> str:
        # State keys are in the form: <namespace>::<source_id>.
        # Use only the source object path for sharding, not the command namespace.
        source_id = key.split("::", 1)[1] if "::" in key else key
        if getattr(self.args, "command", "") == "ns-to-cos":
            base_prefix = getattr(self.args, "ns_source", "")
        elif getattr(self.args, "command", "") == "cos-to-ns":
            base_prefix = getattr(self.args, "cos_source", "")
        else:
            base_prefix = ""
        return first_level_shard(source_id, base_prefix)

    def shard(self, key: str) -> Dict[str, Any]:
        sid = self.shard_id(key)
        if sid not in self.shards:
            p = Path(self.args.state_dir) / f"{sid}.json"
            if p.exists():
                try:
                    data = load_json(p)
                except Exception as exc:
                    info(f"[WARN] failed to load state shard {p}: {exc}; starting with an empty shard")
                    data = {"objects": {}}
            else:
                data = {"objects": {}}
            if not isinstance(data.get("objects"), dict):
                data["objects"] = {}
            self.shards[sid] = data
        return self.shards[sid]

    def get(self, key: str) -> Dict[str, Any]:
        if self.args.strategy != "incremental":
            return {}
        if self.mode == "sharded":
            return self.shard(key).get("objects", {}).get(key, {})
        return self.single_objects.get(key, {})

    def changed(self, key: str, meta: Dict[str, Any]) -> bool:
        if self.args.strategy == "full":
            return True
        prev = self.get(key)
        if not prev:
            return True
        if self.args.detect == "etag":
            cur_etag = (meta.get("etag") or "").strip()
            old_etag = (prev.get("etag") or "").strip()
            if cur_etag and old_etag:
                return cur_etag != old_etag
        return int(meta.get("last_modified", 0) or 0) != int(prev.get("last_modified", 0) or 0) or int(meta.get("size", 0) or 0) != int(prev.get("size", 0) or 0)

    def set(self, key: str, meta: Dict[str, Any]) -> None:
        if self.args.strategy != "incremental":
            return
        if self.mode == "sharded":
            sid = self.shard_id(key)
            s = self.shard(key)
            s.setdefault("objects", {})[key] = meta
            self.dirty.add(sid)
        else:
            self.single_objects[key] = meta

    def mark_seen(self, key: str) -> None:
        self.seen.add(key)

    def save(self) -> None:
        if self.args.strategy != "incremental":
            return
        now = int(time.time())
        if self.mode == "sharded":
            self.meta["last_sync_epoch"] = now
            save_json_atomic(self.args.state_meta_file, self.meta)
            Path(self.args.state_dir).mkdir(parents=True, exist_ok=True)
            wrote = 0
            for sid in self.dirty:
                save_json_atomic(Path(self.args.state_dir) / f"{sid}.json", self.shards[sid])
                wrote += 1
            info(f"[STATE] updated {self.args.state_meta_file}; shards_written={wrote}")
        else:
            self.single["last_sync_epoch"] = now
            self.single["objects"] = self.single_objects
            save_json_atomic(self.args.state_file, self.single)
            info(f"[STATE] updated {self.args.state_file}")


# Transfer helpers

def make_temp_file(args: argparse.Namespace, suffix: str = ".tmp") -> Path:
    tmp = tempfile.NamedTemporaryFile(prefix="ns_cos_", suffix=suffix, dir=args.temp_dir, delete=False)
    p = Path(tmp.name)
    tmp.close()
    return p


def zip_target_root(args: argparse.Namespace, cos_key: str) -> str:
    key = normalize_cos_key(cos_key)
    if args.zip_extract_to_folder:
        base = key[:-4] if key.lower().endswith(".zip") else key.rstrip("/")
        return ensure_dir_prefix(base)
    if key.endswith("/"):
        return key
    parent = key.rsplit("/", 1)[0] if "/" in key else ""
    return ensure_dir_prefix(parent) if parent else ""


def safe_zip_name(name: str) -> str:
    name = name.replace("\\", "/").lstrip("/")
    parts = []
    for p in name.split("/"):
        if p in ("", ".", ".."):
            continue
        parts.append(p)
    return "/".join(parts)


def strip_redundant_zip_root(inner: str, cos_key: str) -> str:
    """Remove a duplicate top-level ZIP folder that matches the ZIP file base name.

    Example:
      cos_key = cp123456/m-page.zip
      inner   = m-page/index.html
      result  = index.html

    This prevents uploading to cp123456/m-page/m-page/index.html when the
    intended extraction root is cp123456/m-page/.
    """
    inner = safe_zip_name(inner)
    if not inner:
        return ""

    key = normalize_cos_key(cos_key).rstrip("/")
    filename = key.rsplit("/", 1)[-1]
    base = filename[:-4] if filename.lower().endswith(".zip") else filename
    base = base.strip("/")
    if not base:
        return inner

    parts = inner.split("/", 1)
    if parts[0].lower() == base.lower():
        return parts[1] if len(parts) > 1 else ""
    return inner


def unzip_to_dir(zip_file: Path, extract_dir: Path) -> None:
    """Extract a ZIP file by calling the system unzip command.

    The system unzip utility is more tolerant than Python's zipfile module for
    some non-standard ZIP files. Some ZIP files may return a non-zero unzip exit
    code even though files were successfully extracted. In that case, continue
    with the extracted files and print a warning. If no files were extracted,
    raise an error so the caller can fallback to uploading the original ZIP.
    """
    if shutil.which("unzip") is None:
        raise RuntimeError("unzip command not found. Please install it first, e.g. sudo apt install -y unzip")

    extract_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["unzip", "-oq", str(zip_file), "-d", str(extract_dir)]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    extracted_files = [p for p in extract_dir.rglob("*") if p.is_file()]
    extracted_dirs = [p for p in extract_dir.rglob("*") if p.is_dir()]

    if result.returncode != 0:
        if extracted_files:
            info(
                "[WARN] unzip returned non-zero exit code "
                f"{result.returncode}, but {len(extracted_files)} files were extracted. "
                "Continue with extracted files."
            )
            if result.stdout:
                info(f"[WARN] unzip stdout: {result.stdout[:1000]}")
            if result.stderr:
                info(f"[WARN] unzip stderr: {result.stderr[:1000]}")
            return

        raise RuntimeError(
            "unzip failed and no files were extracted: "
            f"returncode={result.returncode}; "
            f"stdout={result.stdout[:1000]}; "
            f"stderr={result.stderr[:1000]}"
        )

    if not extracted_files and not extracted_dirs:
        raise RuntimeError("unzip completed but no files or directories were extracted")


def copy_zip_ns_to_cos_extracted(args: argparse.Namespace, ns_remote: str, cos_key: str) -> bool:
    tmp = make_temp_file(args, suffix=".zip")
    extract_dir = Path(tempfile.mkdtemp(prefix="ns_cos_unzip_", dir=args.temp_dir))
    uploaded = 0
    try:
        log(args, f"Temporary ZIP file: {tmp}")
        r1 = ns_download_to_file(args, ns_remote, tmp)
        ensure_success(r1, "NetStorage download ZIP")
        root = zip_target_root(args, cos_key)
        info(f"ZIP extract by unzip: NetStorage {ns_remote} -> COS prefix {root}")

        unzip_to_dir(tmp, extract_dir)

        # Upload extracted files. Directory entries are optional marker objects.
        for local_path in sorted(extract_dir.rglob("*")):
            rel = local_path.relative_to(extract_dir).as_posix()
            rel = safe_zip_name(rel)
            if args.strip_redundant_zip_root:
                rel = strip_redundant_zip_root(rel, cos_key)
            if not rel:
                continue

            target_key = join_key(root, rel) if root else rel
            if local_path.is_dir():
                if args.copy_dir_markers:
                    cos_put_empty_object(args, ensure_dir_prefix(target_key))
                    info(f"OK: COS ZIP directory marker {ensure_dir_prefix(target_key)}")
                continue

            r2 = cos_upload_from_file(args, target_key, local_path)
            ensure_success(r2, "COS upload extracted ZIP entry")
            uploaded += 1
            info(f"OK: ZIP entry {ns_remote}!/{rel} -> COS {target_key}")
            append_success_file_log(args, f"{ns_remote}!/{rel}", target_key)

        if args.zip_upload_original:
            r3 = cos_upload_from_file(args, cos_key, tmp)
            ensure_success(r3, "COS upload original ZIP")
            info(f"OK: NetStorage original ZIP {ns_remote} -> COS {cos_key}")

        info(f"ZIP extract finished: entries_uploaded={uploaded}")
        return True
    finally:
        if args.keep_temp:
            info(f"Temporary ZIP kept: {tmp}")
            info(f"Temporary unzip directory kept: {extract_dir}")
        else:
            tmp.unlink(missing_ok=True)
            shutil.rmtree(extract_dir, ignore_errors=True)


def copy_file_ns_to_cos_raw(args: argparse.Namespace, ns_remote: str, cos_key: str) -> bool:
    """Copy one NetStorage object to COS without ZIP extraction."""
    tmp = make_temp_file(args)
    try:
        log(args, f"Temporary transfer file: {tmp}")
        r1 = ns_download_to_file(args, ns_remote, tmp)
        ensure_success(r1, "NetStorage download")
        downloaded_size = tmp.stat().st_size
        log(args, f"Downloaded bytes: {downloaded_size}")
        if downloaded_size == 0:
            info(f"[ZERO BYTE] NetStorage {ns_remote} is 0 bytes; upload COS empty object {cos_key}")
        r2 = cos_upload_from_file(args, cos_key, tmp)
        ensure_success(r2, "COS upload")
        info(f"OK: NetStorage {ns_remote} -> COS {cos_key}")
        append_success_file_log(args, ns_remote, cos_key)
        return True
    finally:
        if args.keep_temp:
            info(f"Temporary file kept: {tmp}")
        else:
            tmp.unlink(missing_ok=True)


def copy_file_ns_to_cos(args: argparse.Namespace, ns_remote: str, cos_key: str) -> bool:
    if args.extract_zip_on_ns_to_cos and ns_remote.lower().endswith(".zip"):
        try:
            return copy_zip_ns_to_cos_extracted(args, ns_remote, cos_key)
        except Exception as exc:
            info(f"[WARN] ZIP extract failed for {ns_remote}: {exc}")
            info(f"[WARN] Fallback to uploading original ZIP: {ns_remote} -> {cos_key}")
            return copy_file_ns_to_cos_raw(args, ns_remote, cos_key)
    return copy_file_ns_to_cos_raw(args, ns_remote, cos_key)


def copy_file_cos_to_ns(args: argparse.Namespace, cos_key: str, ns_remote: str) -> bool:
    # COS -> NetStorage intentionally does NOT auto-extract ZIP files.
    tmp = make_temp_file(args)
    try:
        log(args, f"Temporary transfer file: {tmp}")
        r1 = cos_download_to_file(args, cos_key, tmp)
        ensure_success(r1, "COS download")
        log(args, f"Downloaded bytes: {tmp.stat().st_size}")
        r2 = ns_upload_from_file(args, tmp, ns_remote)
        ensure_success(r2, "NetStorage upload")
        info(f"OK: COS {cos_key} -> NetStorage {ns_remote}")
        append_success_file_log(args, cos_key, ns_remote)
        return True
    finally:
        if args.keep_temp:
            info(f"Temporary file kept: {tmp}")
        else:
            tmp.unlink(missing_ok=True)


def run_parallel(tasks: List[Tuple[str, str, Dict[str, Any], str]], fn, args: argparse.Namespace, state: StateStore) -> Tuple[int, int, int]:
    copied = failed = done = 0
    bytes_done = 0
    bytes_copied = 0
    total = len(tasks)
    total_bytes = sum(int((item[2] or {}).get("size") or 0) for item in tasks)
    workers = max(1, int(args.workers))
    started = time.time()
    last_report = started
    progress_every = max(1, int(getattr(args, "copy_progress_every_files", 100) or 100))
    progress_interval = max(1.0, float(getattr(args, "progress_interval_seconds", 30.0) or 30.0))
    copy_file_attempts = max(1, int(getattr(args, "copy_file_attempts", 1) or 1))

    info(f"[COPY START] total={total} total_size={fmt_bytes(total_bytes)} workers={workers} copy_file_attempts={copy_file_attempts}")

    def report(force: bool = False) -> None:
        nonlocal last_report
        now = time.time()
        if total == 0:
            return
        if not force and done % progress_every != 0 and (now - last_report) < progress_interval:
            return
        elapsed = max(0.001, now - started)
        pct = done * 100.0 / total
        pending = max(0, total - done)
        files_per_sec = done / elapsed
        bytes_per_sec = bytes_done / elapsed
        if done > 0 and total > done:
            eta = (total - done) / max(files_per_sec, 0.000001)
        else:
            eta = 0
        info(
            "[COPY PROGRESS] "
            f"bar={progress_bar(done, total)} "
            f"done={done}/{total} "
            f"copied={copied} "
            f"failed={failed} "
            f"pending={pending} "
            f"pct={pct:.2f}% "
            f"bytes_done={fmt_bytes(bytes_done)}/{fmt_bytes(total_bytes)} "
            f"bytes_copied={fmt_bytes(bytes_copied)} "
            f"speed={fmt_bytes(bytes_per_sec)}/s "
            f"eta={fmt_eta(eta)} "
            f"elapsed_sec={elapsed:.1f} "
            f"files_per_sec={files_per_sec:.2f}"
        )
        last_report = now

    def handle_with_retries(item: Tuple[str, str, Dict[str, Any], str]) -> bool:
        src, dst, _, _ = item
        last_exc: Optional[Exception] = None
        for attempt in range(1, copy_file_attempts + 1):
            try:
                if attempt > 1:
                    info(f"[COPY RETRY] attempt={attempt}/{copy_file_attempts} {src} -> {dst}")
                return fn(args, src, dst)
            except Exception as exc:
                last_exc = exc
                if attempt < copy_file_attempts:
                    delay = backoff_delay_seconds(float(args.retry_sleep), float(args.retry_max_sleep), attempt)
                    info(f"[COPY RETRY WARN] attempt={attempt}/{copy_file_attempts} failed: {src} -> {dst}: {exc}; exponential_backoff_sleep={delay:.1f}s")
                    time.sleep(delay)
                else:
                    raise last_exc
        return False

    if workers == 1:
        for item in tasks:
            src, dst, meta, skey = item
            size = int((meta or {}).get("size") or 0)
            try:
                if handle_with_retries(item):
                    state.set(skey, meta)
                    copied += 1
                    bytes_copied += size
            except Exception as exc:
                failed += 1
                info(f"FAILED: {src} -> {dst}: {exc}")
                info(traceback.format_exc().rstrip())
                append_fail_log(args.fail_log, src, dst, exc)
                append_failed_file_log(args, src, dst, exc)
                if not args.continue_on_error:
                    raise
            finally:
                done += 1
                bytes_done += size
                report()
        report(force=True)
        return copied, 0, failed

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {ex.submit(handle_with_retries, item): item for item in tasks}
        for fut in concurrent.futures.as_completed(future_map):
            src, dst, meta, skey = future_map[fut]
            size = int((meta or {}).get("size") or 0)
            try:
                if fut.result():
                    # StateStore is intentionally updated only in the main thread.
                    state.set(skey, meta)
                    copied += 1
                    bytes_copied += size
            except Exception as exc:
                failed += 1
                info(f"FAILED: {src} -> {dst}: {exc}")
                info(traceback.format_exc().rstrip())
                append_fail_log(args.fail_log, src, dst, exc)
                append_failed_file_log(args, src, dst, exc)
                if not args.continue_on_error:
                    for f in future_map:
                        f.cancel()
                    raise
            finally:
                done += 1
                bytes_done += size
                report()
    report(force=True)
    return copied, 0, failed

# Synchronization workflows

def transfer_ns_to_cos_once(args: argparse.Namespace) -> None:
    validate_args(args)
    state = StateStore(args)
    recursive = is_recursive(args.ns_source, args.recursive)

    if not recursive:
        ns_remote = "/" + normalize_ns_path(args.ns_source)
        cos_key = normalize_cos_key(args.cos_dest)
        meta = ns_stat_metadata(args, ns_remote)
        skey = state_object_id(args, ns_remote)
        state.mark_seen(skey)
        exists = cos_exists(args, cos_key) if args.verify_target_exists else False
        should = args.strategy == "full" or state.changed(skey, meta) or (args.verify_target_exists and not exists)
        if should and (args.overwrite or not exists):
            if copy_file_ns_to_cos(args, ns_remote, cos_key):
                state.set(skey, meta)
        else:
            info(f"SKIP unchanged/exists: {ns_remote}")
        state.save()
        return

    src_prefix = ensure_dir_prefix(args.ns_source)
    dst_prefix = ensure_dir_prefix(args.cos_dest)
    info(f"Recursive copy: NetStorage /{src_prefix} -> COS {dst_prefix}")
    if getattr(args, "read_list_cache", False):
        files, dirs = load_list_cache(args, src_prefix)
    else:
        files, dirs = ns_walk(args, src_prefix)
        if getattr(args, "write_list_cache", False):
            save_list_cache(args, src_prefix, files, dirs)
    info(f"Total directories found: {len(dirs)}")
    info(f"Total files found: {len(files)}")

    if args.copy_dir_markers:
        for d in dirs:
            rel = relative_under_prefix(d, src_prefix)
            cos_dir = ensure_dir_prefix(dst_prefix if not rel else join_key(dst_prefix, rel))
            try:
                cos_put_empty_object(args, cos_dir)
                info(f"OK: COS directory marker {cos_dir}")
            except Exception as exc:
                info(f"FAILED: create COS directory marker {cos_dir}: {exc}")
                append_fail_log(args.fail_log, d, cos_dir, exc)
                if not args.continue_on_error:
                    raise

    target_files: Set[str] = set()
    if args.strategy == "incremental" and args.verify_target_exists:
        try:
            target_entries, _ = cos_list_prefix(args, dst_prefix)
            target_files = {o["key"] for o in target_entries if not (o["key"].endswith("/") and int(o.get("size", 0)) == 0)}
            info(f"Target COS files currently found under {dst_prefix}: {len(target_files)}")
        except Exception as exc:
            info(f"[WARN] COS target listing failed; fallback to copy changed only: {exc}")

    success_keys = load_success_keys(getattr(args, "success_log", "success-files.log")) if getattr(args, "skip_success_log", False) else set()
    if success_keys:
        info(f"[RESUME] loaded success log entries={len(success_keys)} from {getattr(args, 'success_log', 'success-files.log')}")

    tasks: List[Tuple[str, str, Dict[str, Any], str]] = []
    pre_skipped = 0
    success_log_skipped = 0
    select_started = time.time()
    select_last_report = select_started
    select_every = max(1, int(getattr(args, "select_progress_every_files", 5000) or 5000))
    progress_interval = max(1.0, float(getattr(args, "progress_interval_seconds", 30.0) or 30.0))

    for idx, f in enumerate(files, start=1):
        ns_rel = f["path"]
        ns_remote = "/" + ns_rel
        rel = relative_under_prefix(ns_rel, src_prefix)
        cos_key = join_key(dst_prefix, rel)
        meta = choose_ns_metadata(args, ns_remote, f)
        skey = state_object_id(args, ns_remote)
        state.mark_seen(skey)
        exists = cos_key in target_files if target_files else (cos_exists(args, cos_key) if args.verify_target_exists else False)
        # For ZIP extraction, target is a folder/prefix rather than a single file. If COS target list is available,
        # consider it existing when any object starts with the extract root.
        if args.extract_zip_on_ns_to_cos and ns_remote.lower().endswith(".zip") and target_files:
            zr = zip_target_root(args, cos_key)
            exists = any(k.startswith(zr) for k in target_files)
        already_success = file_log_key(ns_remote, cos_key) in success_keys
        should = args.strategy == "full" or state.changed(skey, meta) or (args.verify_target_exists and not exists)
        if already_success:
            pre_skipped += 1
            success_log_skipped += 1
            state.set(skey, meta)
            info(f"SKIP success-log: {ns_remote}")
        elif should and (args.overwrite or not exists):
            tasks.append((ns_remote, cos_key, meta, skey))
        else:
            pre_skipped += 1
            info(f"SKIP unchanged: {ns_remote}")

        now = time.time()
        if idx == len(files) or idx % select_every == 0 or (now - select_last_report) >= progress_interval:
            elapsed = max(0.001, now - select_started)
            pct = idx * 100.0 / max(1, len(files))
            info(
                "[SELECT PROGRESS] "
                f"bar={progress_bar(idx, len(files))} "
                f"checked={idx}/{len(files)} "
                f"selected={len(tasks)} "
                f"pre_skipped={pre_skipped} "
                f"success_log_skipped={success_log_skipped} "
                f"pct={pct:.2f}% "
                f"elapsed_sec={elapsed:.1f} "
                f"files_per_sec={idx / elapsed:.2f}"
            )
            select_last_report = now

    info(f"Files selected for copy: {len(tasks)}; pre-skipped={pre_skipped}; success-log-skipped={success_log_skipped}")
    copied, _, failed = run_parallel(tasks, copy_file_ns_to_cos, args, state)
    state.save()
    info(f"Recursive copy finished: copied={copied}, skipped={pre_skipped}, failed={failed}")


def transfer_cos_to_ns_once(args: argparse.Namespace) -> None:
    validate_args(args)
    state = StateStore(args)
    recursive = is_recursive(args.cos_source, args.recursive)

    if not recursive:
        cos_key = normalize_cos_key(args.cos_source)
        ns_remote = "/" + normalize_ns_path(args.ns_dest)
        meta = cos_head(args, cos_key) or {}
        skey = state_object_id(args, cos_key)
        state.mark_seen(skey)
        should = args.strategy == "full" or state.changed(skey, meta)
        if should:
            if copy_file_cos_to_ns(args, cos_key, ns_remote):
                state.set(skey, meta)
        else:
            info(f"SKIP unchanged: {cos_key}")
        state.save()
        return

    src_prefix = ensure_dir_prefix(args.cos_source)
    dst_prefix = ensure_dir_prefix(args.ns_dest)
    info(f"Recursive copy: COS {src_prefix} -> NetStorage /{dst_prefix}")
    objects, dirs = cos_list_prefix(args, src_prefix)
    info(f"Total COS objects found: {len(objects)}")
    if args.copy_dir_markers:
        root = "/" + normalize_ns_path(dst_prefix)
        try:
            ns_mkdir(args, root)
        except Exception as exc:
            info(f"FAILED: NetStorage mkdir {root}: {exc}")
            if not args.continue_on_error:
                raise
        for d in dirs:
            rel = relative_under_prefix(d, src_prefix)
            ns_dir = join_ns_path(dst_prefix, rel)
            try:
                ns_mkdir(args, ns_dir)
                info(f"OK: NetStorage directory {ns_dir}")
            except Exception as exc:
                info(f"FAILED: NetStorage mkdir {ns_dir}: {exc}")
                append_fail_log(args.fail_log, d, ns_dir, exc)
                if not args.continue_on_error:
                    raise

    tasks: List[Tuple[str, str, Dict[str, Any], str]] = []
    pre_skipped = 0
    for o in objects:
        key = o["key"]
        if key.endswith("/") and int(o.get("size", 0)) == 0:
            continue
        rel = relative_under_prefix(key, src_prefix)
        ns_remote = join_ns_path(dst_prefix, rel)
        skey = state_object_id(args, key)
        state.mark_seen(skey)
        should = args.strategy == "full" or state.changed(skey, o)
        if should:
            tasks.append((key, ns_remote, o, skey))
        else:
            pre_skipped += 1
            info(f"SKIP unchanged: {key}")
    info(f"Files selected for copy: {len(tasks)}; pre-skipped={pre_skipped}")
    copied, _, failed = run_parallel(tasks, copy_file_cos_to_ns, args, state)
    state.save()
    info(f"Recursive copy finished: copied={copied}, skipped={pre_skipped}, failed={failed}")


def normalize_profile_dict(profile: Dict[str, Any], idx: int = 0) -> Dict[str, Any]:
    """Normalize one NetStorage profile dictionary."""
    out = dict(profile or {})
    if "cp_code" in out and out["cp_code"] is not None:
        out["cp_code"] = str(out["cp_code"])
    out.setdefault("name", str(out.get("cp_code") or f"profile{idx + 1}"))
    return out


def build_netstorage_profiles(ns_cfg: Any) -> List[Dict[str, Any]]:
    """
    Build a list of NetStorage profiles from config.

    Supported formats:
      1) Existing single-CP format:
         "netstorage": {"host": "...", "cp_code": "123456", ...}

      2) Shared credentials with multiple CP Codes:
         "netstorage": {
           "host": "...",
           "key_name": "...",
           "key_secret": "...",
           "cp_codes": ["123456", "1990447"]
         }

      3) Full per-profile format:
         "netstorage": {
           "profiles": [
             {"name": "cp123456", "host": "...", "cp_code": "123456", "key_name": "...", "key_secret": "...", "cos_dest_prefix": "cp123456/"},
             {"name": "cp1990447", "host": "...", "cp_code": "1990447", "key_name": "...", "key_secret": "...", "cos_dest_prefix": "cp1990447/"}
           ]
         }

      4) A direct list under netstorage, equivalent to profiles.
    """
    if isinstance(ns_cfg, list):
        return [normalize_profile_dict(x, i) for i, x in enumerate(ns_cfg) if isinstance(x, dict)]

    if not isinstance(ns_cfg, dict):
        raise ValueError("netstorage must be an object or a list of objects")

    common = {k: v for k, v in ns_cfg.items() if k not in {"profiles", "cp_codes"}}

    raw_profiles = ns_cfg.get("profiles")
    if raw_profiles is not None:
        if not isinstance(raw_profiles, list):
            raise ValueError("netstorage.profiles must be a list")
        profiles: List[Dict[str, Any]] = []
        for i, item in enumerate(raw_profiles):
            if not isinstance(item, dict):
                raise ValueError("each netstorage.profiles item must be an object")
            merged = dict(common)
            merged.update(item)
            profiles.append(normalize_profile_dict(merged, i))
        return profiles

    cp_codes = ns_cfg.get("cp_codes")
    if cp_codes is not None:
        if not isinstance(cp_codes, list):
            raise ValueError("netstorage.cp_codes must be a list")
        profiles = []
        for i, item in enumerate(cp_codes):
            merged = dict(common)
            if isinstance(item, dict):
                merged.update(item)
            else:
                merged["cp_code"] = str(item)
            profiles.append(normalize_profile_dict(merged, i))
        return profiles

    return [normalize_profile_dict(common, 0)]


def args_for_netstorage_profile(args: argparse.Namespace, profile: Dict[str, Any]) -> argparse.Namespace:
    """Return an args copy with one NetStorage profile applied."""
    a = argparse.Namespace(**vars(args))
    a.ns_profile_name = str(profile.get("name") or profile.get("cp_code") or "default")
    a.ns_host = profile.get("host", a.ns_host)
    a.ns_cp_code = str(profile.get("cp_code", a.ns_cp_code))
    a.ns_key_name = profile.get("key_name", a.ns_key_name)
    a.ns_key_secret = profile.get("key_secret", a.ns_key_secret)

    # Optional per-profile COS prefix mapping.
    # If add_cp_code_to_cos_path is enabled, the CP Code is automatically added
    # as the first COS directory. This avoids overwriting identical object paths
    # across multiple CP Codes. Any profile-level cos_dest_prefix is appended
    # after the CP Code directory.
    if getattr(a, "command", "") == "ns-to-cos":
        dest = getattr(args, "cos_dest", "")
        if profile.get("cos_dest_prefix") is not None:
            dest = join_key(str(profile.get("cos_dest_prefix") or ""), dest)
        add_cp_dir = bool(getattr(args, "add_cp_code_to_cos_path", False) or profile.get("add_cp_code_to_cos_path", False))
        if add_cp_dir:
            dest = join_key(str(a.ns_cp_code), dest)
        a.cos_dest = dest
    elif getattr(a, "command", "") == "cos-to-ns" and profile.get("cos_source_prefix") is not None:
        source = join_key(str(profile.get("cos_source_prefix") or ""), getattr(args, "cos_source", ""))
        if bool(getattr(args, "add_cp_code_to_cos_path", False) or profile.get("add_cp_code_to_cos_path", False)):
            source = join_key(str(a.ns_cp_code), source)
        a.cos_source = source

    # Optional per-profile state/failure-log overrides.
    if profile.get("state_dir"):
        a.state_dir = profile["state_dir"]
    if profile.get("state_meta_file"):
        a.state_meta_file = profile["state_meta_file"]
    if profile.get("state_file"):
        a.state_file = profile["state_file"]
    if profile.get("fail_log"):
        a.fail_log = profile["fail_log"]

    return a


def run_once_for_all_profiles(args: argparse.Namespace, once_func) -> None:
    profiles = getattr(args, "ns_profiles", None) or [{
        "name": str(getattr(args, "ns_cp_code", "default")),
        "host": getattr(args, "ns_host", None),
        "cp_code": getattr(args, "ns_cp_code", None),
        "key_name": getattr(args, "ns_key_name", None),
        "key_secret": getattr(args, "ns_key_secret", None),
    }]

    if len(profiles) > 1 and getattr(args, "command", "") == "ns-to-cos":
        if not getattr(args, "add_cp_code_to_cos_path", False):
            missing_prefix = [p for p in profiles if not p.get("cos_dest_prefix")]
            if missing_prefix:
                info("[WARN] Multiple CP Codes are configured without add_cp_code_to_cos_path or cos_dest_prefix. Identical paths may overwrite each other in COS.")

    for i, profile in enumerate(profiles, 1):
        pa = args_for_netstorage_profile(args, profile)
        info(f"[PROFILE] {i}/{len(profiles)} name={getattr(pa, 'ns_profile_name', '')} cp_code={pa.ns_cp_code} host={pa.ns_host}")
        try:
            once_func(pa)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            info(f"[ERROR] profile failed name={getattr(pa, 'ns_profile_name', '')} cp_code={getattr(pa, 'ns_cp_code', '')}: {exc}")
            info(traceback.format_exc().rstrip())
            append_fail_log(getattr(pa, "fail_log", "failed_sync.log"), f"PROFILE:{getattr(pa, 'ns_profile_name', '')}", getattr(pa, "command", "UNKNOWN"), exc)
            if not getattr(pa, "continue_on_error", True):
                raise


def scheduled_loop(args: argparse.Namespace, once_func) -> None:
    if not args.schedule:
        run_once_for_all_profiles(args, once_func)
        return
    info(f"[SCHEDULE] enabled interval_seconds={args.schedule_interval}")
    while True:
        try:
            run_once_for_all_profiles(args, once_func)
            info(f"[SCHEDULE] sleep {args.schedule_interval}s ...")
            time.sleep(int(args.schedule_interval))
        except KeyboardInterrupt:
            info("[INFO] Interrupted by user. Exiting gracefully.")
            return
        except Exception as exc:
            info(f"[ERROR] scheduled run failed: {exc}")
            info(traceback.format_exc().rstrip())
            append_fail_log(getattr(args, "fail_log", "failed_sync.log"), "SCHEDULED_RUN", getattr(args, "command", "UNKNOWN"), exc)


def transfer_ns_to_cos(args: argparse.Namespace) -> None:
    scheduled_loop(args, transfer_ns_to_cos_once)


def transfer_cos_to_ns(args: argparse.Namespace) -> None:
    scheduled_loop(args, transfer_cos_to_ns_once)


# Failed-file retry workflow

def parse_failed_file_line(line: str) -> Optional[Tuple[str, str]]:
    """Parse one compact failed-files.log line.

    Expected format:
      YYYY-mm-dd HH:MM:SS | FAILED | <src> -> <dst> | <detail>

    Returns (src, dst) or None for non-matching lines.
    """
    line = (line or "").strip()
    if " | FAILED | " not in line or " -> " not in line:
        return None
    try:
        payload = line.split(" | FAILED | ", 1)[1]
        pair = payload.split(" | ", 1)[0]
        src, dst = pair.split(" -> ", 1)
        src = src.strip()
        dst = dst.strip()
        if not src or not dst:
            return None
        return src, dst
    except Exception:
        return None


class NoopState:
    def set(self, key: str, meta: Dict[str, Any]) -> None:
        return

    def save(self) -> None:
        return


def retry_failed_once(args: argparse.Namespace) -> None:
    """Retry files listed in failed-files.log without re-listing NetStorage."""
    validate_args(args)
    failed_log_path = Path(args.failed_log_path)
    if not failed_log_path.exists():
        raise FileNotFoundError(f"failed log not found: {failed_log_path}")

    seen: Set[str] = set()
    tasks: List[Tuple[str, str, Dict[str, Any], str]] = []
    parsed = ignored = duplicates = success_skipped = 0

    success_keys = load_success_keys(getattr(args, "success_log", "success-files.log")) if getattr(args, "skip_success_log", False) else set()
    if getattr(args, "skip_success_log", False):
        info(f"[RESUME] loaded success log entries={len(success_keys)} from {getattr(args, 'success_log', 'success-files.log')}")

    with open(failed_log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            item = parse_failed_file_line(line)
            if not item:
                ignored += 1
                continue
            parsed += 1
            src, dst = item
            key = file_log_key(src, dst)
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            if key in success_keys:
                success_skipped += 1
                info(f"SKIP success-log: {src}")
                continue
            # Size is unknown without stat/list. Keep 0 so retry does not add extra metadata calls.
            tasks.append((src, dst, {"size": 0}, key))

    info(
        "[RETRY FAILED START] "
        f"log={failed_log_path} parsed={parsed} ignored={ignored} duplicates={duplicates} "
        f"success_log_skipped={success_skipped} selected={len(tasks)}"
    )

    copied, _, failed = run_parallel(tasks, copy_file_ns_to_cos, args, NoopState())
    info(f"[RETRY FAILED DONE] copied={copied} failed={failed} selected={len(tasks)}")


def retry_failed(args: argparse.Namespace) -> None:
    """Retry failed NS -> COS file copies for all configured profiles.

    Note: failed-files.log records concrete source and destination paths, so no
    source tree listing is performed and no additional stat requests are made.
    """
    run_once_for_all_profiles(args, retry_failed_once)


# Command-line interface and configuration

def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ns-host")
    parser.add_argument("--ns-cp-code")
    parser.add_argument("--ns-key-name")
    parser.add_argument("--ns-key-secret")
    parser.add_argument("--cos-secret-id")
    parser.add_argument("--cos-secret-key")
    parser.add_argument("--cos-region")
    parser.add_argument("--cos-bucket")
    parser.add_argument("--cos-appid")
    parser.add_argument("--cos-expire", type=int)
    parser.add_argument("--cos-max-keys", type=int)
    parser.add_argument("--strategy", choices=["full", "incremental"])
    parser.add_argument("--detect", choices=["etag", "last_modified"])
    parser.add_argument("--state-mode", choices=["single", "sharded"])
    parser.add_argument("--state-dir")
    parser.add_argument("--state-meta-file")
    parser.add_argument("--state-file")
    parser.add_argument("--overwrite", dest="overwrite", action="store_true", default=None)
    parser.add_argument("--no-overwrite", dest="overwrite", action="store_false", default=None)
    parser.add_argument("--verify-target-exists", dest="verify_target_exists", action="store_true", default=None)
    parser.add_argument("--no-verify-target-exists", dest="verify_target_exists", action="store_false", default=None)
    parser.add_argument("--copy-dir-markers", dest="copy_dir_markers", action="store_true", default=None)
    parser.add_argument("--no-copy-dir-markers", dest="copy_dir_markers", action="store_false", default=None)
    parser.add_argument("--extract-zip-on-ns-to-cos", dest="extract_zip_on_ns_to_cos", action="store_true", default=None)
    parser.add_argument("--no-extract-zip-on-ns-to-cos", dest="extract_zip_on_ns_to_cos", action="store_false", default=None)
    parser.add_argument("--zip-extract-to-folder", dest="zip_extract_to_folder", action="store_true", default=None)
    parser.add_argument("--zip-extract-to-dest-prefix", dest="zip_extract_to_folder", action="store_false", default=None)
    parser.add_argument("--zip-upload-original", dest="zip_upload_original", action="store_true", default=None)
    parser.add_argument("--no-zip-upload-original", dest="zip_upload_original", action="store_false", default=None)
    parser.add_argument("--add-cp-code-to-cos-path", dest="add_cp_code_to_cos_path", action="store_true", default=None)
    parser.add_argument("--no-add-cp-code-to-cos-path", dest="add_cp_code_to_cos_path", action="store_false", default=None)
    parser.add_argument("--strip-redundant-zip-root", dest="strip_redundant_zip_root", action="store_true", default=None)
    parser.add_argument("--no-strip-redundant-zip-root", dest="strip_redundant_zip_root", action="store_false", default=None)
    parser.add_argument("--prune-state", dest="prune_state", action="store_true", default=None)
    parser.add_argument("--no-prune-state", dest="prune_state", action="store_false", default=None)
    parser.add_argument("--fail-log")
    parser.add_argument("--success-log")
    parser.add_argument("--failed-files-log")
    parser.add_argument("--failed-dirs-log")
    parser.add_argument("--skip-success-log", dest="skip_success_log", action="store_true", default=None)
    parser.add_argument("--no-skip-success-log", dest="skip_success_log", action="store_false", default=None)
    parser.add_argument("--copy-file-attempts", type=int)
    parser.add_argument("--read-list-cache", dest="read_list_cache", action="store_true", default=None)
    parser.add_argument("--no-read-list-cache", dest="read_list_cache", action="store_false", default=None)
    parser.add_argument("--write-list-cache", dest="write_list_cache", action="store_true", default=None)
    parser.add_argument("--no-write-list-cache", dest="write_list_cache", action="store_false", default=None)
    parser.add_argument("--list-cache-file")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--retries", type=int)
    parser.add_argument("--retry-sleep", type=float)
    parser.add_argument("--retry-max-sleep", type=float)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--temp-dir")
    parser.add_argument("--keep-temp", action="store_true", default=None)
    parser.add_argument("--skip-tls-verify", action="store_true", default=None)
    parser.add_argument("--curl-log")
    parser.add_argument("--quiet", action="store_true", default=None)
    parser.add_argument("--continue-on-error", action="store_true", default=None)
    parser.add_argument("--content-type")
    parser.add_argument("--ns-list-retries", type=int)
    parser.add_argument("--ns-list-retry-sleep", type=float)
    parser.add_argument("--ns-list-max-retry-sleep", type=float)
    parser.add_argument("--skip-failed-dirs", action="store_true", default=None)
    parser.add_argument("--prefer-list-metadata", dest="prefer_list_metadata", action="store_true", default=None)
    parser.add_argument("--no-prefer-list-metadata", dest="prefer_list_metadata", action="store_false", default=None)
    parser.add_argument("--stat-if-list-metadata-incomplete", dest="stat_if_list_metadata_incomplete", action="store_true", default=None)
    parser.add_argument("--no-stat-if-list-metadata-incomplete", dest="stat_if_list_metadata_incomplete", action="store_false", default=None)
    parser.add_argument("--list-progress-every-dirs", type=int)
    parser.add_argument("--select-progress-every-files", type=int)
    parser.add_argument("--copy-progress-every-files", type=int)
    parser.add_argument("--progress-interval-seconds", type=float)
    parser.add_argument("--recursive", action="store_true", default=False)
    parser.add_argument("--schedule", dest="schedule", action="store_true", default=None)
    parser.add_argument("--no-schedule", dest="schedule", action="store_false", default=None)
    parser.add_argument("--schedule-interval", type=int)


def apply_config(args: argparse.Namespace, cfg: Dict[str, Any]) -> argparse.Namespace:
    ns_cfg, cos = cfg["netstorage"], cfg["cos"]
    profiles = build_netstorage_profiles(ns_cfg)
    if not profiles:
        raise ValueError("At least one NetStorage profile or CP Code is required")
    first_ns = profiles[0]

    sync, state = cfg["sync"], cfg["state"]
    sched, retry, trans = cfg["schedule"], cfg["retry"], cfg["transfer"]
    mapping = {
        "ns_host": first_ns.get("host"), "ns_cp_code": first_ns.get("cp_code"), "ns_key_name": first_ns.get("key_name"), "ns_key_secret": first_ns.get("key_secret"),
        "cos_secret_id": cos.get("secret_id"), "cos_secret_key": cos.get("secret_key"), "cos_region": cos.get("region"), "cos_bucket": cos.get("bucket"), "cos_appid": cos.get("appid"), "cos_expire": cos.get("expire"), "cos_max_keys": cos.get("max_keys"),
        "strategy": sync.get("strategy"), "detect": sync.get("detect"), "fail_log": sync.get("fail_log"), "success_log": sync.get("success_log"), "failed_files_log": sync.get("failed_files_log"), "failed_dirs_log": sync.get("failed_dirs_log"), "copy_file_attempts": sync.get("copy_file_attempts"), "list_cache_file": sync.get("list_cache_file"),
        "state_mode": state.get("state_mode"), "state_dir": state.get("state_dir"), "state_meta_file": state.get("state_meta_file"), "state_file": state.get("state_file"),
        "timeout": retry.get("timeout_seconds"), "retries": retry.get("max_attempts"), "retry_sleep": retry.get("base_sleep_seconds"), "retry_max_sleep": retry.get("max_sleep_seconds"),
        "workers": trans.get("workers"), "temp_dir": trans.get("temp_dir"), "curl_log": trans.get("curl_log"), "content_type": trans.get("content_type"),
        "ns_list_retries": trans.get("ns_list_retries"), "ns_list_retry_sleep": trans.get("ns_list_retry_sleep"), "ns_list_max_retry_sleep": trans.get("ns_list_max_retry_sleep"), "list_progress_every_dirs": trans.get("list_progress_every_dirs"), "select_progress_every_files": trans.get("select_progress_every_files"), "copy_progress_every_files": trans.get("copy_progress_every_files"), "progress_interval_seconds": trans.get("progress_interval_seconds"),
        "schedule_interval": sched.get("interval_seconds"),
    }
    for k, v in mapping.items():
        if getattr(args, k, None) is None:
            setattr(args, k, v)
    args.overwrite = bool_value(args.overwrite, bool(sync.get("overwrite", True)))
    args.verify_target_exists = bool_value(args.verify_target_exists, bool(sync.get("verify_target_exists", True)))
    args.copy_dir_markers = bool_value(args.copy_dir_markers, bool(sync.get("copy_dir_markers", True)))
    args.extract_zip_on_ns_to_cos = bool_value(args.extract_zip_on_ns_to_cos, bool(sync.get("extract_zip_on_ns_to_cos", False)))
    args.zip_extract_to_folder = bool_value(args.zip_extract_to_folder, bool(sync.get("zip_extract_to_folder", True)))
    args.zip_upload_original = bool_value(args.zip_upload_original, bool(sync.get("zip_upload_original", False)))
    args.add_cp_code_to_cos_path = bool_value(getattr(args, "add_cp_code_to_cos_path", None), bool(sync.get("add_cp_code_to_cos_path", False)))
    args.strip_redundant_zip_root = bool_value(getattr(args, "strip_redundant_zip_root", None), bool(sync.get("strip_redundant_zip_root", True)))
    args.prune_state = bool_value(args.prune_state, bool(sync.get("prune_state", True)))
    args.schedule = bool_value(args.schedule, bool(sched.get("enabled", False)))
    args.keep_temp = bool_value(args.keep_temp, bool(trans.get("keep_temp", False)))
    args.skip_tls_verify = bool_value(args.skip_tls_verify, bool(trans.get("skip_tls_verify", False)))
    args.quiet = bool_value(args.quiet, bool(trans.get("quiet", False)))
    args.continue_on_error = bool_value(args.continue_on_error, bool(trans.get("continue_on_error", True)))
    args.skip_failed_dirs = bool_value(args.skip_failed_dirs, bool(trans.get("skip_failed_dirs", False)))
    args.prefer_list_metadata = bool_value(getattr(args, "prefer_list_metadata", None), bool(sync.get("prefer_list_metadata", True)))
    args.stat_if_list_metadata_incomplete = bool_value(getattr(args, "stat_if_list_metadata_incomplete", None), bool(sync.get("stat_if_list_metadata_incomplete", False)))
    args.skip_success_log = bool_value(getattr(args, "skip_success_log", None), bool(sync.get("skip_success_log", False)))
    args.read_list_cache = bool_value(getattr(args, "read_list_cache", None), bool(sync.get("read_list_cache", False)))
    args.write_list_cache = bool_value(getattr(args, "write_list_cache", None), bool(sync.get("write_list_cache", True)))
    args.ns_profiles = profiles
    return args

def validate_args(args: argparse.Namespace) -> None:
    args.ns_host = require(args.ns_host, "netstorage.host")
    args.ns_cp_code = require(args.ns_cp_code, "netstorage.cp_code")
    args.ns_key_name = require(args.ns_key_name, "netstorage.key_name")
    args.ns_key_secret = require(args.ns_key_secret, "netstorage.key_secret")
    args.cos_secret_id = require(args.cos_secret_id, "cos.secret_id")
    args.cos_secret_key = require(args.cos_secret_key, "cos.secret_key")
    if args.strategy not in {"full", "incremental"}:
        raise ValueError("--strategy must be full or incremental")
    if args.detect not in {"etag", "last_modified"}:
        raise ValueError("--detect must be etag or last_modified")
    if args.state_mode not in {"single", "sharded"}:
        raise ValueError("--state-mode must be single or sharded")
    if int(args.workers) < 1:
        raise ValueError("--workers must be >= 1")
    if int(args.retries) < 1:
        raise ValueError("--retries must be >= 1")
    if int(getattr(args, "copy_file_attempts", 1) or 1) < 1:
        raise ValueError("--copy-file-attempts must be >= 1")
    if float(args.timeout) <= 0:
        raise ValueError("--timeout must be > 0")
    if int(args.cos_max_keys) < 1 or int(args.cos_max_keys) > 1000:
        raise ValueError("--cos-max-keys must be between 1 and 1000")
    if args.schedule and int(args.schedule_interval) < 1:
        raise ValueError("--schedule-interval must be >= 1")
    if args.temp_dir:
        Path(args.temp_dir).mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync between Akamai NetStorage and Tencent Cloud COS")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Config JSON path. Default: ./ns-cos-config.json next to the script")
    sub = parser.add_subparsers(dest="command", required=True)
    p1 = sub.add_parser("ns-to-cos", help="Copy/sync NetStorage to COS")
    p1.add_argument("ns_source")
    p1.add_argument("cos_dest")
    add_common(p1)
    p1.set_defaults(func=transfer_ns_to_cos)
    p2 = sub.add_parser("cos-to-ns", help="Copy/sync COS to NetStorage")
    p2.add_argument("cos_source")
    p2.add_argument("ns_dest")
    add_common(p2)
    p2.set_defaults(func=transfer_cos_to_ns)
    p3 = sub.add_parser("retry-failed", help="Retry failed NS -> COS files from failed-files.log without re-listing source directories")
    p3.add_argument("failed_log_path", nargs="?", default="failed-files.log")
    add_common(p3)
    p3.set_defaults(func=retry_failed)
    args = parser.parse_args()
    try:
        cfg = load_config(args.config)
        args = apply_config(args, cfg)
        init_file_logs(args)
        args.func(args)
    except KeyboardInterrupt:
        info("[INFO] Interrupted by user. Exiting gracefully.")
        sys.exit(130)
    except Exception as exc:
        info(f"[FATAL] {exc}")
        info(traceback.format_exc().rstrip())
        try:
            fail_log = getattr(args, "fail_log", None) or "failed_sync.log"
            append_fail_log(fail_log, "MAIN", getattr(args, "command", "UNKNOWN"), exc)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
