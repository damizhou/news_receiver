#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bbc_rss_pull.py
固定抓取 BBC 中文（简体）RSS（只请求一次），并将 title/summary 统一转换为简体中文（OpenCC t2s）。

覆盖“所有情况”的行为：
- 首次运行：无状态/无输出 → 覆盖写入当下 RSS 快照的全量，并建立状态（ETag/Last-Modified/seen_ids）
- 之后增量：HTTP 200 → 仅写新增；HTTP 304 → 不写；若输出被删/变空 → 自动再做一次快照覆盖补回
- 旧状态迁移：把 seen_ids 里的 /trad、#片段、?参数 统一规范化为 /simp 且去掉片段/参数，防同文异链

依赖：
  pip install requests feedparser
  # 启用繁转简（推荐）：
  pip install opencc-python-reimplemented   # 或 pip install opencc
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
import feedparser

# ===== 固定配置（按需改这几项） =====
FEED_URL   = "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0")
STATE_PATH = Path("./rss_state.json")
OUTPUT_PATH: Optional[Path] = Path("./bbc.ndjson")  # None 表示写 stdout
TIMEOUT    = 15
MAX_SEEN   = 2000
LOG_LEVEL  = logging.INFO

# 是否将标题与摘要统一转换为简体中文
CONVERT_T2S = True

# ===== OpenCC（可选） =====
_cc = None
_cc_err = None
if CONVERT_T2S:
    try:
        from opencc import OpenCC  # type: ignore
        _cc = OpenCC("t2s")
    except Exception as e:
        _cc = None
        _cc_err = str(e)

# ===== 数据结构 =====
@dataclass
class FeedState:
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    seen_ids: List[str] = field(default_factory=list)

@dataclass
class AppState:
    feeds: Dict[str, FeedState] = field(default_factory=dict)

    @staticmethod
    def load(path: Path) -> "AppState":
        if not path.exists():
            return AppState()
        try:
            raw = json.loads(path.read_text("utf-8"))
            feeds: Dict[str, FeedState] = {}
            for url, st in raw.get("feeds", {}).items():
                feeds[url] = FeedState(
                    etag=st.get("etag"),
                    last_modified=st.get("last_modified"),
                    seen_ids=list(st.get("seen_ids", [])),
                )
            return AppState(feeds=feeds)
        except Exception as e:
            logging.warning("状态文件读取失败（将从空状态开始）：%s", e)
            return AppState()

    def save(self, path: Path) -> None:
        out = {"feeds": {
            url: {"etag": st.etag, "last_modified": st.last_modified, "seen_ids": st.seen_ids}
            for url, st in self.feeds.items()
        }}
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2), "utf-8")

# ===== 工具函数 =====
def iso_utc(ts_struct) -> Optional[str]:
    if not ts_struct:
        return None
    try:
        dt = datetime(*ts_struct[:6], tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def canonicalize_bbc_cn(url: Optional[str]) -> Optional[str]:
    """域名小写、去 query/fragment、/trad→/simp（兼容两种路径风格）"""
    if not url:
        return url
    p = urlparse(url)
    netloc = p.netloc.lower()
    path = p.path
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "zhongwen":
        # 旧风格：/zhongwen/trad/... 或 /zhongwen/simp/...
        if parts[1] in ("trad", "simp"):
            parts[1] = "simp"
            path = "/" + "/".join(parts)
        # 新风格：/zhongwen/articles/<slug>/<trad|simp>
        elif len(parts) >= 4 and parts[1] == "articles" and parts[-1] in ("trad", "simp"):
            parts[-1] = "simp"
            path = "/" + "/".join(parts)
    return urlunparse((p.scheme, netloc, path, "", "", ""))

def migrate_seen_ids(ids: List[str]) -> List[str]:
    """对历史 seen_ids 做一次规范化并去重（保序）"""
    out, seen = [], set()
    for k in ids:
        if not isinstance(k, str):
            continue
        ck = canonicalize_bbc_cn(k)
        if ck and ck not in seen:
            out.append(ck)
            seen.add(ck)
    return out[:MAX_SEEN]

def entry_id(e: Any) -> str:
    """唯一键：id/guid/link → 规范化；都缺时用 title::published 兜底"""
    def _get(k: str):
        return getattr(e, k, None) if not isinstance(e, dict) else (e.get(k) or getattr(e, k, None))
    for k in ("id", "guid", "link"):
        v = _get(k)
        if v:
            return canonicalize_bbc_cn(str(v)) or str(v)
    title = _get("title") or ""
    pub = _get("published") or ""
    return f"{title}::{pub}"

def fetch_feed(session: requests.Session, url: str, st: FeedState) -> Tuple[Optional[bytes], Optional[str], Optional[str], int]:
    """返回：(content_bytes, new_etag, new_last_modified, status_code)"""
    headers = {"User-Agent": USER_AGENT}
    if st.etag:
        headers["If-None-Match"] = st.etag
    if st.last_modified:
        headers["If-Modified-Since"] = st.last_modified
    try:
        r = session.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.content, r.headers.get("ETag") or st.etag, r.headers.get("Last-Modified") or st.last_modified, 200
        if r.status_code == 304:
            return None, st.etag, st.last_modified, 304
        logging.error("HTTP %s: %s", r.status_code, url)
        return None, st.etag, st.last_modified, r.status_code
    except requests.RequestException as e:
        logging.error("请求失败 %s: %s", url, e)
        return None, st.etag, st.last_modified, -1

def refetch_without_condition(session: requests.Session, url: str, st: FeedState) -> Tuple[Optional[bytes], Optional[str], Optional[str], int]:
    """用于“需要快照写入但命中304/无内容”的兜底：去掉条件头再抓一次"""
    try:
        r = session.get(url, headers={"User-Agent": USER_AGENT, "Cache-Control": "no-cache", "Pragma": "no-cache"}, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.content, r.headers.get("ETag") or st.etag, r.headers.get("Last-Modified") or st.last_modified, 200
        logging.error("兜底抓取 HTTP %s: %s", r.status_code, url)
        return None, st.etag, st.last_modified, r.status_code
    except requests.RequestException as e:
        logging.error("兜底抓取失败 %s: %s", url, e)
        return None, st.etag, st.last_modified, -1

def _t2s(text: Optional[str]) -> Optional[str]:
    """将文本转为简体；OpenCC 不可用时原样返回"""
    if not text or not CONVERT_T2S:
        return text
    if _cc is None:
        # 首次告警（避免刷屏）
        global _cc_err
        if _cc_err:
            logging.warning("未安装 OpenCC，跳过繁转简（pip install opencc-python-reimplemented）。原因：%s", _cc_err)
            _cc_err = None
        return text
    try:
        return _cc.convert(text)
    except Exception as e:
        logging.warning("OpenCC 转换失败，保留原文：%s", e)
        return text

def parse_entries(content: bytes, source_feed: str) -> List[dict]:
    fp = feedparser.parse(content)
    items: List[dict] = []
    for e in fp.entries:
        eid = entry_id(e)
        published_iso = iso_utc(getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None))
        updated_iso = iso_utc(getattr(e, "updated_parsed", None))

        orig_link = getattr(e, "link", None)
        canon_link = canonicalize_bbc_cn(orig_link) if orig_link else None

        # —— 核心：将标题与摘要转换为简体 —— #
        title   = _t2s(getattr(e, "title", None))
        summary = _t2s(getattr(e, "summary", None))

        items.append({
            "id": eid,
            "title": title,
            "summary": summary,
            "link": canon_link or orig_link,  # 展示用：统一到 /simp，去掉参数/片段
            "_orig_link": orig_link,          # 备查
            "published": published_iso,
            "updated": updated_iso,
            "authors": [a.get("name") for a in getattr(e, "authors", [])] if getattr(e, "authors", None) else None,
            "tags": [t.get("term") for t in getattr(e, "tags", [])] if getattr(e, "tags", None) else None,
            "_feed": source_feed,
        })

    # 稳定输出：按发布时间升序（旧→新）；缺时间的放后
    items.sort(key=lambda it: (0, it["published"]) if it.get("published") else (1, it.get("id","")))
    return items

def write_ndjson(path: Optional[Path], items: Iterable[dict], *, mode: str = "a") -> int:
    """mode='w' 覆盖写快照；mode='a' 追加写增量；path=None→stdout"""
    n = 0
    if path:
        if mode == "w":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", "utf-8")  # 清空
        with path.open("a", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
                n += 1
    else:
        for it in items:
            print(json.dumps(it, ensure_ascii=False))
            n += 1
    return n

# ===== 单次主流程 =====
def run_once() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=LOG_LEVEL)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # 1) 读状态 & 迁移 seen_ids
    app = AppState.load(STATE_PATH)
    st = app.feeds.get(FEED_URL, FeedState())
    old_len = len(st.seen_ids)
    st.seen_ids = migrate_seen_ids(st.seen_ids)
    if len(st.seen_ids) != old_len:
        logging.info("迁移去重 seen_ids: %d -> %d", old_len, len(st.seen_ids))

    # 2) 判断是否需要快照覆盖：首跑或输出文件缺失/为空
    output_missing_or_empty = (OUTPUT_PATH is not None) and (not OUTPUT_PATH.exists() or OUTPUT_PATH.stat().st_size == 0)
    first_bootstrap = (len(st.seen_ids) == 0)
    need_snapshot_write = first_bootstrap or output_missing_or_empty

    # 3) 抓取（先条件请求；若304且需要快照，则无条件兜底）
    content, new_etag, new_lm, code = fetch_feed(session, FEED_URL, st)
    if code == 304 and need_snapshot_write:
        logging.info("命中304但需要快照写入 → 执行无条件兜底抓取")
        content, new_etag, new_lm, code = refetch_without_condition(session, FEED_URL, st)
    if code != 200 and content is None:
        logging.warning("本轮无内容写入（HTTP %s）", code)
        # 仍保存状态文件以保持存在
        app.feeds[FEED_URL] = st
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        app.save(STATE_PATH)
        return

    # 4) 解析条目（含繁转简）
    entries = parse_entries(content, FEED_URL)
    if not entries:
        logging.info("解析到 0 条条目")
        app.feeds[FEED_URL] = st
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        app.save(STATE_PATH)
        return

    # 5) 写入策略
    if need_snapshot_write:
        wrote = write_ndjson(OUTPUT_PATH, entries, mode="w")
        # seen_ids 合并：把当前快照的键放前面，然后接旧值，再规范化截断
        snap_ids = [it["id"] for it in entries if it.get("id")]
        st.seen_ids = migrate_seen_ids(snap_ids + st.seen_ids)
        logging.info("快照覆盖写入 %d 条；seen_ids=%d", wrote, len(st.seen_ids))
    else:
        seen = set(st.seen_ids)
        new_items: List[dict] = []
        for it in entries:
            key = it["id"]
            if key and key not in seen:
                new_items.append(it)
                seen.add(key)
                st.seen_ids.insert(0, key)
        st.seen_ids = st.seen_ids[:MAX_SEEN]
        if new_items:
            wrote = write_ndjson(OUTPUT_PATH, new_items, mode="a")
            logging.info("新增写入 %d 条；seen_ids=%d", wrote, len(st.seen_ids))
        else:
            logging.info("去重后无新增条目；不写入输出")

    # 6) 保存状态
    st.etag = new_etag
    st.last_modified = new_lm
    app.feeds[FEED_URL] = st
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    app.save(STATE_PATH)

if __name__ == "__main__":
    run_once()
