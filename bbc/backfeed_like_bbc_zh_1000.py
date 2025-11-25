#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfeed_like_bbc_zh_1000.py
按 Backfeed 思路，通过 archive.org Wayback 快照合并 BBC 中文（简体优先）“最新N条”。
新增：将 title/summary 统一转换为简体（OpenCC t2s，默认开启，可 --no-t2s 关闭）

依赖：
  pip install requests feedparser
  # 启用繁转简（推荐）：
  pip install opencc-python-reimplemented  # 或 pip install opencc
"""
from __future__ import annotations
import argparse, json, logging, time, sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Iterable
from urllib.parse import urlparse, urlunparse

import requests, feedparser
from datetime import datetime, timezone

# ---- OpenCC（可选） ----
_CC = None
_CC_ERR = None
def _try_init_opencc():
    global _CC, _CC_ERR
    if _CC is not None or _CC_ERR is not None:
        return
    try:
        from opencc import OpenCC  # type: ignore
        _CC = OpenCC("t2s")
    except Exception as e:
        _CC = None
        _CC_ERR = str(e)

def _t2s(text: Optional[str], enabled: bool) -> Optional[str]:
    """将文本转为简体；OpenCC 不可用/未启用则原样返回"""
    if not text or not enabled:
        return text
    _try_init_opencc()
    if _CC is None:
        # 只警告一次，避免日志刷屏
        global _CC_ERR
        if _CC_ERR:
            logging.warning("未安装 OpenCC，跳过繁转简（pip install opencc-python-reimplemented）。原因：%s", _CC_ERR)
            _CC_ERR = None
        return text
    try:
        return _CC.convert(text)
    except Exception as e:
        logging.warning("OpenCC 转换失败，保留原文：%s", e)
        return text

UA = "bbc-backfeed-like/1.1 (+https://example.com)"
TIMEOUT = 20
CDX = "https://web.archive.org/cdx/search/cdx"

# 默认同时查 simp + trad 的 RSS 快照，更稳
FEED_URLS_DEFAULT = [
    "https://feeds.bbci.co.uk/zhongwen/simp/index.xml",               # 主页
#     "https://www.bbc.co.uk/zhongwen/simp/world/index.xml",         # 国际新闻
#     "https://www.bbc.co.uk/zhongwen/simp/china/index.xml",         # 两岸三地 / 中国
#     "https://www.bbc.co.uk/zhongwen/simp/uk/index.xml",            # 英国动态
#     "https://www.bbc.co.uk/zhongwen/simp/business/index.xml",      # 金融财经
#     "https://www.bbc.co.uk/zhongwen/simp/interactive/index.xml",   # 网上互动
#     "https://www.bbc.co.uk/zhongwen/simp/multimedia/index.xml",    # 音视图片
#     "https://www.bbc.co.uk/zhongwen/simp/indepth/index.xml",       # 分析评论
#     "https://www.bbc.co.uk/zhongwen/simp/chinese_analysis/index.xml",   # 中国评论
#     "https://www.bbc.co.uk/zhongwen/simp/world_commentary/index.xml",   # 国际分析
#     "https://www.bbc.co.uk/zhongwen/simp/focus_on_china/index.xml",     # 点评中国
#     "https://www.bbc.co.uk/zhongwen/simp/comments_on_china/index.xml",  # 大家谈中国
#     "https://www.bbc.co.uk/zhongwen/simp/hong_kong_review/index.xml",   # 香港观察
#     "https://www.bbc.co.uk/zhongwen/simp/taiwan_letters/index.xml",     # 台湾来鸿
#     "https://www.bbc.co.uk/zhongwen/simp/fooc/index.xml",               # 记者来鸿
]

def cdx_list_snapshots(session: requests.Session, feed_url: str, limit: int | None = None) -> List[str]:
    """
    以“最新→最旧”的顺序列出某个 feed 的所有时间戳（YYYYMMDDhhmmss）。
    说明：sort=reverse 倒序仅对 exact URL 查询可用且高效。
    """
    params = {
        "url": feed_url,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "gzip": "false",
        "sort": "reverse",  # 倒序（新→旧）
    }
    if limit:
        params["limit"] = str(max(limit, 1))

    r = session.get(CDX, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    rows = data[1:] if data else []  # 第 1 行是表头
    return [row[0] for row in rows]

def wb_raw_url(ts: str, original: str) -> str:
    """Wayback 原始响应（不注入 replay HTML）"""
    return f"https://web.archive.org/web/{ts}id_/{original}"

def iso_utc(ts_struct) -> Optional[str]:
    if not ts_struct:
        return None
    dt = datetime(*ts_struct[:6], tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def canonicalize_bbc_cn(url: Optional[str]) -> Optional[str]:
    """
    规范化 BBC 中文链接：
    - 域名小写
    - 去 query / fragment
    - /zhongwen 下若含语言段，将 trad 统一为 simp
      兼容两种历史路径：
        /zhongwen/articles/<slug>/<trad|simp>
        /zhongwen/<trad|simp>/china/...   （旧站风格）
    """
    if not url:
        return url
    p = urlparse(url)
    netloc = p.netloc.lower()
    path = p.path
    parts = path.strip("/").split("/")

    if len(parts) >= 2 and parts[0] == "zhongwen":
        # 旧风格：/zhongwen/trad/...
        if parts[1] in ("trad", "simp"):
            parts[1] = "simp"
            path = "/" + "/".join(parts)
        # 新风格：/zhongwen/articles/<slug>/<trad|simp>
        elif len(parts) >= 4 and parts[1] == "articles" and parts[-1] in ("trad", "simp"):
            parts[-1] = "simp"
            path = "/" + "/".join(parts)

    return urlunparse((p.scheme, netloc, path, "", "", ""))

def parse_feed(content: bytes, source_feed: str, snapshot_ts: str, t2s_enabled: bool) -> List[Dict]:
    fp = feedparser.parse(content)
    out: List[Dict] = []
    for e in fp.entries:
        orig_link = getattr(e, "link", None)
        canon = canonicalize_bbc_cn(orig_link)
        eid = canonicalize_bbc_cn(getattr(e, "id", None) or getattr(e, "guid", None) or orig_link) or (
            (getattr(e,"title",None) or "") + "::" + (getattr(e,"published",None) or "")
        )
        pub = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        upd = getattr(e, "updated_parsed", None)

        title_raw = getattr(e, "title", None)
        summary_raw = getattr(e, "summary", None)
        title   = _t2s(title_raw,   enabled=t2s_enabled)
        summary = _t2s(summary_raw, enabled=t2s_enabled)

        item = {
            "id": eid,
            "title": title,
            "link": canon or orig_link,
            "_orig_link": orig_link,
            "published": iso_utc(pub),
            "updated": iso_utc(upd),
            "summary": summary,
            "_feed": source_feed,
            "_snapshot_ts": snapshot_ts,  # 该条目首次出现的 RSS 快照时间
        }
        out.append(item)
    return out

def write_ndjson(path: Path, items: Iterable[Dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser(description="通过 Wayback 快照合并 BBC 中文（简体优先）最新 N 条（Backfeed 风格，支持繁→简）")
    ap.add_argument("--limit", type=int, default=1000, help="需要的条目数（默认 1000）")
    ap.add_argument("--feeds", nargs="*", default=FEED_URLS_DEFAULT, help="要合并的 RSS 源（默认 simp 与 trad 两个）")
    ap.add_argument("--sleep", type=float, default=0.35, help="抓取快照间隔秒（礼貌限速）")
    ap.add_argument("--csv", action="store_true", help="以 CSV 输出（默认 NDJSON）")
    ap.add_argument("--no-t2s", action="store_true", help="禁用繁体→简体转换（默认开启）")
    ap.add_argument("-v","--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    need = max(args.limit, 1)
    seen: set[str] = set()
    results: List[Dict] = []

    # 汇总所有 feed 的倒序快照列表（保持“新→旧”顺序）
    feed_snapshots: List[tuple[str,str]] = []  # (timestamp, feed_url)
    for feed in args.feeds:
        try:
            tss = cdx_list_snapshots(session, feed)
            logging.info("feed=%s 快照 %d 个", feed, len(tss))
            feed_snapshots.extend((ts, feed) for ts in tss)
        except Exception as e:
            logging.warning("列举快照失败 %s: %s", feed, e)

    # 统一按时间倒序（新→旧）
    feed_snapshots.sort(key=lambda x: x[0], reverse=True)

    # 逐快照抓取解析，直到凑够 need 条
    for idx, (ts, feed) in enumerate(feed_snapshots, 1):
        if len(results) >= need:
            break
        url = wb_raw_url(ts, feed)
        try:
            r = session.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                logging.debug("跳过快照 %s HTTP %s", ts, r.status_code)
                time.sleep(args.sleep)
                continue
            items = parse_feed(r.content, source_feed=feed, snapshot_ts=ts, t2s_enabled=(not args.no_t2s))
            # 倒序合并：同一快照内部一般已是新→旧；我们为了“最新优先”，保持倒序即可
            for it in items:
                key = it["id"] or it.get("link")
                if not key or key in seen:
                    continue
                seen.add(key)
                results.append(it)
                if len(results) >= need:
                    break
        except Exception as e:
            logging.debug("抓取/解析失败 %s: %s", ts, e)
        time.sleep(args.sleep)

    # 最后按发布时间排序（缺失发布时间的放后）
    def sort_key(it: Dict):
        return (0, it["published"]) if it.get("published") else (1, it["id"])
    results.sort(key=sort_key, reverse=True)  # 最终导出也用“新→旧”顺序

    # 输出
    out = Path('/home/pcz/news_receiver/bbc/bbc_zh_test1.ndjson')
    out.parent.mkdir(parents=True, exist_ok=True)
    write_ndjson(out, results[:need])

    print(f"OK: 合并去重后共 {len(results[:need])} 条，写入 {out}", file=sys.stderr)

if __name__ == "__main__":
    main()
