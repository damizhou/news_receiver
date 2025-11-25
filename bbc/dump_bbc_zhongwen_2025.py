
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dump_bbc_zhongwen_2025.py
从 Wayback Machine 合并还原 BBC 中文 2025 年 RSS 全量条目。
依赖：requests, feedparser

用法：
  python dump_bbc_zhongwen_2025.py --out bbc_zhongwen_2025.ndjson --year 2025 -v
  # 如需 CSV：
  python dump_bbc_zhongwen_2025.py --out bbc_zhongwen_2025.csv --csv -v
"""
from __future__ import annotations
import argparse, json, logging, time, sys
from pathlib import Path
from typing import List, Dict, Tuple, Iterable, Optional
from urllib.parse import urlparse, urlunparse

import requests, feedparser

CDX = "https://web.archive.org/cdx/search/cdx"  # Wayback CDX API
UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
TIMEOUT = 20

FEEDS = [
    "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml",
    "https://feeds.bbci.co.uk/zhongwen/trad/rss.xml",
]

def get_cdx_timestamps(session: requests.Session, url: str, year: int) -> List[str]:
    """
    返回该 feed 在某年内 status=200 的所有 Wayback 时间戳（YYYYMMDDhhmmss）。
    文档：Wayback CDX Server API / Wayback APIs
    """
    params = {
        "url": url,
        "from": str(year),
        "to":   str(year),
        "output": "json",
        "filter": "statuscode:200",
        "fl": "timestamp,original,statuscode",
        "gzip": "false",
    }
    r = session.get(CDX, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # 第一行是表头；其后每行为 [timestamp, original, statuscode]
    ts = [row[0] for row in data[1:]] if data else []
    # 去重并排序
    return sorted(set(ts))

def wb_raw_url(ts: str, original: str) -> str:
    # 使用 id_ 取得“原始响应体”（不注入 Wayback HTML）
    return f"https://web.archive.org/web/{ts}id_/{original}"

def canonicalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return url
    p = urlparse(url)
    netloc = p.netloc.lower()
    path = p.path
    # 仅对 /zhongwen/articles/<slug>/<lang> 做语种统一（trad/simp -> simp）
    parts = path.strip("/").split("/")
    if len(parts) >= 4 and parts[0] == "zhongwen" and parts[1] == "articles":
        if parts[-1] in ("trad", "simp"):
            parts[-1] = "simp"
            path = "/" + "/".join(parts)
    return urlunparse((p.scheme, netloc, path, "", "", ""))  # 去掉 query/fragment

def parse_feed(content: bytes, source_feed: str) -> List[Dict]:
    feed = feedparser.parse(content)
    items = []
    for e in feed.entries:
        orig_link = getattr(e, "link", None)
        canon = canonicalize_url(orig_link)
        eid = canonicalize_url(getattr(e, "id", None) or getattr(e, "guid", None) or orig_link) or (
            (getattr(e,"title",None) or "") + "::" + (getattr(e,"published",None) or "")
        )
        pub = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        upd = getattr(e, "updated_parsed", None)
        def _iso(ts):
            if not ts: return None
            from datetime import datetime, timezone
            dt = datetime(*ts[:6], tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00","Z")
        items.append({
            "id": eid,
            "title": getattr(e, "title", None),
            "link": canon or orig_link,
            "_orig_link": orig_link,
            "published": _iso(pub),
            "updated": _iso(upd),
            "summary": getattr(e, "summary", None),
            "_feed": source_feed,
        })
    return items

def dump_year(feeds: List[str], year: int, out: Path, csv: bool, sleep: float, verbose: bool) -> int:
    session = requests.Session()
    # session.headers.update({"User-Agent": UA})
    seen: set[str] = set()
    all_items: List[Dict] = []

    for feed in feeds:
        logging.info("查询 CDX：%s", feed)
        tss = get_cdx_timestamps(session, feed, year)
        logging.info("发现 %d 个快照", len(tss))
        for i, ts in enumerate(tss, 1):
            url = wb_raw_url(ts, feed)
            try:
                r = session.get(url, timeout=TIMEOUT)
                if r.status_code != 200:
                    logging.warning("快照 %s HTTP %s", ts, r.status_code); continue
                items = parse_feed(r.content, source_feed=feed)
                # 旧→新排序更稳，处理顺序不重要；关键是去重键
                for it in items:
                    key = it["id"] or it.get("link")
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    all_items.append(it)
                if verbose and i % 10 == 0:
                    logging.info("进度 %d/%d，累计唯一条目 %d", i, len(tss), len(all_items))
            except Exception as e:
                logging.warning("抓取/解析失败 %s: %s", ts, e)
            time.sleep(sleep)  # 礼貌限速，避免给 Wayback 施压

    # 最终按发布时间排序（缺失时间的放最后）
    def sort_key(it):
        return (0, it["published"]) if it.get("published") else (1, it["id"])
    all_items.sort(key=sort_key)

    # 输出
    out.parent.mkdir(parents=True, exist_ok=True)
    if csv:
        import csv as _csv
        fields = ["id","title","link","published","updated","summary","_feed","_orig_link"]
        with out.open("w", encoding="utf-8", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for it in all_items:
                w.writerow({k: (it.get(k) or "") for k in fields})
    else:
        with out.open("w", encoding="utf-8") as f:
            for it in all_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
    return len(all_items)

def main():
    ap = argparse.ArgumentParser(description="合并 Wayback 快照还原 BBC 中文 2025 年 RSS 全量")
    ap.add_argument("--year", type=int, default=2025, help="年份（默认 2025）")
    ap.add_argument("--csv", action="store_true", help="以 CSV 输出（默认 NDJSON）")
    ap.add_argument("--sleep", type=float, default=0.4, help="快照抓取间隔秒，礼貌限速")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s"
    )
    out = Path('bbc_zhongwen_2025.ndjson')
    total = dump_year(FEEDS, args.year, out, args.csv, args.sleep, args.verbose)
    print(f"OK: {args.year} 合计唯一条目 {total}", file=sys.stderr)

if __name__ == "__main__":
    main()
