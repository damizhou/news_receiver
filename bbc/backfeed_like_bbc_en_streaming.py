#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfeed_like_bbc_en_streaming.py
按 Backfeed 思路，通过 archive.org Wayback 快照合并 BBC 英文所有条目。
实时写入模式 - 抓取一次存一次，无上限，持续抓取所有快照

依赖：
  pip install requests feedparser
"""
from __future__ import annotations
import argparse, json, logging, time, sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Iterable
from urllib.parse import urlparse, urlunparse

import requests, feedparser
from datetime import datetime, timezone

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
TIMEOUT = 30
CDX = "https://web.archive.org/cdx/search/cdx"

# BBC 英文 RSS feeds
FEED_URLS_DEFAULT = [
    # 综合新闻
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/uk/rss.xml",

    # 地区新闻
    "https://feeds.bbci.co.uk/news/world/africa/rss.xml",
    "https://feeds.bbci.co.uk/news/world/asia/rss.xml",
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
    "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
    "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
    "https://feeds.bbci.co.uk/news/world/latin_america/rss.xml",

    # 专题
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "https://feeds.bbci.co.uk/news/health/rss.xml",
    "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
    "https://feeds.bbci.co.uk/news/education/rss.xml",
]


def cdx_list_snapshots(session: requests.Session, feed_url: str, limit: int | None = None, max_retries: int = 3) -> List[str]:
    """
    以"最新→最旧"的顺序列出某个 feed 的所有时间戳（YYYYMMDDhhmmss）。
    """
    params = {
        "url": feed_url,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "gzip": "false",
        "sort": "reverse",
    }
    if limit:
        params["limit"] = str(max(limit, 1))

    for attempt in range(max_retries):
        try:
            r = session.get(CDX, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            rows = data[1:] if data else []
            return [row[0] for row in rows]
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logging.warning("CDX 请求超时/连接错误（尝试 %d/%d），%d 秒后重试: %s", attempt + 1, max_retries, wait, feed_url)
                time.sleep(wait)
            else:
                raise
    return []


def wb_raw_url(ts: str, original: str) -> str:
    """Wayback 原始响应（不注入 replay HTML）"""
    return f"https://web.archive.org/web/{ts}id_/{original}"


def iso_utc(ts_struct) -> Optional[str]:
    if not ts_struct:
        return None
    dt = datetime(*ts_struct[:6], tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def canonicalize_bbc_url(url: Optional[str]) -> Optional[str]:
    """
    规范化 BBC 链接：
    - 域名小写
    - 去 query / fragment
    """
    if not url:
        return url
    p = urlparse(url)
    netloc = p.netloc.lower()
    path = p.path
    return urlunparse((p.scheme, netloc, path, "", "", ""))


def parse_feed(content: bytes, source_feed: str, snapshot_ts: str) -> List[Dict]:
    fp = feedparser.parse(content)
    out: List[Dict] = []
    for e in fp.entries:
        orig_link = getattr(e, "link", None)
        canon = canonicalize_bbc_url(orig_link)
        eid = canonicalize_bbc_url(getattr(e, "id", None) or getattr(e, "guid", None) or orig_link) or (
                (getattr(e, "title", None) or "") + "::" + (getattr(e, "published", None) or ""))
        pub = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        upd = getattr(e, "updated_parsed", None)

        title = getattr(e, "title", None)
        summary = getattr(e, "summary", None)

        item = {
            "id": eid,
            "title": title,
            "link": canon or orig_link,
            "_orig_link": orig_link,
            "published": iso_utc(pub),
            "updated": iso_utc(upd),
            "summary": summary,
            "_feed": source_feed,
            "_snapshot_ts": snapshot_ts,
        }
        out.append(item)
    return out


def append_ndjson(path: Path, items: Iterable[Dict]) -> int:
    """追加写入 NDJSON，返回实际写入条数"""
    count = 0
    with path.open("a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
            count += 1
    return count


def load_existing_ids(path: Path) -> set[str]:
    """从现有 NDJSON 文件加载已存在的 ID"""
    if not path.exists():
        return set()
    seen = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                key = item.get("id") or item.get("link")
                if key:
                    seen.add(key)
            except Exception:
                continue
    return seen


def main():
    ap = argparse.ArgumentParser(description="通过 Wayback 快照合并 BBC 英文所有条目（实时写入，无上限）")
    ap.add_argument("--feeds", nargs="*", default=FEED_URLS_DEFAULT, help="要合并的 RSS 源")
    ap.add_argument("--sleep", type=float, default=0.5, help="抓取快照间隔秒（礼貌限速，默认 0.5s）")
    ap.add_argument("--output", type=str, default="bbc_en_all.ndjson", help="输出文件路径")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    # 加载已存在的 ID，避免重复
    logging.info("加载现有数据以避免重复...")
    seen = load_existing_ids(out)
    logging.info("已存在 %d 条记录", len(seen))

    total_written = 0
    total_snapshots = 0

    # 逐个 feed 处理
    for feed_idx, feed in enumerate(args.feeds, 1):
        logging.info("=" * 60)
        logging.info("处理 Feed %d/%d: %s", feed_idx, len(args.feeds), feed)

        # 列举当前 feed 的快照
        try:
            tss = cdx_list_snapshots(session, feed)
            logging.info("快照数量: %d", len(tss))
        except Exception as e:
            logging.warning("列举快照失败，跳过此 feed: %s", e)
            continue

        if not tss:
            logging.info("无可用快照，跳过")
            continue

        # 抓取当前 feed 的所有快照
        # 空内容限制：跟踪每100个快照的写入数量
        batch_snapshot_count = 0  # 当前批次已处理的快照数
        batch_written_count = 0   # 当前批次写入的记录数

        for snap_idx, ts in enumerate(tss, 1):
            total_snapshots += 1
            batch_snapshot_count += 1
            url = wb_raw_url(ts, feed)
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    r = session.get(url, timeout=TIMEOUT)
                    if r.status_code != 200:
                        logging.debug("跳过快照 %s HTTP %s", ts, r.status_code)
                        break

                    items = parse_feed(r.content, source_feed=feed, snapshot_ts=ts)

                    # 过滤已存在的条目
                    new_items = []
                    for it in items:
                        key = it["id"] or it.get("link")
                        if not key or key in seen:
                            continue
                        seen.add(key)
                        new_items.append(it)

                    # 实时追加写入
                    if new_items:
                        written = append_ndjson(out, new_items)
                        total_written += written
                        batch_written_count += written
                        logging.info("Feed %d/%d 快照 %d/%d [%s] 新增 %d 条（累计 %d）",
                                     feed_idx, len(args.feeds), snap_idx, len(tss), ts, written, total_written)
                    else:
                        logging.debug("快照 %s 无新条目", ts)

                    break

                except (requests.Timeout, requests.ConnectionError) as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning("快照抓取超时/连接错误（尝试 %d/%d），%d 秒后重试: %s",
                                        attempt + 1, max_retries, wait, ts)
                        time.sleep(wait)
                    else:
                        logging.warning("快照 %s 抓取失败（已重试 %d 次）: %s", ts, max_retries, e)
                except Exception as e:
                    logging.debug("抓取/解析失败 %s: %s", ts, e)
                    break

            time.sleep(args.sleep)

            # 空内容限制：每100个快照检查写入数量
            if batch_snapshot_count >= 100:
                if batch_written_count < 10:
                    logging.warning("Feed %d/%d 空内容限制触发：100个快照仅写入 %d 条记录（<10），跳过剩余快照",
                                    feed_idx, len(args.feeds), batch_written_count)
                    print(f"Feed {feed_idx}/{len(args.feeds)} 空内容限制：100个快照仅写入 {batch_written_count} 条，跳过剩余 {len(tss) - snap_idx} 个快照",
                          file=sys.stderr)
                    break
                # 重置计数器，开始新的100个快照批次
                batch_snapshot_count = 0
                batch_written_count = 0

            # 每 100 个快照显示总进度
            if total_snapshots % 100 == 0:
                print(f"总进度: 已处理 {total_snapshots} 个快照，累计写入 {total_written} 条新记录", file=sys.stderr)

        logging.info("Feed %d/%d 完成，本 feed 快照数 %d", feed_idx, len(args.feeds), len(tss))

    print(f"\n完成！共处理 {total_snapshots} 个快照，写入 {total_written} 条新记录", file=sys.stderr)
    print(f"输出文件: {out}", file=sys.stderr)
    print(f"文件总计: {len(seen)} 条记录", file=sys.stderr)


if __name__ == "__main__":
    main()
