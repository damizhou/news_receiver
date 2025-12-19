#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfeed_like_bbc_zh_streaming.py
按 Backfeed 思路，通过 archive.org Wayback 快照合并 BBC 中文（简体优先）"所有条目"。
修改：实时写入模式 - 抓取一次存一次，无上限，持续抓取所有快照

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
TIMEOUT = 30  # 增加到 30 秒
CDX = "https://web.archive.org/cdx/search/cdx"

# 默认同时查 simp + trad 的 RSS 快照，更稳
FEED_URLS_DEFAULT = [# 综合新闻
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


def cdx_list_snapshots(session: requests.Session, feed_url: str, limit: int | None = None, max_retries: int = 3) -> \
List[str]:
    """
    以"最新→最旧"的顺序列出某个 feed 的所有时间戳（YYYYMMDDhhmmss）。
    说明：sort=reverse 倒序仅对 exact URL 查询可用且高效。
    """
    params = {"url": feed_url, "output": "json", "fl": "timestamp,original,statuscode", "filter": "statuscode:200",
        "gzip": "false", "sort": "reverse",  # 倒序（新→旧）
    }
    if limit:
        params["limit"] = str(max(limit, 1))

    for attempt in range(max_retries):
        try:
            r = session.get(CDX, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            rows = data[1:] if data else []  # 第 1 行是表头
            return [row[0] for row in rows]
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 指数退避：1s, 2s, 4s
                logging.warning("CDX 请求超时/连接错误（尝试 %d/%d），%d 秒后重试: %s", attempt + 1, max_retries, wait,
                                feed_url)
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
                (getattr(e, "title", None) or "") + "::" + (getattr(e, "published", None) or ""))
        pub = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        upd = getattr(e, "updated_parsed", None)

        title_raw = getattr(e, "title", None)
        summary_raw = getattr(e, "summary", None)
        title = _t2s(title_raw, enabled=t2s_enabled)
        summary = _t2s(summary_raw, enabled=t2s_enabled)

        item = {"id": eid, "title": title, "link": canon or orig_link, "_orig_link": orig_link,
            "published": iso_utc(pub), "updated": iso_utc(upd), "summary": summary, "_feed": source_feed,
            "_snapshot_ts": snapshot_ts,  # 该条目首次出现的 RSS 快照时间
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
    ap = argparse.ArgumentParser(description="通过 Wayback 快照合并 BBC 中文（简体优先）所有条目（实时写入，无上限）")
    ap.add_argument("--feeds", nargs="*", default=FEED_URLS_DEFAULT, help="要合并的 RSS 源（默认 simp 系列）")
    ap.add_argument("--sleep", type=float, default=0.5, help="抓取快照间隔秒（礼貌限速，默认 0.5s）")
    ap.add_argument("--output", type=str, default="/home/pcz/news_receiver/bbc/bbc_en_all.ndjson", help="输出文件路径")
    ap.add_argument("--no-t2s", action="store_true", help="禁用繁体→简体转换（默认开启）")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s")

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

    # 逐个 feed 处理：列举快照 → 抓取 → 下一个 feed
    # 避免启动时一次性列举所有 feeds 导致超时
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
        for snap_idx, ts in enumerate(tss, 1):
            total_snapshots += 1
            url = wb_raw_url(ts, feed)
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    r = session.get(url, timeout=TIMEOUT)
                    print(f"url:{url}")
                    if r.status_code != 200:
                        logging.debug("跳过快照 %s HTTP %s", ts, r.status_code)
                        break

                    items = parse_feed(r.content, source_feed=feed, snapshot_ts=ts, t2s_enabled=(not args.no_t2s))

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
                        logging.info("Feed %d/%d 快照 %d/%d [%s] 新增 %d 条（累计 %d）", feed_idx, len(args.feeds),
                                     snap_idx, len(tss), ts, written, total_written)
                    else:
                        logging.debug("快照 %s 无新条目", ts)

                    break  # 成功则跳出重试循环

                except (requests.Timeout, requests.ConnectionError) as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning("快照抓取超时/连接错误（尝试 %d/%d），%d 秒后重试: %s", attempt + 1, max_retries,
                                        wait, ts)
                        time.sleep(wait)
                    else:
                        logging.warning("快照 %s 抓取失败（已重试 %d 次）: %s", ts, max_retries, e)
                except Exception as e:
                    logging.debug("抓取/解析失败 %s: %s", ts, e)
                    break

            time.sleep(args.sleep)

            # 每 100 个快照显示总进度
            if total_snapshots % 100 == 0:
                print(f"总进度: 已处理 {total_snapshots} 个快照，累计写入 {total_written} 条新记录", file=sys.stderr)

        logging.info("Feed %d/%d 完成，本 feed 快照数 %d", feed_idx, len(args.feeds), len(tss))

    print(f"\n✓ 完成！共处理 {len(total_snapshots)} 个快照，写入 {total_written} 条新记录", file=sys.stderr)
    print(f"✓ 输出文件: {out}", file=sys.stderr)
    print(f"✓ 文件总计: {len(seen)} 条记录", file=sys.stderr)


if __name__ == "__main__":
    main()