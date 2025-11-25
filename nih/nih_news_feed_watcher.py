#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIH News Releases RSS/Atom 订阅抓取（持续运行 + 与 nih.ndjson 去重）
- 拉取: https://www.nih.gov/news-releases/feed.xml
- 解析 RSS 或 Atom，产出与站点抓取脚本相同字段：
    title / date_str / date_iso / summary / url / page_index(-1)
- 输出文件固定为: nih.ndjson（NDJSON/JSONL，每行一条）
- 去重策略：读取 nih.ndjson 中已有 URL，增量写入，仅追加新条目
- 所有配置为全局变量，无命令行参数；默认循环执行，温和速率、带重试
"""

from __future__ import annotations
import json
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin

import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

# =============== 全局配置（按需修改） ===============
FEED_URL: str = "https://www.nih.gov/news-releases/feed.xml"
OUTPUT_PATH: str = "nih.ndjson"     # 与站点抓取脚本保持一致
RUN_FOREVER: bool = True            # True=持续轮询; False=拉一次就退出
TIMEOUT: float = 20.0               # HTTP 超时
RETRIES: int = 3                    # 失败重试次数
BACKOFF: float = 1.6                # 指数退避系数
SLEEP_JITTER: float = 0.4           # 每次请求的随机抖动
DRY_RUN: bool = False               # 演练模式，不写文件

# =============== 常量 ===============
BASE = "https://www.nih.gov"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

# =============== 工具函数 ===============
def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def strip_html(s: str) -> str:
    if not s:
        return ""
    # 非严格 HTML 去标签，足够应对常见 <p>, <br>, <em> 等
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    return normalize_ws(s)

def load_seen_urls(path: str) -> Set[str]:
    seen: Set[str] = set()
    if not os.path.exists(path):
        return seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                url = obj.get("url")
                if url:
                    seen.add(url)
            except Exception:
                continue
    return seen

def write_ndjson(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def http_get_with_retries(url: str) -> Optional[str]:
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept": "application/rss+xml, application/xml;q=0.9,*/*;q=0.8"})
    last_exc = None
    for i in range(RETRIES):
        try:
            # 温和一点，带轻微抖动
            time.sleep(0.6 + random.random() * SLEEP_JITTER)
            r = sess.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504, 403):
                time.sleep((BACKOFF ** i) + random.random())
                continue
            # 其它状态也直接返回文本（可能是 301/302 HTML），交给解析处理
            return r.text
        except requests.RequestException as e:
            last_exc = e
            time.sleep((BACKOFF ** i) + random.random())
    # 最终失败
    if last_exc:
        print(f"[{ts()}] ERROR http_get: {last_exc}")
    return None

# =============== XML 解析（兼容 RSS 与 Atom） ===============
def _find_text_any(elem: ET.Element, tags: List[str]) -> Optional[str]:
    """在任意命名空间下查找首个匹配文本，例如 tags=['title','{*}title']"""
    for tag in tags:
        # 先尝试无命名空间
        t = elem.find(tag)
        if t is not None and t.text:
            return t.text
        # 再尝试通配命名空间
        t = elem.find(f".//{{*}}{tag}")
        if t is not None and t.text:
            return t.text
    return None

def _find_link(elem: ET.Element) -> Optional[str]:
    """
    RSS: <link>http...</link>
    Atom: <link href="http..." rel="alternate"/>
    """
    # RSS 形式
    val = _find_text_any(elem, ["link"])
    if val and val.strip():
        return val.strip()
    # Atom 形式
    for link in elem.findall(".//{*}link"):
        href = link.attrib.get("href")
        if href:
            # 若有 rel="alternate" 优先
            rel = link.attrib.get("rel", "")
            if rel in ("alternate", "", None):
                return href
    return None

def _find_pub_date(elem: ET.Element) -> (str, Optional[str]):
    """
    返回: (date_str, date_iso)
    RSS 常见: <pubDate>Tue, 15 Oct 2024 12:34:56 GMT</pubDate>
    Atom 常见: <updated>2024-10-15T12:34:56Z</updated> / <published>...</published>
    """
    raw = _find_text_any(elem, ["pubDate", "updated", "published"])
    if not raw:
        return ("", None)

    raw = raw.strip()
    # 先尝试 RFC822
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        date_iso = dt.date().isoformat()
        # 友好的英文月日格式
        date_str = dt.strftime("%B %d, %Y")
        return (date_str, date_iso)
    except Exception:
        pass

    # 再尝试 ISO 8601（Atom）
    try:
        # 允许结尾 'Z'
        if raw.endswith("Z"):
            raw2 = raw.replace("Z", "+00:00")
        else:
            raw2 = raw
        dt = datetime.fromisoformat(raw2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        date_iso = dt.date().isoformat()
        date_str = dt.strftime("%B %d, %Y")
        return (date_str, date_iso)
    except Exception:
        return (raw, None)

def parse_feed(xml_text: str) -> List[Dict]:
    """
    解析 RSS/Atom，返回标准化记录：
      {title, date_str, date_iso, summary, url, page_index(-1)}
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    items: List[Dict] = []

    # RSS: channel/item
    channel = root.find("channel") or root.find(".//channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = normalize_ws(_find_text_any(it, ["title"]) or "")
            link = _find_link(it) or ""
            link = urljoin(BASE, link) if link and link.startswith("/") else link
            date_str, date_iso = _find_pub_date(it)
            desc = _find_text_any(it, ["description", "content", "summary"]) or ""
            summary = strip_html(desc)

            if link:
                items.append({
                    "title": title,
                    "date_str": date_str,
                    "date_iso": date_iso,
                    "summary": summary or None,
                    "url": link,
                    "page_index": -1
                })

        # 已找到 RSS 项，直接返回
        if items:
            # 去重（同一 feed 里的偶发重复）
            uniq: Dict[str, Dict] = {}
            for rec in items:
                if rec["url"] not in uniq:
                    uniq[rec["url"]] = rec
            return list(uniq.values())

    # Atom: feed/entry
    for it in root.findall(".//{*}entry"):
        title = normalize_ws(_find_text_any(it, ["title"]) or "")
        link = _find_link(it) or ""
        link = urljoin(BASE, link) if link and link.startswith("/") else link
        date_str, date_iso = _find_pub_date(it)
        # Atom 的摘要可能在 <summary> 或 <content>
        desc = _find_text_any(it, ["summary", "content"]) or ""
        summary = strip_html(desc)

        if link:
            items.append({
                "title": title,
                "date_str": date_str,
                "date_iso": date_iso,
                "summary": summary or None,
                "url": link,
                "page_index": -1
            })

    # 去重
    uniq: Dict[str, Dict] = {}
    for rec in items:
        if rec["url"] not in uniq:
            uniq[rec["url"]] = rec
    return list(uniq.values())

# =============== 主流程（持续或单次） ===============
def run_once() -> int:
    """抓取一次 feed，追加写入新纪录；返回新增数。"""
    xml_text = http_get_with_retries(FEED_URL)
    if not xml_text:
        print(f"[{ts()}] WARN 无法获取 feed。")
        return 0

    records = parse_feed(xml_text)
    print(f"[{ts()}] 解析到 {len(records)} 条。")

    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_PATH)) or ".", exist_ok=True)
    seen = load_seen_urls(OUTPUT_PATH)
    new_count = 0

    for rec in records:
        if rec["url"] in seen:
            continue
        if not DRY_RUN:
            write_ndjson(OUTPUT_PATH, rec)
        seen.add(rec["url"])
        new_count += 1

    print(f"[{ts()}] 新增 {new_count} 条 -> {OUTPUT_PATH}（DRY_RUN={DRY_RUN}）")
    return new_count

def main_loop():
    run_once()

if __name__ == "__main__":
    main_loop()
