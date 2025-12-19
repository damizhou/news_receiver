#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIH News Releases 全量抓取（无命令行参数版）
- 遍历 https://www.nih.gov/news-events/nih-research-matters?page={n}
- 仅抓列表页字段：title / date_str / date_iso / summary / url / page_index
- 不抓详情正文
- 输出 NDJSON：nih.ndjson（断点续跑基于 URL 去重）
- 设计目标：代码清晰、可移植、可长期复跑
"""

from __future__ import annotations
import json
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List, Dict, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# =========================
# 全局配置（按需修改）
# =========================
OUTPUT_PATH: str = "nih.ndjson"   # 固定输出文件（NDJSON / JSONL）
START_PAGE: int = 0               # 从第 0 页开始（0 即第一页）
MAX_PAGES: Optional[int] = None   # 最多抓多少页；None 表示不设上限
STOP_AFTER_EMPTY: int = 3         # 连续空页阈值（越界或全重复时停止）
SLEEP_BASE: float = 0.8           # 每次请求基础 sleep（温和抓取）
TIMEOUT: float = 20.0             # HTTP 超时（秒）
RETRIES: int = 3                  # 失败重试次数
BACKOFF: float = 1.6              # 指数退避系数
DRY_RUN: bool = False             # 演练模式：不写文件，仅打印

# =========================
# 常量
# =========================
BASE = "https://www.nih.gov"
LIST_URL = f"{BASE}/news-events/nih-research-matters"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
DATE_PAT = re.compile(r"([A-Za-z]+ \d{1,2}, \d{4})")
DASH = "—"  # em-dash，列表页常用“ — ”分隔摘要

# =========================
# 数据结构
# =========================
@dataclass
class NewsItem:
    title: str
    date_str: str
    date_iso: Optional[str]
    summary: Optional[str]
    url: str
    page_index: int

# =========================
# HTTP 封装（温和 + 重试）
# =========================
class Http:
    def __init__(self, timeout: float = TIMEOUT, retries: int = RETRIES,
                 backoff: float = BACKOFF, sleep_base: float = SLEEP_BASE):
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.sleep_base = sleep_base

    def get(self, url: str, params: dict | None = None) -> requests.Response:
        # 基础延时，避免给对方带来压力
        time.sleep(self.sleep_base + random.random() * 0.4)
        last_exc = None
        for i in range(self.retries):
            try:
                r = self.sess.get(url, params=params, timeout=self.timeout)
                if r.status_code in (200, 404):
                    return r
                if r.status_code in (429, 500, 502, 503, 504, 403):
                    time.sleep((self.backoff ** i) + random.random())
                    continue
                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep((self.backoff ** i) + random.random())
        if last_exc:
            raise last_exc
        raise RuntimeError("HTTP unexpected flow")

# =========================
# 工具函数
# =========================
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def load_seen_urls(path: str) -> Set[str]:
    """从 NDJSON 读取已抓 URL，支持断点续跑。"""
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
                # 跳过坏行
                continue
    return seen

def write_ndjson(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# =========================
# 解析列表页
# =========================
def parse_list_items(html: str, page_index: int) -> List[NewsItem]:
    """
    列表页单条新闻通常呈现为：
    "<Title> <Month DD, YYYY> — <Summary>"
    这里不依赖特定 CSS 类，直接根据“日期 + em-dash”做稳健解析。
    """
    soup = BeautifulSoup(html, "html.parser")
    items: List[NewsItem] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if not href.startswith("http"):
            href = urljoin(BASE, href)
        parsed = urlparse(href)
        # 仅保留 /news-events/nih-research-matters/xxx 详情页链接
        if not parsed.path.startswith("/news-events/nih-research-matters/"):
            continue

        text = normalize_ws(a.get_text(" ").strip())
        m = DATE_PAT.search(text)
        if not m:
            # 导航等无日期的链接会被过滤掉
            continue

        date_str = m.group(1)
        try:
            date_iso = datetime.strptime(date_str, "%B %d, %Y").date().isoformat()
        except Exception:
            date_iso = None

        # 按 em-dash 分隔摘要
        summary = None
        if f" {DASH} " in text:
            left, _, right = text.partition(f" {DASH} ")
            summary = right.strip() or None
            title = normalize_ws(left.replace(date_str, "").strip())
        else:
            title = normalize_ws(text.replace(date_str, "").strip())

        title = title.rstrip(" -—:;").strip()

        items.append(NewsItem(
            title=title,
            date_str=date_str,
            date_iso=date_iso,
            summary=summary,
            url=href,
            page_index=page_index
        ))

    # 同页按 URL 去重
    uniq: Dict[str, NewsItem] = {}
    for it in items:
        if it.url not in uniq:
            uniq[it.url] = it
    return list(uniq.values())

# =========================
# 主抓取流程
# =========================
def crawl_all() -> None:
    http = Http()
    seen = load_seen_urls(OUTPUT_PATH)
    total_new = 0
    page = START_PAGE
    empty_pages = 0

    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_PATH)) or ".", exist_ok=True)

    while True:
        list_url = LIST_URL if page == 0 else f"{LIST_URL}?page={page}"
        r = http.get(list_url)

        if r.status_code == 404:
            print(f"[{page}] 404（越界）。结束。")
            break

        if r.status_code != 200:
            print(f"[{page}] HTTP {r.status_code}，跳过并继续。")
            page += 1
            continue

        items = parse_list_items(r.text, page_index=page)
        new_items = [it for it in items if it.url not in seen]

        print(f"[{page}] 提取 {len(items)} 条；新 {len(new_items)} 条；累计新 {total_new}。")

        if not new_items:
            empty_pages += 1
        else:
            empty_pages = 0

        for it in new_items:
            rec = asdict(it)
            if not DRY_RUN:
                write_ndjson(OUTPUT_PATH, rec)
            seen.add(it.url)
            total_new += 1

        page += 1

        # 停止条件 1：达到上限页
        if MAX_PAGES is not None and page >= START_PAGE + MAX_PAGES:
            print(f"达到 MAX_PAGES={MAX_PAGES}，结束。")
            break
        # 停止条件 2：连续空页（越过最后一页或全重复）
        if empty_pages >= STOP_AFTER_EMPTY:
            print(f"连续空页 {empty_pages} 次（可能已越过最后有效页）。结束。")
            break

    print(f"完成：新增 {total_new} 条。输出文件：{OUTPUT_PATH}（DRY_RUN={DRY_RUN}）")

# =========================
# 入口
# =========================
if __name__ == "__main__":
    crawl_all()
