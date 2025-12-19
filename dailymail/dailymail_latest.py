#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily Mail 最新 N 条抓取器（仅 archive 模式，输出 NDJSON）
- 先试 /home/sitemaparchive/day_YYYYMMDD.html
- 若不可用，则回退 /home/sitemaparchive/index.html?d=YYYY-MM-DD
"""
from __future__ import annotations
import argparse, dataclasses, datetime as dt, json, re, sys, time
from typing import Iterable, List, Set
import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from bs4 import BeautifulSoup  # 可选，提升解析稳健性
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False

BASE = "https://www.dailymail.co.uk"
DAY_URL_TMPL    = BASE + "/home/sitemaparchive/day_{date_compact}.html"  # YYYYMMDD
INDEX_URL_TMPL  = BASE + "/home/sitemaparchive/index.html?d={date}"      # YYYY-MM-DD

@dataclasses.dataclass
class Article:
    title: str
    url: str
    date: str  # YYYY-MM-DD
    def key(self) -> str:
        u = re.sub(r"[?#].*$", "", self.url.strip())
        return u[:-1] if u.endswith("/") else u

def _with_timeout(request_func, timeout: int):
    def wrapper(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return request_func(method, url, **kwargs)
    return wrapper

def make_session(timeout: int = 20) -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.6,
                    status_forcelist=(429,500,502,503,504),
                    allowed_methods=frozenset(["GET","HEAD"]),
                    raise_on_status=False)
    ad = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=16)
    s.mount("http://", ad); s.mount("https://", ad)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; DM-ArchiveFetcher/1.1)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    s.request = _with_timeout(s.request, timeout)
    return s

def fetch_archive_html(session: requests.Session, day: dt.date) -> str:
    """优先 day_YYYYMMDD.html；失败回退 index.html?d=YYYY-MM-DD。均返回 2xx 才算成功。"""
    candidates = [
        # DAY_URL_TMPL.format(date_compact=day.strftime("%Y%m%d")),
        INDEX_URL_TMPL.format(date=day.isoformat()),
    ]
    last_err = None
    for u in candidates:
        try:
            resp = session.get(u)
            if 200 <= resp.status_code < 300 and resp.text.strip():
                return resp.text
        except Exception as e:
            last_err = e
    raise RuntimeError(f"归档页获取失败（{day.isoformat()}）：{last_err or 'HTTP 非 2xx'}")

def parse_archive(html: str, date_str: str) -> List[Article]:
    arts: List[Article] = []; seen: Set[str] = set()
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "article-" in href and href.endswith(".html"):
                url = href if href.startswith("http") else (BASE + href)
                key = re.sub(r"[?#].*$", "", url)
                if key in seen: continue
                title = a.get_text(strip=True) or ""
                arts.append(Article(title=title, url=url, date=date_str)); seen.add(key)
    else:
        for m in re.finditer(r'href="(?P<u>/[^\"]*?article-\d+[^\"]*?\.html)"', html):
            url = BASE + m.group("u"); key = re.sub(r"[?#].*$", "", url)
            if key in seen: continue
            arts.append(Article(title="", url=url, date=date_str)); seen.add(key)
    return arts

def daterange_backwards(start: dt.date, max_days: int) -> Iterable[dt.date]:
    for i in range(max_days):
        yield start - dt.timedelta(days=i)

def save_ndjson(path: str, items: List[Article]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for a in items:
            f.write(json.dumps(dataclasses.asdict(a), ensure_ascii=False) + "\n")

def append_ndjson(path: str, item: Article) -> None:
    """追加单条记录到 ndjson 文件"""
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(dataclasses.asdict(item), ensure_ascii=False) + "\n")

def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch latest Daily Mail via daily archive, output NDJSON (无限抓取).")
    ap.add_argument("--out", "-o", default="dailymail_all1.ndjson")
    ap.add_argument("--from-date", default=None)      # YYYY-MM-DD
    ap.add_argument("--max-days", type=int, default=36500)  # 默认约100年，实际无限
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.from_date) if args.from_date else dt.date.today()
    session = make_session(timeout=args.timeout)

    # 清空输出文件
    with open(args.out, "w", encoding="utf-8") as f:
        pass

    total_written = 0
    seen: Set[str] = set()
    for day in daterange_backwards(start, args.max_days):
        try:
            html = fetch_archive_html(session, day)
        except Exception as e:
            print(f"[WARN] {day} 获取失败：{e}", file=sys.stderr); time.sleep(args.sleep); continue
        arts = parse_archive(html, day.isoformat())
        added = 0
        for a in arts:
            k = a.key()
            if k in seen: continue
            seen.add(k)
            append_ndjson(args.out, a)
            total_written += 1
            added += 1
        print(f"[INFO] {day} 抽取 {len(arts)} 条，新增 {added} 条，累计 {total_written} 条")
        time.sleep(args.sleep)

    print(f"[OK] 输出 {total_written} 条 → {args.out}")

if __name__ == "__main__":
    main()
