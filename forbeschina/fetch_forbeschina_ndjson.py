#!/usr/bin/env python3
from __future__ import annotations
import re
import subprocess
import time
from typing import List, Dict
from bs4 import BeautifulSoup
from tools.chrome import create_chrome_driver
import json
from pathlib import Path
from typing import Iterable, Mapping, Any
# https://forbeschina.com/channels/api?action=loadArticles&pn={2}&path=innovation&code=innovation&cid=
# https://forbeschina.com/channels/api?action=loadArticles2&pn=2&path=activity&channel_id=5 活动
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=insights&code=industry_research&cid= Insight白皮书/特别报告/行业研究
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=insights&code=study_enjoy&cid= 研享行
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=leadership&code=leadership&cid= 领导力
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=business&code=%E5%95%86%E4%B8%9A&cid= 商业
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=youth&code=youth&cid= 青年
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=investment&code=%E6%8A%95%E8%B5%84&cid= 投资
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=life&code=%E7%94%9F%E6%B4%BB%C2%B7%E6%96%87%E5%A8%B1&cid= 生活·文娱
# https://forbeschina.com/channels/api?action=loadArticles&pn=2&path=woman&code=woman&cid= 女性
# 0 - 21
def parse_forbeschina_list_html(page_source: str,
                                domain: str = "www.forbeschina.com",
                                section: str = "leadership") -> List[Dict[str, Any]]:
    """
    从 driver.page_source（ForbesChina 列表页 HTML 片段/页面）中解析文章条目。
    返回字段：id, section, url, title, desc, author_name, author_url, author_id, date_cn, date_iso, image

    用法：
        html = driver.page_source
        items = parse_forbeschina_list_html(html)
        # 写 NDJSON:
        # import json
        # print("\n".join(json.dumps(x, ensure_ascii=False) for x in items))
    """
    soup = BeautifulSoup(page_source, "html.parser")
    blocks = soup.select("div.item.new_list") or soup.select("div.item")

    # 背景图的 url('...') 提取
    bg_url_re = re.compile(r"url\(\s*['\"]?\s*(?P<u>[^)'\"]+)\s*['\"]?\s*\)", re.IGNORECASE)

    out: List[Dict[str, Any]] = []

    for b in blocks:
        try:
            info = b.select_one("div.info")
            if not info:
                continue

            # 日期：如 2025年10月15日
            date_cn = (info.select_one("p.s") or {}).get_text(strip=True)
            m = re.match(r"^\s*(\d{4})年(\d{2})月(\d{2})日\s*$", date_cn or "")
            date_iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""

            # 标题/详情链接
            a_title = info.select_one("h4.title a")
            if not a_title:
                continue
            title = a_title.get_text(strip=True)
            href = (a_title.get("href") or "").strip()
            # 文章 ID：/leadership/70498
            mid = re.search(r"/(\d+)(?:/)?$", href)
            if not mid:
                continue
            art_id = int(mid.group(1))

            # 绝对链接
            url = href if href.startswith(("http://", "https://")) else f"https://{domain}{href if href.startswith('/') else '/' + href}"

            # 描述
            desc = (info.select_one("p.desc") or {}).get_text(strip=True)

            # 作者（通常在<p class="s">里最后一个<a>）
            author_name, author_url, author_id = "", "", None
            a_list = info.select("p.s a")
            if a_list:
                a_author = a_list[-1]
                author_name = a_author.get_text(strip=True)
                au_href = (a_author.get("href") or "").strip()
                author_url = au_href if au_href.startswith(("http://", "https://")) else f"https://{domain}{au_href if au_href.startswith('/') else '/' + au_href}"
                m2 = re.search(r"/author/(\d+)", au_href)
                author_id = int(m2.group(1)) if m2 else None

            # 图片（background-image: url(' ... ');）
            image = None
            a_img = b.select_one("div.imgBox a.img")
            if a_img:
                style = a_img.get("style", "")
                m3 = bg_url_re.search(style)
                if m3:
                    image = m3.group("u").strip()

            out.append({
                "id": art_id,
                "section": section,
                "url": url,
                "title": title,
                "desc": desc,
                "author_name": author_name,
                "author_url": author_url,
                "author_id": author_id,
                "date_cn": date_cn.strip() if isinstance(date_cn, str) else "",
                "date_iso": date_iso,
                "image": image,
            })
        except Exception:
            # 单条失败不影响整体
            continue

    return out

def append_ndjson(items: Iterable[Mapping[str, Any]], out_file: str | Path) -> int:
    """
    将 items（字典列表）追加写入到 NDJSON 文件。
    返回成功写入的条数。
    """
    path = Path(out_file)
    path.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with path.open("r", encoding="utf-8") as f:
        seen = set(f.readlines())

    with path.open("a", encoding="utf-8") as f:
        for obj in items:
            # 保险起见，仅写入可序列化的 dict
            line = json.dumps(dict(obj), ensure_ascii=False) + "\n"
            if line in seen:
                continue
            f.write(line)
            seen.add(line)
            n += 1
    return n


# 清除浏览器进程
def kill_chrome_processes():
    try:
        # Run the command to kill all processes containing 'chrome'
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['pkill', '-f', 'google-chrome'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")

kill_chrome_processes()
driver = create_chrome_driver()
driver.get("https://www.voachinese.com/a/8017961.html")
time.sleep(20)
print(f"{driver.current_url}")
