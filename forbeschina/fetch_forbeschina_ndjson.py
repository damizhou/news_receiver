#!/usr/bin/env python3
from __future__ import annotations
import re
import subprocess
from typing import List, Dict
from bs4 import BeautifulSoup
import json
from pathlib import Path
from typing import Iterable, Mapping, Any
from chrome import create_chrome_driver
import time

RATE_LIMIT_SLEEP = 600          # 访问过快时，回退等待的时间（秒）= 10 分钟
MAX_RATE_LIMIT_RETRY = 10        # 同一页最多回退重试次数
OUT_FILE = 'forbeschina.ndjson'
url_list = [
    # 创新
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=innovation&code=innovation&cid=",

    # 活动
    # "https://forbeschina.com/channels/api?action=loadArticles2&pn={pn}&path=activity&channel_id=5",
    #
    # # Insight 白皮书 / 特别报告 / 行业研究
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=insights&code=industry_research&cid=",
    #
    # # 研享行
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=insights&code=study_enjoy&cid=",
    #
    # # 领导力
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=leadership&code=leadership&cid=",
    #
    # # 商业
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=business&code=%E5%95%86%E4%B8%9A&cid=",
    #
    # # 青年
    # "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=youth&code=youth&cid=",

    # 投资
    "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=investment&code=%E6%8A%95%E8%B5%84&cid=",

    # 生活·文娱
    "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=life&code=%E7%94%9F%E6%B4%BB%C2%B7%E6%96%87%E5%A8%B1&cid=",

    # 女性
    "https://forbeschina.com/channels/api?action=loadArticles&pn={pn}&path=woman&code=woman&cid=",
]

# 0 - 21
def parse_forbeschina_list_html(page_source: str,
                                domain: str = "www.forbeschina.com") -> List[Dict[str, Any]]:
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

    # 如果文件还不存在，seen 设为空集合
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            seen = set(f.readlines())
    else:
        seen = set()

    n = 0
    with path.open("a", encoding="utf-8") as f:
        for obj in items:
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
pn = 0
blank_count = 0
url_num = 0
for tmpl in url_list:
    pn = 0
    blank_count = 0
    rate_limit_retry = 0        # 当前频道的“访问过快”重试计数

    while True:
        pn += 1
        url = tmpl.format(pn=pn)
        print(f"[INFO] Fetching: {url}")

        driver.get(url)
        html = driver.page_source
        text = html.strip()

        # 1）空页处理：连续 3 页几乎没内容就认为这个频道到头了
        if len(text) < 100:
            blank_count += 1
            print(f"[WARN] blank page #{blank_count} for {url}")
            if blank_count >= 3:
                print(f"[INFO] too many blank pages, stop this channel, tmpl={tmpl}")
                pn = 0
                break
            # 空页也稍微等一下，避免太狂暴
            time.sleep(3)
            continue
        else:
            blank_count = 0

        # 2）这里可以根据页面内容判断“访问过快”，如果你发现有固定提示文案可以加进去
        # 例如：if "访问过于频繁" in text 或 "Too Many Requests" in text 之类
        # 下面先用“解析结果为空”来当作访问过快的信号

        items = parse_forbeschina_list_html(html)

        # 解析结果为空：认为可能是访问过快 / 被限流，回退并等待 10 分钟后重试同一页
        if not items:
            rate_limit_retry += 1
            print(f"[WARN] no items parsed for pn={pn}, maybe rate limited. "
                  f"retry={rate_limit_retry}/{MAX_RATE_LIMIT_RETRY}, sleep {RATE_LIMIT_SLEEP}s")

            # 回退 pn，让下一轮 while 还是访问同一页
            pn -= 1

            # 超过最大重试次数就放弃这个频道，避免死循环
            if rate_limit_retry >= MAX_RATE_LIMIT_RETRY:
                print(f"[INFO] reach max rate-limit retry for tmpl={tmpl}, stop this channel")
                pn = 0
                break

            time.sleep(RATE_LIMIT_SLEEP)
            continue

        # 有正常数据，重置“访问过快”计数
        rate_limit_retry = 0
        new_n = append_ndjson(items, OUT_FILE)
        print(f"[INFO] wrote {new_n} new items to {OUT_FILE}")
        url_num += new_n

    if url_num >= 10000:
        print(f"[INFO] reached total {url_num} items, exiting.")
        break
