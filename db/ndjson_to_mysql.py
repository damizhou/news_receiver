#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仅写入版（硬编码 NDJSON 路径）：
- 不创建/修改任何表或索引
- 显式将 update_time / classify_time / traffic_time 写入 NULL
- --insert-mode upsert 时，不把已有行的三列时间覆盖为 NULL（COALESCE）
- 数据库连接从 db_config.ini 读取
"""
import configparser
import json
import csv
from sqlalchemy import text as _sql_text
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy import text, bindparam
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def parse_ts(v, naive: bool = True):
    """
    将输入时间解析并转换为北京时间 (Asia/Shanghai)。
    支持 int/float（秒级时间戳）与 ISO8601 字符串（含 Z 或偏移）。
    若字符串无时区信息，按 UTC 解释再转北京时区。
    :param naive: True 返回无 tzinfo 的“墙上时间”；False 返回 tz-aware。
    """
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v, tz=timezone.utc)             # epoch -> UTC
        else:
            s = str(v).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"                                   # Z -> +00:00
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:                                       # 无时区 -> 当作 UTC
                dt = dt.replace(tzinfo=timezone.utc)
        dt_cn = dt.astimezone(ZoneInfo("Asia/Shanghai"))                # 转北京时区
        return dt_cn.replace(tzinfo=None) if naive else dt_cn
    except Exception:
        return None

def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i+n]

def connect_db():
    cp = configparser.ConfigParser(interpolation=None)
    if not cp.read("db_config.ini", encoding="utf-8-sig"):
        raise FileNotFoundError("未找到配置文件：db_config.ini")
    if not cp.has_section("mysql"):
        raise KeyError("缺少配置节 [mysql]")

    c = cp["mysql"]

    def need(k):
        v = c.get(k, "").strip()
        if not v:
            raise ValueError(f"缺少 {k}")
        return v

    user = need("user")
    pwd = need("password")
    host = need("host")
    port = need("port")
    db = need("database")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"

    cs = c.get("charset", "").strip()
    if cs:
        url += f"?charset={cs}"

    engine = create_engine(url, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine, f"连接成功：{engine.url.render_as_string(hide_password=True)}"

def insert_bbc_ndjson(engine):
    """
        将 items（dict 列表）写入表 bbc_content，按 link/url 去重。
        仅写入列：title, summary, url, published, updated。
        - items 中 url 取优先级：item["link"] or item["url"]
        - 批量去重策略：同一批先查已存在 url，再批量插入剩余。
        - 时间字段支持 ISO8601（含 Z），转换为 UTC 无时区 datetime（MySQL TIMESTAMP 接受）。

        :param items: 形如 [{'title':..., 'summary':..., 'link':..., 'published':..., 'updated':...}, ...]
        :param engine: SQLAlchemy Engine（已连到 MySQL）
        :return: 实际插入的行数
        """
    items = []
    with open("bbc_en_all.ndjson", "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            try:
                data = json.loads(line.strip())
                items.append(data)
            except Exception:
                continue
    # 预处理 + 批内去重（同一 url 只保留第一条）
    normalized, seen = [], set()
    for it in items or []:
        try:
            url = it.get("link").strip()
        except Exception:
            continue
        if not url or url in seen:  # 无链接或批内重复跳过
            continue
        seen.add(url)
        normalized.append({"title": it.get("title") , "summary": it.get("summary"), "url": url,
            "published": parse_ts(it.get("published")), "updated": f"{datetime.now()}" })
    if not normalized:
        return 0

    sel = text("SELECT url FROM bbc_content WHERE url IN :urls").bindparams(bindparam("urls", expanding=True))
    ins = text("""
            INSERT INTO bbc_content (title, summary, url, published, updated)
            VALUES (:title, :summary, :url, :published, :updated)
        """)

    inserted = 0
    with engine.begin() as conn:
        for batch in chunks(normalized, 100):
            # 批量查询已有 url
            exist = set(r[0] for r in conn.execute(sel, {"urls": [row["url"] for row in batch]}))
            to_insert = [row for row in batch if row["url"] not in exist]
            if to_insert:
                conn.execute(ins, to_insert)  # executemany
                print(f'插入了{to_insert}条数据')
                inserted += len(to_insert)
    return inserted

def insert_nih_ndjson(engine):
    """
        将 items（dict 列表）写入表 bbc_content，按 link/url 去重。
        仅写入列：title, summary, url, published, updated。
        - items 中 url 取优先级：item["link"] or item["url"]
        - 批量去重策略：同一批先查已存在 url，再批量插入剩余。
        - 时间字段支持 ISO8601（含 Z），转换为 UTC 无时区 datetime（MySQL TIMESTAMP 接受）。

        :param items: 形如 [{'title':..., 'summary':..., 'link':..., 'published':..., 'updated':...}, ...]
        :param engine: SQLAlchemy Engine（已连到 MySQL）
        :return: 实际插入的行数
        """
    items = []
    with open("../nih/nih.ndjson", "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            try:
                data = json.loads(line.strip())
                items.append(data)
            except Exception:
                continue
    # 预处理 + 批内去重（同一 url 只保留第一条）
    normalized, seen = [], set()
    for it in items or []:
        url = it.get("url") .strip()
        if not url or url in seen:  # 无链接或批内重复跳过
            continue
        seen.add(url)
        normalized.append({"title": it.get("title"), "summary": it.get("summary"), "url": url,
            "published": parse_ts(it.get("date_iso")), "updated": f"{datetime.now()}" })
    if not normalized:
        return 0

    sel = text("SELECT url FROM nih_content WHERE url IN :urls").bindparams(bindparam("urls", expanding=True))
    ins = text("""
            INSERT INTO nih_content (title, summary, url, published, updated)
            VALUES (:title, :summary, :url, :published, :updated)
        """)

    inserted = 0
    with engine.begin() as conn:
        for batch in chunks(normalized, 100):
            # 批量查询已有 url
            exist = set(r[0] for r in conn.execute(sel, {"urls": [row["url"] for row in batch]}))
            to_insert = [row for row in batch if row["url"] not in exist]
            if to_insert:
                conn.execute(ins, to_insert)  # executemany
                inserted += len(to_insert)
    return inserted

def insert_forbes_ndjson(engine):
    """
        将 items（dict 列表）写入表 bbc_content，按 link/url 去重。
        仅写入列：title, summary, url, published, updated。
        - items 中 url 取优先级：item["link"] or item["url"]
        - 批量去重策略：同一批先查已存在 url，再批量插入剩余。
        - 时间字段支持 ISO8601（含 Z），转换为 UTC 无时区 datetime（MySQL TIMESTAMP 接受）。

        :param items: 形如 [{'title':..., 'summary':..., 'link':..., 'published':..., 'updated':...}, ...]
        :param engine: SQLAlchemy Engine（已连到 MySQL）
        :return: 实际插入的行数
        """
    items = []
    with open("../forbeschina/forbeschina.ndjson", "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            try:
                data = json.loads(line.strip())
                items.append(data)
            except Exception:
                continue
    # 预处理 + 批内去重（同一 url 只保留第一条）
    normalized, seen = [], set()
    for it in items or []:
        url = it.get("url") .strip()
        if not url or url in seen:  # 无链接或批内重复跳过
            continue
        seen.add(url)
        normalized.append({"title": it.get("title"), "summary": it.get("desc"), "url": url,
            "published": parse_ts(it.get("date_iso")), "updated": f"{datetime.now()}" })
    if not normalized:
        return 0

    sel = text("SELECT url FROM forbeschina_content WHERE url IN :urls").bindparams(bindparam("urls", expanding=True))
    ins = text("""
            INSERT INTO forbeschina_content (title, summary, url, published, updated)
            VALUES (:title, :summary, :url, :published, :updated)
        """)

    inserted = 0
    with engine.begin() as conn:
        for batch in chunks(normalized, 100):
            # 批量查询已有 url
            exist = set(r[0] for r in conn.execute(sel, {"urls": [row["url"] for row in batch]}))
            to_insert = [row for row in batch if row["url"] not in exist]
            if to_insert:
                conn.execute(ins, to_insert)  # executemany
                inserted += len(to_insert)
    return inserted

def insert_dailymail_ndjson(engine):
    """
        将 items（dict 列表）写入表 bbc_content，按 link/url 去重。
        仅写入列：title, summary, url, published, updated。
        - items 中 url 取优先级：item["link"] or item["url"]
        - 批量去重策略：同一批先查已存在 url，再批量插入剩余。
        - 时间字段支持 ISO8601（含 Z），转换为 UTC 无时区 datetime（MySQL TIMESTAMP 接受）。

        :param items: 形如 [{'title':..., 'summary':..., 'link':..., 'published':..., 'updated':...}, ...]
        :param engine: SQLAlchemy Engine（已连到 MySQL）
        :return: 实际插入的行数
        """
    items = []
    with open("../dailymail/dailymail_latest_1000.ndjson", "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            try:
                data = json.loads(line.strip())
                items.append(data)
            except Exception:
                continue
    # 预处理 + 批内去重（同一 url 只保留第一条）
    normalized, seen = [], set()
    for it in items or []:
        url = it.get("url") .strip()
        if not url or url in seen:  # 无链接或批内重复跳过
            continue
        seen.add(url)
        normalized.append({"title": it.get("title"), "summary": "", "url": url,
            "published": parse_ts(it.get("date")), "updated": f"{datetime.now()}" })
    if not normalized:
        return 0

    sel = text("SELECT url FROM dailymail_content WHERE url IN :urls").bindparams(bindparam("urls", expanding=True))
    ins = text("""
            INSERT INTO dailymail_content (title, summary, url, published, updated)
            VALUES (:title, :summary, :url, :published, :updated)
        """)

    inserted = 0
    with engine.begin() as conn:
        for batch in chunks(normalized, 100):
            # 批量查询已有 url
            exist = set(r[0] for r in conn.execute(sel, {"urls": [row["url"] for row in batch]}))
            to_insert = [row for row in batch if row["url"] not in exist]
            if to_insert:
                conn.execute(ins, to_insert)  # executemany
                print(f'插入了{to_insert}条数据')
                inserted += len(to_insert)
    return inserted

def insert_reuters_ndjson(engine):
    """
        将 items（dict 列表）写入表 bbc_content，按 link/url 去重。
        仅写入列：title, summary, url, published, updated。
        - items 中 url 取优先级：item["link"] or item["url"]
        - 批量去重策略：同一批先查已存在 url，再批量插入剩余。
        - 时间字段支持 ISO8601（含 Z），转换为 UTC 无时区 datetime（MySQL TIMESTAMP 接受）。

        :param items: 形如 [{'title':..., 'summary':..., 'link':..., 'published':..., 'updated':...}, ...]
        :param engine: SQLAlchemy Engine（已连到 MySQL）
        :return: 实际插入的行数
        """
    items = []
    with open("../reuters/reuters_latest_20251017_163850.ndjson", "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            try:
                data = json.loads(line.strip())
                items.append(data)
            except Exception:
                continue
    # 预处理 + 批内去重（同一 url 只保留第一条）
    normalized, seen = [], set()
    for it in items or []:
        url = it.get("url") .strip()
        if not url or url in seen:  # 无链接或批内重复跳过
            continue
        seen.add(url)
        normalized.append({"title": "", "summary": "", "url": url,
            "published": parse_ts(it.get("lastmod")), "updated": f"{datetime.now()}" })
    if not normalized:
        return 0

    sel = text("SELECT url FROM reuters_content WHERE url IN :urls").bindparams(bindparam("urls", expanding=True))
    ins = text("""
            INSERT INTO reuters_content (title, summary, url, published, updated)
            VALUES (:title, :summary, :url, :published, :updated)
        """)

    inserted = 0
    with engine.begin() as conn:
        for batch in chunks(normalized, 100):
            # 批量查询已有 url
            exist = set(r[0] for r in conn.execute(sel, {"urls": [row["url"] for row in batch]}))
            to_insert = [row for row in batch if row["url"] not in exist]
            if to_insert:
                conn.execute(ins, to_insert)  # executemany
                inserted += len(to_insert)
    return inserted

def export_missing_pcap_csv(engine, table: str,out_csv: str | None = None,) -> int:
    """
    从给定表中导出 pcap_path 为空的记录到 CSV。

    逻辑：
      1) 统计已有 pcap_path 的记录数 a（且 url 不为空）
      2) 计算 b = 10000 - a
      3) 只导出 b 条缺失 pcap 的记录到 CSV（b <= 0 时不导出）
    """
    print("开始查找未处理的数据。")
    total = 10000

    p = Path(out_csv)
    need_header = (not p.exists()) or (p.stat().st_size in (0, 3))

    exported = 0

    with engine.connect() as conn:
        # 1) 统计已有 pcap_path 的记录数 a
        # count_sql = f"""
        #     SELECT COUNT(*)
        #     FROM {table}
        #     WHERE pcap_path IS NOT NULL AND pcap_path <> ''
        #       AND url IS NOT NULL AND url <> ''
        # """
        # current_count = conn.execute(_sql_text(count_sql)).scalar() or 0
        # current_count = int(current_count)
        #
        # # 2) 计算需要导出的缺失数量 b
        # missing_count = total - current_count
        # if missing_count <= 0:
        #     print(
        #         f"[skip] {table}: 已有 {current_count} 条 pcap 记录，已达到或超过 {total} 条，总量，不再导出缺失任务。"
        #     )
        #     return 0

        # 3) 只取 b 条 pcap_path 为空的记录
        sql = f"""
            SELECT id, url
            FROM {table}
            WHERE (pcap_path IS NULL OR pcap_path = '')
              AND url IS NOT NULL AND url <> ''
            ORDER BY id 
        """

        with open(out_csv, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "url", "domain"])
            if need_header:
                writer.writeheader()

            # 统一确定域名（未知表就留空字符串）
            domain = ""
            if table == "bbc_content":
                domain = "bbc.com"
            elif table == "nih_content":
                domain = "nih.gov"
            elif table == "forbeschina_content":
                domain = "forbeschina.com"
            elif table == "dailymail_content":
                domain = "dailymail.co.uk"
            elif table == "wikicontent":
                domain = "zh.wikipedia.org"
            elif table == "theguardian_content":
                domain = "theguardian.com"

            result = conn.execute(_sql_text(sql))
            for row in result:
                _id = row[0]
                if table == "wikicontent":
                    _url = "https://zh.wikipedia.org/wiki/" + row[1]
                else:
                    _url = row[1]
                writer.writerow({"id": _id, "url": _url, "domain": domain})
                exported += 1

    print(f"[write] {exported} rows -> {out_csv}")
    return exported

def main():
    engine, msg = connect_db()
    for i in range(1):
        # # tables = ["dailymail_content", "bbc_content", "nih_content", "forbeschina_content"]
        tables = ["bbc_content"]
        # # tables = ["nih_content", "forbeschina_content", , "theguardian_content", "wikicontent"]
        # # tables = ["wikicontent"]
        for table in tables:
            export_missing_pcap_csv(engine, table=table, out_csv=f"missing_pcap.csv")
    # insert_dailymail_ndjson(engine)
    # insert_count = insert_bbc_ndjson(engine)
    # insert_count = insert_forbes_ndjson(engine)
    # insert_count = insert_nih_ndjson(engine)
    # print(f"Inserted {insert_count} rows into table")
if __name__ == "__main__":
    main()
