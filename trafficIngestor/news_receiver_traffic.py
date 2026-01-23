#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
news_receiver_traffic.py

从数据库读取新闻 URL，使用容器池并发采集流量数据。
"""

import os
import sys
import configparser
from typing import List, Dict

from sqlalchemy import create_engine, text

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class NewsReceiverTrafficIngestor(BaseTrafficIngestor):
    """新闻流量采集器"""

    # ============== 配置 ==============
    CONTAINER_PREFIX = f"{get_real_username()}_news_traffic"
    START_IDX = 0
    END_IDX = 2
    HOST_CODE_PATH = os.path.join(_project_root, 'traffic_capture')
    BASE_DST = '/netdisk/news_receiver'
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    BATCH_SIZE = 10000

    # 需要处理的表及其对应的 domain
    TABLES_CONFIG = [
        {"table": "dailymail_content", "domain": "dailymail.co.uk"},
        # {"table": "bbc_content", "domain": "bbc.com"},
        # {"table": "nih_content", "domain": "nih.gov"},
        # {"table": "forbeschina_content", "domain": "forbeschina.com"},
    ]

    TABLE_MAP = {
        "bbc.com": "bbc_content",
        "nih.gov": "nih_content",
        "forbeschina.com": "forbeschina_content",
        "dailymail.co.uk": "dailymail_content",
    }

    DB_CONFIG_PATH = os.path.join(_project_root, 'db', 'db_config.ini')

    def __init__(self):
        super().__init__()
        self._db_engine = None

    def setup(self) -> None:
        """初始化数据库连接"""
        try:
            self._db_engine = self._connect_db()
            self.log("数据库连接成功")
        except Exception as e:
            self.log(f"FATAL: 数据库连接失败，无法继续: {e}")
            sys.exit(1)

    def _connect_db(self):
        """连接数据库并返回引擎"""
        cp = configparser.ConfigParser(interpolation=None)
        if not cp.read(self.DB_CONFIG_PATH, encoding="utf-8-sig"):
            raise FileNotFoundError(f"未找到配置文件：{self.DB_CONFIG_PATH}")
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
        return engine

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """从数据库获取任务"""
        all_jobs: List[Dict[str, str]] = []

        for table_config in self.TABLES_CONFIG:
            table = table_config["table"]
            domain = table_config["domain"]

            sql = f"""
                SELECT id, url
                FROM {table}
                WHERE (pcap_path IS NULL OR pcap_path = '')
                AND url IS NOT NULL AND url <> ''
                ORDER BY id
                LIMIT {self.BATCH_SIZE}
            """

            try:
                with self._db_engine.connect() as conn:
                    result = conn.execute(text(sql))
                    for row in result:
                        row_id = str(row[0])
                        url = row[1]
                        all_jobs.append({"row_id": row_id, "url": url, "domain": domain})

                if all_jobs:
                    self.log(f"从 {table} 获取了 {len(all_jobs)} 条任务")
            except Exception as e:
                self.log(f"WARN: 从数据库获取任务失败: {e}")

        return all_jobs

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功后更新数据库"""
        try:
            row_id = int(task.get("row_id", "0"))
            domain = task.get("domain", "")
            table = self.TABLE_MAP.get(domain, "")

            if not table:
                self.log(f"WARN: 不支持的 domain: {domain}，跳过数据库上传")
                return

            sql = f"""
                UPDATE {table}
                SET classify_status=%s,
                    traffic_status=%s,
                    pcap_path=%s,
                    ssl_key_path=%s,
                    content_path=%s,
                    html_path=%s,
                    traffic_feature=%s,
                    current_url=%s
                WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
            """.strip()

            data = (0, 0, paths['pcap'], paths['ssl_key'], paths['content'], paths['html'], None, paths.get('current_url', ''), row_id)

            conn = self._db_engine.raw_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, data)
                    affected = cur.rowcount
                conn.commit()
                if affected > 0:
                    self.log(f"数据库更新成功 row_id={row_id}")
                else:
                    self.log(f"数据库更新失败或无匹配行 row_id={row_id}")
            finally:
                conn.close()

        except Exception as e:
            self.log(f"WARN: 数据库操作异常 row_id={task.get('row_id', '')}: {e}")

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """任务失败后标记数据库"""
        try:
            row_id = int(task.get("row_id", "0"))
            domain = task.get("domain", "")
            table = self.TABLE_MAP.get(domain, "")

            if not table:
                return

            sql = f"""
                UPDATE {table}
                SET pcap_path=%s
                WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
            """.strip()

            conn = self._db_engine.raw_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, ('error', row_id))
                conn.commit()
                self.log(f"失败记录已标记到数据库 row_id={row_id}")
            finally:
                conn.close()

        except Exception as e:
            self.log(f"WARN: 数据库标记异常 row_id={task.get('row_id', '')}: {e}")


if __name__ == "__main__":
    NewsReceiverTrafficIngestor.main()
