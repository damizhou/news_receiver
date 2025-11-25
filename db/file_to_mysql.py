from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, Tuple, Optional, List, Set
import configparser
from sqlalchemy import create_engine, text

@dataclass(frozen=True)
class Record:
    classify_status: int
    traffic_status: int
    pcap_path: str
    ssl_key_path: Optional[str]
    content_path: Optional[str]
    html_path: Optional[str]
    traffic_feature: Optional[str] = None

def list_pcaps(dir_path: str | Path) -> list[Path]:
    """返回 dir_path 当前层级的 pcap 文件列表（不进入子目录）。"""
    p = Path(dir_path).expanduser()
    if not p.is_dir():
        raise NotADirectoryError(f"不是目录: {p}")
    wanted = '.pcap'
    files = [f.resolve() for f in p.iterdir() if f.is_file() and f.suffix.lower() == wanted]
    return sorted(files)

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


def file_path_to_mysql(engine, domain: str):
    table = ''
    if domain == "bbc.com":
        table = 'bbc_content'
    elif domain == "nih.gov":
        table = 'nih_content'
    elif domain == "forbeschina.com":
        table = 'forbeschina_content'
    elif domain == "dailymail.co.uk":
        table = 'dailymail_content'
    if not table:
        raise ValueError(f"unsupported domain: {domain}")

    pcap_dir_path = '/netdisk/news_receiver' + f'/{domain}/pcap/'
    print(pcap_dir_path)
    pcap_paths = list_pcaps(pcap_dir_path)

    batch: List[Record] = []
    for pcap_path in pcap_paths:
        ssl_key_path = str(pcap_path).replace('/pcap/', '/ssl_key/').replace('.pcap', '_ssl_key.log')
        content_path = str(pcap_path).replace('/pcap/', '/content/').replace('.pcap', '.text')
        html_path = str(pcap_path).replace('/pcap/', '/html/').replace('.pcap', '.html')

        rec = Record(
            classify_status=0,
            traffic_status=0,
            pcap_path=str(pcap_path),
            ssl_key_path=ssl_key_path,
            content_path=content_path,
            html_path=html_path,
            traffic_feature=None,
        )
        batch.append(rec)

    # ✅ 循环外统一批量更新
    n = update_batch(engine, table, batch)  # 已在内部提交
    print(f"[INFO] {table} 按 id 更新 {n} 条")

def update_batch(engine, table_name, rows: List[Record]) -> int:
    """
    按 row_id（来自文件名开头的数字）更新已存在记录：
    UPDATE {table_name} SET ... WHERE id = :row_id
    找不到该 id 的行会被自动跳过（受影响行数为 0）。
    """
    if not rows:
        return 0

    sql = f"""
        UPDATE {table_name}
        SET classify_status=%s,
            traffic_status=%s,
            pcap_path=%s,
            ssl_key_path=%s,
            content_path=%s,
            html_path=%s,
            traffic_feature=%s
        WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
    """.strip()

    data = []
    for r in rows:
        # 约定：pcap 文件名形如 "12345_xxx.pcap"，row_id=12345
        try:
            row_id = int(Path(r.pcap_path).stem.split("_", 1)[0])
        except Exception:
            # 文件名不符合约定则跳过该行
            continue

        data.append((
            r.classify_status,
            r.traffic_status,
            r.pcap_path,
            r.ssl_key_path,
            r.content_path,
            r.html_path,
            r.traffic_feature,
            row_id,            # WHERE id = ?
        ))

    if not data:
        return 0

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
            affected = cur.rowcount  # 实际命中的 UPDATE 行数
        conn.commit()
        return affected
    finally:
        conn.close()

if __name__ == "__main__":
    # domains = ["bbc.com", "dailymail.co.uk", "forbeschina.com", "nih.gov"]
    domains = ["dailymail.co.uk"]
    engine, msg = connect_db()
    print(msg)
    for domain in domains:
        file_path_to_mysql(engine, domain)

