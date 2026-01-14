#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_news_receiver_pool.py

- 从 csv 读取记录（id,url,domain）
- 每行转 JSON：{"row_id": id, "url": url, "domain": domain}
- 使用容器池 news_receiver0..78 并发执行：
    docker exec <name> python -u /app/action.py '<JSON>'
- 创建容器时：--init 防僵尸进程，并挂载 HOST_CODE_PATH:/app
- 每个容器启动后执行一次：关闭包合并（tso/gso/gro off）

长时间执行：
- 启动仅准备容器池一次
- 死循环：读取任务 -> 调度执行 -> 汇总 -> 清空CSV(保留表头)
- 若无任务，等待 10 分钟再来一轮
"""

from __future__ import annotations
import csv
import os
import sys
import time
import json
import signal
import queue
import subprocess
import configparser
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor
import shutil
import threading
from sqlalchemy import create_engine, text

# ============== 配置 ==============
CONTAINER_PREFIX = "news_traffic"
START_IDX = 0
END_IDX = 21 * 12 - 1                    # 0..78 共 79 个容器（若只需 76 个，把 END_IDX 改为 75）
DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
# DOCKER_IMAGE = "chuanzhoupan/trace_spider_firefox:251104"
CONTAINER_CODE_PATH = "/app"
HOST_CODE_PATH = '/home/pcz/code/news_receiver/traffice_capture'  # ★ 按你要求固定
DASE_DST = '/netdisk/news_receiver'
# =================================
CREATE_WITH_TTY = True            # 创建容器时加 -itd
DOCKER_EXEC_TIMEOUT = 6000        # 单次 docker exec 超时
RETRY = 5                         # 失败重试次数（不含首次）
NO_TASK_SLEEP_SECONDS = 600       # 无任务时等待 10 分钟
# =================================
EXEC_INTERVAL = 1.0  # 两次 docker exec 之间至少间隔多少秒，可自己调
DB_CONFIG_PATH = "/home/pcz/code/news_receiver/db/db_config.ini"
BATCH_SIZE = 10000  # 每次从数据库获取的任务数量
# 需要处理的表及其对应的 domain
TABLES_CONFIG = [
    {"table": "dailymail_content", "domain": "dailymail.co.uk"},
    # {"table": "bbc_content", "domain": "bbc.com"},
    # {"table": "nih_content", "domain": "nih.gov"},
    # {"table": "forbeschina_content", "domain": "forbeschina.com"},
]

_last_exec_ts = 0.0
_last_exec_lock = threading.Lock()
_stats_lock = threading.Lock()
_db_engine = None  # 全局数据库引擎

def connect_db():
    """连接数据库并返回引擎"""
    cp = configparser.ConfigParser(interpolation=None)
    if not cp.read(DB_CONFIG_PATH, encoding="utf-8-sig"):
        raise FileNotFoundError(f"未找到配置文件：{DB_CONFIG_PATH}")
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

def get_table_name(domain: str) -> str:
    """根据 domain 返回对应的表名"""
    table_map = {
        "bbc.com": "bbc_content",
        "nih.gov": "nih_content",
        "forbeschina.com": "forbeschina_content",
        "dailymail.co.uk": "dailymail_content",
    }
    return table_map.get(domain, "")

def fetch_jobs_from_db(engine, table: str, domain: str, limit: int = BATCH_SIZE) -> List[Dict[str, str]]:
    """
    从数据库获取需要处理的任务（pcap_path 为空的记录）
    :param engine: 数据库引擎
    :param table: 表名
    :param domain: 域名
    :param limit: 每次获取的数量
    :return: 任务列表
    """
    sql = f"""
        SELECT id, url
        FROM {table}
        WHERE (pcap_path IS NULL OR pcap_path = '')
        AND url IS NOT NULL AND url <> ''
        ORDER BY id
        LIMIT {limit}
    """
    
    jobs: List[Dict[str, str]] = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            for row in result:
                row_id = str(row[0])
                url = row[1]
                if table == "wikicontent":
                    url = "https://zh.wikipedia.org/wiki/" + url
                jobs.append({"row_id": row_id, "url": url, "domain": domain})
    except Exception as e:
        log(f"WARN: 从数据库获取任务失败: {e}")
    
    return jobs

def upload_single_record_to_db(engine, domain: str, row_id: int, pcap_path: str,
                                ssl_key_path: str, content_path: str, html_path: str) -> bool:
    """上传单条记录到数据库"""
    table = get_table_name(domain)
    if not table:
        log(f"WARN: 不支持的 domain: {domain}，跳过数据库上传")
        return False

    sql = f"""
        UPDATE {table}
        SET classify_status=%s,
            traffic_status=%s,
            pcap_path=%s,
            ssl_key_path=%s,
            content_path=%s,
            html_path=%s,
            traffic_feature=%s
        WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
    """.strip()

    data = (0, 0, pcap_path, ssl_key_path, content_path, html_path, None, row_id)

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, data)
            affected = cur.rowcount
        conn.commit()
        return affected > 0
    except Exception as e:
        log(f"WARN: 数据库更新失败 row_id={row_id}: {e}")
        return False
    finally:
        conn.close()

def mark_failed_record_to_db(engine, domain: str, row_id: int) -> bool:
    """标记失败记录到数据库，只更新 pcap_path='error'"""
    table = get_table_name(domain)
    if not table:
        log(f"WARN: 不支持的 domain: {domain}，跳过数据库标记")
        return False

    sql = f"""
        UPDATE {table}
        SET pcap_path=%s
        WHERE id=%s AND (pcap_path IS NULL OR pcap_path = '')
    """.strip()

    data = ('error', row_id)

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, data)
            affected = cur.rowcount
        conn.commit()
        return affected > 0
    except Exception as e:
        log(f"WARN: 数据库标记失败 row_id={row_id}: {e}")
        return False
    finally:
        conn.close()


def clear_host_code_subdirs(base: str | Path = HOST_CODE_PATH) -> None:
    """
    只删除 HOST_CODE_PATH 下的所有子文件夹，但保留 HOST_CODE_PATH 下的文件。

    示例：
        clear_host_code_subdirs()  # 默认清理 HOST_CODE_PATH
    """
    base_path = Path(base)
    if not base_path.exists() or not base_path.is_dir():
        log(f"WARN: HOST_CODE_PATH 不存在或不是目录：{base_path}")
        return

    for entry in base_path.iterdir():
        # 只处理子目录，不处理文件
        if entry.is_dir():
            try:
                shutil.rmtree(entry)
                log(f"删除子目录: {entry}")
            except Exception as e:
                log(f"WARN: 删除子目录失败: {entry} -> {e}")

def _wait_before_exec():
    """
    全局节流：保证所有线程之间，每次 docker exec 至少间隔 EXEC_INTERVAL 秒。
    """
    global _last_exec_ts
    while True:
        with _last_exec_lock:
            now = time.monotonic()
            delta = _last_exec_ts + EXEC_INTERVAL - now
            if delta <= 0:
                # 轮到我执行了，记录时间点后返回
                _last_exec_ts = now
                return
        # 还没轮到我，先睡一会儿再抢
        if delta > 0:
            time.sleep(min(delta, 0.5))
def log(*a):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]", *a, flush=True)

def run(cmd: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)

def ensure_docker_available():
    try:
        run(["docker", "version"]).check_returncode()
    except Exception as e:
        log("FATAL: docker 不可用。", e)
        sys.exit(2)

def container_exists(name: str) -> Optional[bool]:
    cp = run(["docker", "inspect", "-f", "{{.State.Running}}", name])
    if cp.returncode != 0:
        return None
    out = cp.stdout.strip().lower()
    return (out == "true") or (out == "false")

def container_running(name: str) -> bool:
    cp = run(["docker", "inspect", "-f", "{{.State.Running}}", name])
    return (cp.returncode == 0) and (cp.stdout.strip().lower() == "true")

def create_container(name: str, host_code_path: str, image: str):
    uid, gid = str(os.getuid()), str(os.getgid())
    cmd = [
        "docker", "run",
        "--init",
        "--volume", f"{host_code_path}:{CONTAINER_CODE_PATH}",
        "-e", f"HOST_UID={uid}",
        "-e", f"HOST_GID={gid}",
        "--privileged",
    ]
    if CREATE_WITH_TTY:
        cmd += ["-itd"]
    else:
        cmd += ["-d"]
    cmd += ["--name", name, image, "/bin/bash"]
    cp = run(cmd)
    if cp.returncode != 0:
        log(f"FATAL: 创建容器失败: {name} -> {cp.stderr.strip()}")
        sys.exit(2)
    log(f"created container: {name}")

def start_container(name: str):
    cp = run(["docker", "start", name])
    if cp.returncode != 0:
        log(f"FATAL: 启动容器失败: {name} -> {cp.stderr.strip()}")
        sys.exit(2)
    log(f"started container: {name}")

def disable_offload_once(name: str):
    """
    在容器里仅执行一次：关闭包合并（TSO/GSO/GRO）
    使用标记文件 /tmp/.offload_disabled 防重复执行。
    有 sudo 则 sudo，没有就直接 ethtool。
    """
    shell = r'''
        if [ -f /tmp/.offload_disabled ]; then
            exit 0
        fi
        if command -v sudo >/dev/null 2>&1; then
            sudo ethtool -K eth0 tso off gso off gro off
        else
            ethtool -K eth0 tso off gso off gro off
        fi
        rc=$?
        if [ $rc -eq 0 ]; then
            touch /tmp/.offload_disabled
        fi
        exit $rc
    '''
    cp = run(["docker", "exec", name, "sh", "-lc", shell])
    if cp.returncode == 0:
        log(f"{name}: offload disabled (TSO/GSO/GRO off)")
    else:
        msg = (cp.stderr or cp.stdout).strip()
        log(f"WARN: {name}: 关闭包合并失败：{msg if msg else 'unknown error'}")


def ensure_container_ready(name: str, host_code_path: str, image: str) -> bool:
    """
    确保容器存在并处于运行状态。
    返回值: 是否为“本次新创建”的容器（True/False）。
    """
    exists = container_exists(name)
    if exists is None:
        create_container(name, host_code_path, image)   # --init -itd 已经运行中
        return True
    if not container_running(name):
        start_container(name)                            # 仅启动，不做 offload
    return False


def build_container_names(prefix: str, start_idx: int, end_idx: int) -> List[str]:
    return [f"{prefix}{i}" for i in range(start_idx, end_idx + 1)]




def chown_recursive(path: str, uid: int = 1002, gid: int = 1002) -> None:
    """把 path（文件或目录）及其子项（若为目录）设为 uid:gid。尽量不抛异常。"""
    import os
    try:
        os.chown(path, uid, gid, follow_symlinks=False)
    except Exception:
        pass
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path, followlinks=False):
            for name in dirs:
                p = os.path.join(root, name)
                try: os.chown(p, uid, gid, follow_symlinks=False)
                except Exception: pass
            for name in files:
                p = os.path.join(root, name)
                try: os.chown(p, uid, gid, follow_symlinks=False)
                except Exception: pass

def exec_once(task: Dict[str, str]) -> Tuple[bool, str]:
    _wait_before_exec()
    payload = json.dumps(task, ensure_ascii=False)
    container = task["container"]
    cmd = [
        "docker", "exec", container,
        "python", "-u", f"{CONTAINER_CODE_PATH}/action.py",
        payload
    ]
    print("执行命令", cmd)
    cp = run(cmd, timeout=DOCKER_EXEC_TIMEOUT)
    if cp.returncode == 0:
        try:
            with open(HOST_CODE_PATH + f"/meta/{container}_last.json", "r", encoding="utf-8") as f:
                result = json.load(f)
            print('result', result)
            pcap_path = result.get("pcap_path")
            ssl_key_file_path = result.get("ssl_key_file_path")
            content_path = result.get("content_path")
            html_path = result.get("html_path")
            screenshot_path = result.get("screenshot_path")

            if not all([pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path]):
                return False, "result JSON missing required paths"

            pcap_path = pcap_path.replace("/app", HOST_CODE_PATH)
            ssl_key_file_path = ssl_key_file_path.replace("/app", HOST_CODE_PATH)
            content_path = content_path.replace("/app", HOST_CODE_PATH)
            html_path = html_path.replace("/app", HOST_CODE_PATH)
            screenshot_path = screenshot_path.replace("/app", HOST_CODE_PATH)
            print('screenshot_path', screenshot_path)
            dst = os.path.join(DASE_DST, task['domain'])
            pcap_dst = os.path.join(dst, 'pcap')
            if not os.path.exists(pcap_dst):
                os.makedirs(pcap_dst)
            ssl_key_dst = os.path.join(dst, 'ssl_key')
            if not os.path.exists(ssl_key_dst):
                os.makedirs(ssl_key_dst)
            content_dst = os.path.join(dst, 'content')
            if not os.path.exists(content_dst):
                os.makedirs(content_dst)
            html_dst = os.path.join(dst, 'html')
            if not os.path.exists(html_dst):
                os.makedirs(html_dst)
            screenshot_dst = os.path.join(dst, 'screenshot')
            if not os.path.exists(screenshot_dst):
                os.makedirs(screenshot_dst)

            new_pcap = shutil.move(pcap_path, pcap_dst)
            chown_recursive(new_pcap, uid=1002, gid=1002)

            new_ssl = shutil.move(ssl_key_file_path, ssl_key_dst)
            chown_recursive(new_ssl, uid=1002, gid=1002)

            new_content = shutil.move(content_path, content_dst)
            chown_recursive(new_content, uid=1002, gid=1002)

            new_html = shutil.move(html_path, html_dst)
            chown_recursive(new_html, uid=1002, gid=1002)

            new_screenshot = shutil.move(screenshot_path, screenshot_dst)
            chown_recursive(new_screenshot, uid=1002, gid=1002)

            # 上传到数据库
            try:
                row_id_int = int(task.get("row_id", "0"))
                domain = task.get("domain", "")
                if _db_engine and domain:
                    db_ok = upload_single_record_to_db(
                        _db_engine, domain, row_id_int,
                        new_pcap, new_ssl, new_content, new_html
                    )
                    if db_ok:
                        log(f"数据库更新成功 row_id={row_id_int}")
                    else:
                        log(f"数据库更新失败或无匹配行 row_id={row_id_int}")
            except Exception as e:
                log(f"WARN: 数据库操作异常 row_id={task.get('row_id','')}: {e}")

            return True, ""
        except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
            return False, f"post-processing error: {e}"
    return False, (cp.stderr.strip() or cp.stdout.strip())


def worker_loop(container: str, q: "queue.Queue[Dict[str, str]]", stats: dict, retry: int):
    """
    带重试的 worker：每个任务最多尝试 retry+1 次（首次 + retry 次重试）
    """
    while True:
        try:
            task = q.get_nowait()
        except queue.Empty:
            return
        row_id = task.get("row_id", "")
        url    = task.get("url", "")
        task["container"] = container

        ok = False
        err = ""
        for attempt in range(retry + 1):  # 首次 + retry 次重试
            try:
                if attempt == 0:
                    log(f"{container} -> start [{row_id}] {url}")
                else:
                    log(f"{container} -> retry {attempt}/{retry} [{row_id}] {url}")

                ok, err = exec_once(task)
                if ok:
                    log(f"{container} -> done  [{row_id}] {url}")
                    with _stats_lock:
                        stats["ok"] += 1
                    break  # 成功，跳出重试循环
                else:
                    log(f"{container} -> fail  [{row_id}] {err[:200]}")
                    if attempt < retry:
                        time.sleep(5)  # 重试前等待 5 秒

            except subprocess.TimeoutExpired:
                err = f"timeout>{DOCKER_EXEC_TIMEOUT}s"
                log(f"{container} -> timeout [{row_id}] {url}")
                if attempt < retry:
                    time.sleep(5)

            except Exception as e:
                err = repr(e)
                log(f"{container} -> error [{row_id}] {err}")
                if attempt < retry:
                    time.sleep(5)

        # 所有重试都失败
        if not ok:
            log(f"{container} -> give up [{row_id}] {url} after {retry + 1} attempts")
            # 失败时也写数据库，标记 pcap_path='error'
            try:
                row_id_int = int(task.get("row_id", "0"))
                domain = task.get("domain", "")
                if _db_engine and domain:
                    db_ok = mark_failed_record_to_db(_db_engine, domain, row_id_int)
                    if db_ok:
                        log(f"失败记录已标记到数据库 row_id={row_id_int}")
                    else:
                        log(f"数据库标记失败或无匹配行 row_id={row_id_int}")
            except Exception as e:
                log(f"WARN: 数据库标记异常 row_id={task.get('row_id','')}: {e}")
            with _stats_lock:
                stats["fail"] += 1
                stats["errors"].append((task, err))

        q.task_done()

def prepare_pool_once() -> List[str]:
    ensure_docker_available()

    host_code = Path(HOST_CODE_PATH)
    if not host_code.exists():
        log(f"WARN: 宿主机代码目录不存在：{host_code}，仍会尝试挂载。")
    if not host_code.is_absolute():
        log(f"WARN: 建议使用绝对路径挂载，当前={host_code}")

    names = build_container_names(CONTAINER_PREFIX, START_IDX, END_IDX)
    log(f"容器池规模={len(names)}: {names[0]} … {names[-1]}")

    created: List[str] = []

    # Pass 1：缺就建（记录本轮新建的容器名）
    for n in names:
        exists = container_exists(n)
        if exists is None:
            create_container(n, str(host_code), DOCKER_IMAGE)
            created.append(n)

    # Pass 2：不在运行的统一 start（包含老容器；新建容器通常已在运行，冪等调用无害）
    for n in names:
        if not container_running(n):
            start_container(n)

    # Pass 3：所有 docker run 完成后，按顺序对“本次新建”的容器执行一次 offload 关闭
    for n in created:
        disable_offload_once(n)

    return names

# ——收到中断后“立刻”退出（不等线程/子进程收尾，不跑 finally）——
def sig(signum, _frame):
    log(f"收到中断信号({signum})，立即退出。")
    try:
        sys.stdout.flush(); sys.stderr.flush()
    finally:
        os._exit(128 + signum)  # 130=SIGINT, 143=SIGTERM

def main():
    global _db_engine
    signal.signal(signal.SIGINT, sig)
    signal.signal(signal.SIGTERM, sig)

    # 初始化数据库连接
    try:
        _db_engine = connect_db()
        log("数据库连接成功")
    except Exception as e:
        log(f"FATAL: 数据库连接失败，无法继续: {e}")
        return

    names = prepare_pool_once()
    total_ok = 0
    total_fail = 0
    batch_num = 0

    try:
        while True:
            # 从所有表中获取任务
            all_jobs: List[Dict[str, str]] = []
            for table_config in TABLES_CONFIG:
                table = table_config["table"]
                domain = table_config["domain"]
                jobs = fetch_jobs_from_db(_db_engine, table, domain, BATCH_SIZE)
                if jobs:
                    log(f"从 {table} 获取了 {len(jobs)} 条任务")
                    all_jobs.extend(jobs)
            
            if not all_jobs:
                log("所有表都没有需要处理的任务，退出。")
                break
            
            batch_num += 1
            log(f"===== 批次 {batch_num}: 共 {len(all_jobs)} 条任务 =====")

            # 调度执行
            q: "queue.Queue[Dict[str, str]]" = queue.Queue()
            for t in all_jobs:
                q.put(t)

            stats = {"ok": 0, "fail": 0, "errors": []}  # type: ignore[dict-item]
            log(f"开始执行：jobs={len(all_jobs)}，并发容器={len(names)}，镜像={DOCKER_IMAGE}")
            with ThreadPoolExecutor(max_workers=len(names)) as pool:
                for n in names:
                    pool.submit(worker_loop, n, q, stats, RETRY)
                q.join()

            # 汇总本批次
            total_ok += stats['ok']
            total_fail += stats['fail']
            log(f"[批次 {batch_num} 汇总] success={stats['ok']} fail={stats['fail']} batch_total={len(all_jobs)}")
            if stats["errors"]:
                log("失败样例：")
                for task, err in stats["errors"][:10]:
                    log(f" - id={task.get('row_id','')} url={task.get('url','')} err={err[:200]}")
            
            # 清理 HOST_CODE_PATH 下的子目录，为下一批做准备
            clear_host_code_subdirs()
            log(f"已清理 HOST_CODE_PATH 子目录，准备下一批次...")

    except Exception as e:
        log(f"WARN: 执行异常：{e}")

    # 最终汇总
    log(f"[最终汇总] 共处理 {batch_num} 批次，成功={total_ok}，失败={total_fail}")

    # 等待并清理容器
    time.sleep(60)
    subprocess.run(f'docker ps -aq -f "name=^{CONTAINER_PREFIX}" | xargs -r docker rm -f', shell=True, check=False)


if __name__ == "__main__":
    subprocess.run(f'docker ps -aq -f "name=^{CONTAINER_PREFIX}" | xargs -r docker rm -f', shell=True, check=False)
    clear_host_code_subdirs()
    main()
