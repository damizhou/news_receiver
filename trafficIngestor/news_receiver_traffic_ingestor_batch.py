#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
news_receiver_traffic_ingestor_batch.py

批量模式：按 domain 分组，同一 domain 的多个 URL 共享一个 pcap 文件。
- 从 CSV 读取记录（id,url,domain）
- 按 domain 分组，每组 URL 按 id 排序
- 每个 domain 作为一个任务，由一个容器执行
- 容器内依次访问所有 URL，共享一个 pcap
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
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor
import shutil
import threading

# ============== 配置 ==============
CODE_BASE_PATH = '/home/pcz/code/news_receiver'
CSV_PATH = "collected_request_urls_all.csv"
CONTAINER_PREFIX = "batch_traffic_ingestor"
START_IDX = 0
END_IDX = 19 * 5 - 1                       # 0..2 共 3 个容器
DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
CONTAINER_CODE_PATH = "/app"
HOST_CODE_PATH = CODE_BASE_PATH + "/batch_traffice_capture"
DASE_DST = '/netdisk/dataset/ablation_study/batch'
# =================================
CREATE_WITH_TTY = True
DOCKER_EXEC_TIMEOUT = 6000
RETRY = 1
NO_TASK_SLEEP_SECONDS = 600
# =================================
EXEC_INTERVAL = 1.0

_last_exec_ts = 0.0
_last_exec_lock = threading.Lock()
_stats_lock = threading.Lock()


def clear_host_code_subdirs(base: str | Path = HOST_CODE_PATH) -> None:
    """只删除数据文件目录：ssl_key, content, html, screenshot, data"""
    base_path = Path(base)
    if not base_path.exists() or not base_path.is_dir():
        log(f"WARN: HOST_CODE_PATH 不存在或不是目录：{base_path}")
        return

    dirs_to_delete = ["ssl_key", "content", "html", "screenshot", "data"]
    for dir_name in dirs_to_delete:
        dir_path = base_path / dir_name
        if dir_path.exists() and dir_path.is_dir():
            try:
                shutil.rmtree(dir_path)
                log(f"删除子目录: {dir_path}")
            except Exception as e:
                log(f"WARN: 删除子目录失败: {dir_path} -> {e}")


def _wait_before_exec():
    """全局节流：保证所有线程之间，每次 docker exec 至少间隔 EXEC_INTERVAL 秒。"""
    global _last_exec_ts
    while True:
        with _last_exec_lock:
            now = time.monotonic()
            delta = _last_exec_ts + EXEC_INTERVAL - now
            if delta <= 0:
                _last_exec_ts = now
                return
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


def build_container_names(prefix: str, start_idx: int, end_idx: int) -> List[str]:
    return [f"{prefix}{i}" for i in range(start_idx, end_idx + 1)]


def read_jobs_batch(csv_path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    读取 CSV 并按 domain 分组，每组包含该 domain 下所有 URL（按 id 排序）。
    返回: ([{"domain": "xxx", "urls": [{"row_id": "1", "url": "..."}, ...]}, ...], header_fields)
    """
    p = Path(csv_path)
    if not p.exists():
        return [], ["id", "url", "domain"]

    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return [], ["id", "url", "domain"]

        header_fields = [h.strip() for h in reader.fieldnames]

        def get_case_insensitive(row: Dict[str, str], key: str) -> str:
            for k, v in row.items():
                if k.lower() == key:
                    return (v or "").strip()
            return ""

        # 按 domain 分组
        domain_groups: Dict[str, List[Dict[str, str]]] = {}
        for r in reader:
            rid = get_case_insensitive(r, "id")
            url = get_case_insensitive(r, "url")
            dom = get_case_insensitive(r, "domain")
            if not url:
                continue
            if dom not in domain_groups:
                domain_groups[dom] = []
            domain_groups[dom].append({"row_id": rid, "url": url})

        # 每组按 id 排序，生成任务列表
        jobs: List[Dict[str, Any]] = []
        for domain, urls in domain_groups.items():
            try:
                urls.sort(key=lambda x: int(x["row_id"]))
            except (ValueError, TypeError):
                urls.sort(key=lambda x: x["row_id"])
            jobs.append({"domain": domain, "urls": urls})

    return jobs, header_fields


def chown_recursive(path: str, uid: int = 1002, gid: int = 1002) -> None:
    """把 path（文件或目录）及其子项设为 uid:gid。"""
    try:
        os.chown(path, uid, gid, follow_symlinks=False)
    except Exception:
        pass
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path, followlinks=False):
            for name in dirs:
                p = os.path.join(root, name)
                try:
                    os.chown(p, uid, gid, follow_symlinks=False)
                except Exception:
                    pass
            for name in files:
                p = os.path.join(root, name)
                try:
                    os.chown(p, uid, gid, follow_symlinks=False)
                except Exception:
                    pass


def exec_batch(task: Dict[str, Any]) -> Tuple[bool, str]:
    """
    执行批量任务：一个 domain 的多个 URL 共享一个 pcap 和 ssl_key
    task: {"domain": "xxx", "urls": [...], "container": "xxx"}
    """
    _wait_before_exec()
    payload = json.dumps(task, ensure_ascii=False)
    container = task["container"]
    cmd = [
        "docker", "exec", container,
        "python", "-u", f"{CONTAINER_CODE_PATH}/action_batch.py",
        payload
    ]
    log(f"执行命令: {cmd}")
    cp = run(cmd, timeout=DOCKER_EXEC_TIMEOUT)

    if cp.returncode == 0:
        try:
            with open(CODE_BASE_PATH + f"/batch_traffice_capture/meta/{container}_last.json", "r", encoding="utf-8") as f:
                result = json.load(f)

            pcap_path = result.get("pcap_path")
            ssl_key_file_path = result.get("ssl_key_file_path")

            if not pcap_path or not ssl_key_file_path:
                return False, "result JSON missing pcap_path or ssl_key_file_path"

            domain = task["domain"]
            dst = os.path.join(DASE_DST, domain)

            # 移动 pcap
            pcap_dst = os.path.join(dst, 'pcap')
            os.makedirs(pcap_dst, exist_ok=True)
            pcap_path_host = pcap_path.replace("/app/", CODE_BASE_PATH + "/batch_traffice_capture/")
            new_pcap = shutil.move(pcap_path_host, pcap_dst)
            chown_recursive(new_pcap)

            # 移动 ssl_key
            ssl_key_dst = os.path.join(dst, 'ssl_key')
            os.makedirs(ssl_key_dst, exist_ok=True)
            ssl_key_path_host = ssl_key_file_path.replace("/app/", CODE_BASE_PATH + "/batch_traffice_capture/")
            new_ssl = shutil.move(ssl_key_path_host, ssl_key_dst)
            chown_recursive(new_ssl)

            return True, ""
        except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
            return False, f"post-processing error: {e}"
    return False, (cp.stderr.strip() or cp.stdout.strip())


def worker_loop_batch(container: str, q: "queue.Queue[Dict[str, Any]]", stats: dict, retry: int):
    """批量模式的 worker：每个任务是一个 domain 的所有 URL"""
    while True:
        try:
            task = q.get_nowait()
        except queue.Empty:
            return
        domain = task.get("domain", "")
        url_count = len(task.get("urls", []))
        task["container"] = container
        try:
            log(f"{container} -> start domain={domain} urls={url_count}")
            ok, err = exec_batch(task)
            if ok:
                log(f"{container} -> done  domain={domain}")
                with _stats_lock:
                    stats["ok"] += 1
            else:
                log(f"{container} -> fail  domain={domain} err={err[:200]}")
                with _stats_lock:
                    stats["fail"] += 1
                    stats["errors"].append((task, err))
        except subprocess.TimeoutExpired:
            err = f"timeout>{DOCKER_EXEC_TIMEOUT}s"
            log(f"{container} -> timeout domain={domain}")
            with _stats_lock:
                stats["fail"] += 1
                stats["errors"].append((task, err))
        except Exception as e:
            err = repr(e)
            log(f"{container} -> error domain={domain} err={err}")
            with _stats_lock:
                stats["fail"] += 1
                stats["errors"].append((task, err))
        finally:
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

    for n in names:
        exists = container_exists(n)
        if exists is None:
            create_container(n, str(host_code), DOCKER_IMAGE)
            created.append(n)

    for n in names:
        if not container_running(n):
            start_container(n)

    for n in created:
        disable_offload_once(n)

    return names


def sig(signum, _frame):
    log(f"收到中断信号({signum})，立即退出。")
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(128 + signum)


def main():
    signal.signal(signal.SIGINT, sig)
    signal.signal(signal.SIGTERM, sig)

    names = prepare_pool_once()

    try:
        jobs, header_fields = read_jobs_batch(CSV_PATH)
        if not jobs:
            log("没有可处理的任务，退出。")
            return

        q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        for t in jobs:
            q.put(t)

        stats = {"ok": 0, "fail": 0, "errors": []}
        log(f"开始执行：domain任务数={len(jobs)}，并发容器={len(names)}，镜像={DOCKER_IMAGE}")

        with ThreadPoolExecutor(max_workers=len(names)) as pool:
            for n in names:
                pool.submit(worker_loop_batch, n, q, stats, RETRY)
            q.join()

        log(f"[summary] success={stats['ok']} fail={stats['fail']} total={len(jobs)}")
        if stats["errors"]:
            log("失败样例：")
            for task, err in stats["errors"][:10]:
                log(f" - domain={task.get('domain','')} err={err[:200]}")

    except Exception as e:
        log(f"WARN: 执行异常：{e}")

    time.sleep(60)
    # subprocess.run(f'docker ps -aq -f "name=^{CONTAINER_PREFIX}" | xargs -r docker rm -f', shell=True, check=False)

if __name__ == "__main__":
    subprocess.run(f'docker ps -aq -f "name=^{CONTAINER_PREFIX}" | xargs -r docker rm -f', shell=True, check=False)
    clear_host_code_subdirs()
    count = 120
    print(f"开始执行数据采集任务,共计{count}次")
    for i in range(120):
        print(f'当前开始执行第{i + 1}次')
        main()
