#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
action_batch.py

批量模式：同一 domain 的多个 URL 共享一个 pcap 文件和一个 ssl_key 文件。
只保存 pcap 和 ssl_key，不保存 content、html、screenshot。
接收 JSON 格式：{"domain": "xxx", "urls": [{"row_id": "1", "url": "..."}, ...], "container": "xxx"}
"""

import json
import sys
import os
import subprocess
import time
import threading
from datetime import datetime
from capture import capture, stop_capture
from logger import logger
from chrome import create_chrome_driver
from selenium.webdriver.support.ui import WebDriverWait


def _start_reaper():
    def _loop():
        while True:
            try:
                while True:
                    pid, _ = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
            except ChildProcessError:
                pass
            time.sleep(1)
    threading.Thread(target=_loop, daemon=True).start()


_start_reaper()


def kill_chrome_processes():
    try:
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['pkill', '-f', 'google-chrome'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


def kill_tcpdump_processes():
    try:
        subprocess.run(['sudo', 'pkill', '-f', 'tcpdump'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


def traffic(domain, formatted_time):
    """启动流量捕获"""
    capture(domain, formatted_time, "batch")


def visit_url(driver, url, wait_secs=8):
    """访问 URL，等待页面加载完成"""
    driver.get(url)
    WebDriverWait(driver, wait_secs).until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(15)  # 等待页面完全加载


def start_batch_task():
    payload = json.loads(sys.argv[1])
    container = payload["container"]
    domain = payload["domain"]
    urls = payload["urls"]  # [{"row_id": "1", "url": "..."}, ...]

    logger.info(f"开始批量任务: domain={domain}, url_count={len(urls)}")

    # 清理旧文件
    meta_path = f"/app/meta/{container}_last.json"
    if os.path.exists(meta_path):
        try:
            size = os.path.getsize(meta_path)
            if size != 0:
                with open(meta_path, "r", encoding="utf-8") as f:
                    old_result = json.load(f)
                # 删除旧的 pcap 和 ssl_key
                for key in ["pcap_path", "ssl_key_file_path"]:
                    path = old_result.get(key)
                    if path and os.path.exists(path):
                        os.remove(path)
        except Exception as e:
            logger.error(f"删除旧文件失败: {e}")

    formatted_time = datetime.now().strftime("%Y%m%d_%H_%M_%S")
    kill_chrome_processes()
    kill_tcpdump_processes()
    time.sleep(1)

    # 启动流量捕获（只启动一次，所有 URL 共享）
    traffic_thread = threading.Thread(target=traffic, kwargs={"domain": domain, "formatted_time": formatted_time})
    traffic_thread.start()
    time.sleep(1)

    # 创建浏览器
    logger.info(f"创建浏览器")
    browser, ssl_key_file_path = create_chrome_driver(domain, formatted_time, "batch")

    # 记录访问的 URL
    visited_urls = []

    # 依次访问每个 URL
    for url_info in urls:
        row_id = url_info["row_id"]
        url = url_info["url"]
        logger.info(f"访问 [{row_id}] {url}")

        try:
            visit_url(browser, url)
            visited_urls.append({"row_id": row_id, "url": url, "status": "ok"})
        except Exception as e:
            logger.error(f"访问 [{row_id}] {url} 失败: {e}")
            visited_urls.append({"row_id": row_id, "url": url, "status": "fail", "error": str(e)})

        # URL 之间稍微等待
        time.sleep(15)

    # 关闭浏览器
    try:
        browser.quit()
    except Exception as e:
        logger.warning(f"browser.quit() 异常: {e}")
    logger.info("清理浏览器进程(兜底)")
    kill_chrome_processes()

    # 等待 TCP 挥手完成
    logger.info(f"等待TCP结束挥手完成，耗时60秒")
    time.sleep(60)

    # 停止流量捕获
    logger.info(f"关流量收集")
    pcap_path = stop_capture()
    pcap_file_size = os.path.getsize(pcap_path) if os.path.exists(pcap_path) else 0
    ssl_key_file_size = os.path.getsize(ssl_key_file_path) if os.path.exists(ssl_key_file_path) else 0
    logger.info(f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}")

    # 写入结果（只包含 pcap 和 ssl_key）
    result = {
        "domain": domain,
        "pcap_path": pcap_path or "",
        "ssl_key_file_path": ssl_key_file_path or "",
        "url_count": len(urls),
        "visited_urls": visited_urls
    }

    if not os.path.exists(os.path.dirname(meta_path)):
        os.makedirs(os.path.dirname(meta_path))
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    logger.info(f"批量任务完成: domain={domain}")
    time.sleep(1)


if __name__ == "__main__":
    start_batch_task()
