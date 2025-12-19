import json
import shutil
import sys
import os
import subprocess
import time
import threading
from datetime import datetime
from capture import capture, stop_capture
from logger import logger
from chrome import create_chrome_driver, open_url_and_save_content
current_index = 0
allowed_domain = ""
wiwi_pcap_lowest_size = 250000
wiwi_ssl_key_lowest_size = 2000

def _start_reaper():
    import threading, os, time
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

# 清除浏览器进程
def kill_chrome_processes():
    try:
        # Run the command to kill all processes containing 'chrome'
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['pkill', '-f', 'google-chrome'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


# 流量捕获进程
def traffic(index=0, formatted_time=None):
    # 获取当前时间
    current_time = datetime.now()
    # 格式化输出
    capture(allowed_domain, formatted_time, f"{index}")

# 清理流量捕获进程
def kill_tcpdump_processes():
    try:
        # Run the command to kill all processes containing 'chrome'
        # logger.info(f"清理流量捕获进程")
        subprocess.run(['sudo', 'pkill', '-f', 'tcpdump'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


def start_task():
    global current_index
    current_index += 1
    global allowed_domain
    payload = json.loads(sys.argv[1])
    container = payload["container"]
    row_id = payload["row_id"]
    url = payload["url"]
    allowed_domain = payload["domain"]

    # 清理旧文件
    meta_path = f"/app/meta/{container}_last.json"
    if os.path.exists(meta_path):
        size = os.path.getsize(meta_path)
        if size != 0:
            with open(meta_path, "r", encoding="utf-8") as f:
                old_result = json.load(f)
                pcap_path = old_result.get("pcap_path")
                ssl_key_file_path = old_result.get("ssl_key_file_path")
                content_path = old_result.get("content_path")
                html_path = old_result.get("html_path")
                screenshot_path = old_result.get("screenshot_path")
                # 删除文件
                try:
                    if pcap_path and os.path.exists(pcap_path):
                        os.remove(pcap_path)
                    if ssl_key_file_path and os.path.exists(ssl_key_file_path):
                        os.remove(ssl_key_file_path)
                    if content_path and os.path.exists(content_path):
                        os.remove(content_path)
                    if html_path and os.path.exists(html_path):
                        os.remove(html_path)
                    if html_path and os.path.exists(screenshot_path):
                        os.remove(screenshot_path)
                except Exception as e:
                    logger.error(f"删除旧文件失败: {e}")

    formatted_time = datetime.now().strftime("%Y%m%d_%H_%M_%S")
    kill_chrome_processes()
    kill_tcpdump_processes()
    time.sleep(1)

    # 开流量收集
    traffic_thread = threading.Thread(target=traffic, kwargs={"index": row_id, "formatted_time":formatted_time} )
    traffic_thread.start()
    time.sleep(1)
    logger.info(f"创建浏览器")
    browser, ssl_key_file_path = create_chrome_driver(allowed_domain, formatted_time, f"{row_id}")
    logger.info(f"开始访问第{row_id}的词条：{url}")
    content_path, html_path, screenshot_path = open_url_and_save_content(browser, url, ssl_key_file_path)

    try:
        browser.quit()  # 彻底退出，会回收 chromedriver 与子进程
    except Exception as e:
        logger.warning(f"browser.quit() 异常: {e}")
    logger.info("清理浏览器进程(兜底)")
    kill_chrome_processes()

    logger.info(f"等待TCP结束挥手完成，耗时60秒")
    time.sleep(60)

    # 关流量收集
    logger.info(f"关流量收集")
    pcap_path = stop_capture()
    pcap_file_size = os.path.getsize(pcap_path)
    ssl_key_file_size = os.path.getsize(ssl_key_file_path)
    logger.info(f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}")
    need_restart = False
    if pcap_file_size > wiwi_pcap_lowest_size and ssl_key_file_size > wiwi_ssl_key_lowest_size and os.path.exists(content_path) and os.path.exists(html_path):
        logger.info("数据文件校验通过")
    else:
        need_restart = True
        # 删除不合格的文件
        try:
            if os.path.exists(pcap_path):
                os.remove(pcap_path)
            if os.path.exists(ssl_key_file_path):
                os.remove(ssl_key_file_path)
            if os.path.exists(content_path):
                os.remove(content_path)
            if os.path.exists(html_path):
                os.remove(html_path)
            if os.path.exists(screenshot_path):
                os.remove(screenshot_path)
        except Exception as e:
            logger.error(f"删除不合格文件失败: {e}")

    if need_restart and current_index < 4:
        logger.info("流量文件大小未通过校验，准备重试")
        time.sleep(5)
        start_task()
    else:
        result = {"pcap_path": pcap_path or "", "ssl_key_file_path": ssl_key_file_path or "", "content_path": content_path or "",
            "html_path": html_path or "", "row_id": row_id, "screenshot_path": screenshot_path}
        if not os.path.exists(os.path.dirname(meta_path)):
            os.makedirs(os.path.dirname(meta_path))
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)  # 中文不转义，缩进美化
    time.sleep(1)

if __name__ == "__main__":
    start_task()
