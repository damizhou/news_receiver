"""
统一的批量任务模块
批量模式：同一 domain 的多个 URL 共享一个 pcap 文件和一个 ssl_key 文件
"""
import json
import sys
import os
import time
import threading
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.capture import capture, stop_capture, kill_tcpdump_processes
from tools.chrome import create_chrome_driver, kill_chrome_processes
from tools.logger import setup_logging


class BatchAction:
    """批量任务Action"""

    def __init__(self):
        self.logger = setup_logging()
        self._start_reaper()

    def _start_reaper(self):
        """启动僵尸进程收割器"""
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

    def traffic(self, domain, formatted_time):
        """启动流量捕获"""
        capture(domain, formatted_time, "batch", data_base_dir=_project_root)

    def visit_url(self, driver, url, wait_secs=8):
        """访问 URL，等待页面加载完成"""
        driver.get(url)
        WebDriverWait(driver, wait_secs).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(15)  # 等待页面完全加载

    def start_batch_task(self, payload):
        """
        启动批量任务

        Args:
            payload: 任务参数，包含container, domain, urls
        """
        container = payload["container"]
        domain = payload["domain"]
        urls = payload["urls"]  # [{"row_id": "1", "url": "..."}, ...]

        self.logger.info(f"开始批量任务: domain={domain}, url_count={len(urls)}")

        # 清理旧文件
        meta_path = os.path.join(_project_root, "meta", f"{container}_last.json")
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
                self.logger.error(f"删除旧文件失败: {e}")

        formatted_time = datetime.now().strftime("%Y%m%d_%H_%M_%S")
        kill_chrome_processes()
        kill_tcpdump_processes()
        time.sleep(1)

        # 启动流量捕获（只启动一次，所有 URL 共享）
        traffic_thread = threading.Thread(
            target=self.traffic,
            kwargs={"domain": domain, "formatted_time": formatted_time}
        )
        traffic_thread.start()
        time.sleep(1)

        # 创建浏览器
        self.logger.info(f"创建浏览器")
        browser, ssl_key_file_path = create_chrome_driver(
            domain, formatted_time, "batch",
            data_base_dir=_project_root
        )

        # 记录访问的 URL
        visited_urls = []

        # 依次访问每个 URL
        for url_info in urls:
            row_id = url_info["row_id"]
            url = url_info["url"]
            self.logger.info(f"访问 [{row_id}] {url}")

            try:
                self.visit_url(browser, url)
                visited_urls.append({"row_id": row_id, "url": url, "status": "ok"})
            except Exception as e:
                self.logger.error(f"访问 [{row_id}] {url} 失败: {e}")
                visited_urls.append({"row_id": row_id, "url": url, "status": "fail", "error": str(e)})

            # URL 之间稍微等待
            time.sleep(15)

        # 关闭浏览器
        try:
            browser.quit()
        except Exception as e:
            self.logger.warning(f"browser.quit() 异常: {e}")

        self.logger.info("清理浏览器进程(兜底)")
        kill_chrome_processes()

        # 等待 TCP 挥手完成
        self.logger.info(f"等待TCP结束挥手完成，耗时60秒")
        time.sleep(60)

        # 停止流量捕获
        self.logger.info(f"关流量收集")
        pcap_path = stop_capture()
        pcap_file_size = os.path.getsize(pcap_path) if pcap_path and os.path.exists(pcap_path) else 0
        ssl_key_file_size = os.path.getsize(ssl_key_file_path) if ssl_key_file_path and os.path.exists(ssl_key_file_path) else 0
        self.logger.info(f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}")

        # 写入结果
        result = {
            "domain": domain,
            "pcap_path": pcap_path or "",
            "ssl_key_file_path": ssl_key_file_path or "",
            "url_count": len(urls),
            "visited_urls": visited_urls
        }

        meta_dir = os.path.dirname(meta_path)
        if not os.path.exists(meta_dir):
            os.makedirs(meta_dir)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        self.logger.info(f"批量任务完成: domain={domain}")
        time.sleep(1)

    @classmethod
    def run_from_argv(cls):
        """从命令行参数运行任务"""
        if len(sys.argv) < 2:
            print("Usage: python action_batch.py '<json_payload>'")
            sys.exit(1)

        payload = json.loads(sys.argv[1])
        action = cls()
        action.start_batch_task(payload)


if __name__ == "__main__":
    BatchAction.run_from_argv()
