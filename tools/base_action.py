"""
BaseAction基类 - 抽取action.py中的公共方法
各目录的action.py继承此基类，只需实现差异部分
"""
import json
import os
import sys
import subprocess
import time
import threading
from datetime import datetime
from abc import ABC, abstractmethod

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.capture import capture, stop_capture, kill_tcpdump_processes
from tools.chrome import create_chrome_driver, open_url_and_save_content, kill_chrome_processes
from tools.logger import setup_logging


class BaseAction(ABC):
    """
    Action基类，封装流量捕获任务的公共逻辑
    """

    # 默认阈值，子类可覆盖
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000
    max_retry_count = 4

    def __init__(self):
        self.current_index = 0
        self.allowed_domain = ""
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

    def traffic(self, index=0, formatted_time=None):
        """启动流量捕获"""
        capture(self.allowed_domain, formatted_time, f"{index}", data_base_dir=_project_root)

    def clean_old_files(self, meta_path):
        """清理旧文件"""
        if os.path.exists(meta_path):
            size = os.path.getsize(meta_path)
            if size != 0:
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        old_result = json.load(f)

                    paths_to_delete = [
                        old_result.get("pcap_path"),
                        old_result.get("ssl_key_file_path"),
                        old_result.get("content_path"),
                        old_result.get("html_path"),
                        old_result.get("screenshot_path"),
                    ]

                    for path in paths_to_delete:
                        if path and os.path.exists(path):
                            os.remove(path)
                except Exception as e:
                    self.logger.error(f"删除旧文件失败: {e}")

    def delete_invalid_files(self, pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path):
        """删除不合格的文件"""
        try:
            for path in [pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path]:
                if path and os.path.exists(path):
                    os.remove(path)
        except Exception as e:
            self.logger.error(f"删除不合格文件失败: {e}")

    def validate_files(self, pcap_path, ssl_key_file_path, content_path, html_path):
        """
        验证文件是否有效
        子类可覆盖此方法实现自定义验证逻辑

        Returns:
            bool: 文件是否有效
        """
        if not os.path.exists(pcap_path) or not os.path.exists(ssl_key_file_path):
            return False

        pcap_file_size = os.path.getsize(pcap_path)
        ssl_key_file_size = os.path.getsize(ssl_key_file_path)

        self.logger.info(f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}")

        if (pcap_file_size > self.pcap_lowest_size and
            ssl_key_file_size > self.ssl_key_lowest_size and
            os.path.exists(content_path) and
            os.path.exists(html_path)):
            return True

        self.logger.info(f"pcap_lowest_size:{self.pcap_lowest_size} > pcap_file_size:{pcap_file_size}")
        self.logger.info(f"ssl_key_lowest_size:{self.ssl_key_lowest_size} > ssl_key_file_size:{ssl_key_file_size}")
        return False

    def check_page_not_found(self, html_path, domain):
        """
        检查页面是否为404
        子类可覆盖此方法实现自定义检测逻辑

        Returns:
            bool: 是否为404页面
        """
        return False

    def write_result(self, meta_path, result):
        """写入结果到meta文件"""
        meta_dir = os.path.dirname(meta_path)
        if not os.path.exists(meta_dir):
            os.makedirs(meta_dir)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    def start_task(self, payload):
        """
        启动任务的主方法

        Args:
            payload: 任务参数，包含container, row_id, url, domain
        """
        self.current_index += 1

        container = payload["container"]
        row_id = payload["row_id"]
        url = payload["url"]
        self.allowed_domain = payload["domain"]

        # 清理旧文件
        meta_path = os.path.join(_project_root, "meta", f"{container}_last.json")
        self.clean_old_files(meta_path)

        formatted_time = datetime.now().strftime("%Y%m%d_%H_%M_%S")
        kill_chrome_processes()
        kill_tcpdump_processes()
        time.sleep(1)

        # 初始化变量
        content_path = ""
        html_path = ""
        screenshot_path = ""
        pcap_path = ""

        # 开流量收集
        traffic_thread = threading.Thread(
            target=self.traffic,
            kwargs={"index": row_id, "formatted_time": formatted_time}
        )
        traffic_thread.start()
        time.sleep(1)

        self.logger.info(f"创建浏览器")
        browser, ssl_key_file_path = create_chrome_driver(
            self.allowed_domain, formatted_time, f"{row_id}",
            data_base_dir=_project_root
        )

        self.logger.info(f"开始访问第{row_id}的词条：{url}")
        try:
            content_path, html_path, screenshot_path = open_url_and_save_content(
                browser, url, ssl_key_file_path,
                data_base_dir=_project_root
            )
        except Exception as e:
            self.logger.error(f"open_url_and_save_content 异常: {e}")

        try:
            browser.quit()
        except Exception as e:
            self.logger.warning(f"browser.quit() 异常: {e}")

        self.logger.info("清理浏览器进程(兜底)")
        kill_chrome_processes()

        self.logger.info(f"等待TCP结束挥手完成，耗时60秒")
        time.sleep(60)

        # 关流量收集
        self.logger.info(f"关流量收集")
        pcap_path = stop_capture()

        # 检查页面是否为404
        page_not_found = self.check_page_not_found(html_path, self.allowed_domain)

        # 验证文件
        need_restart = False
        if page_not_found:
            self.logger.warning("页面不存在，跳过重试")
        elif self.validate_files(pcap_path, ssl_key_file_path, content_path, html_path):
            self.logger.info("数据文件校验通过")
        else:
            need_restart = True

        # 校验不通过时删除文件
        if page_not_found or need_restart:
            self.delete_invalid_files(pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path)

        if need_restart and self.current_index < self.max_retry_count:
            self.logger.info("流量文件大小未通过校验，准备重试")
            time.sleep(5)
            self.start_task(payload)
        else:
            # 构建结果
            if need_restart or page_not_found:
                result = {
                    "pcap_path": "",
                    "ssl_key_file_path": "",
                    "content_path": "",
                    "html_path": "",
                    "row_id": row_id,
                    "screenshot_path": ""
                }
                if page_not_found:
                    self.logger.warning(f"页面不存在，任务失败: row_id={row_id}")
                else:
                    self.logger.warning(f"重试次数用尽，任务失败: row_id={row_id}")
            else:
                result = {
                    "pcap_path": pcap_path or "",
                    "ssl_key_file_path": ssl_key_file_path or "",
                    "content_path": content_path or "",
                    "html_path": html_path or "",
                    "row_id": row_id,
                    "screenshot_path": screenshot_path or ""
                }

            self.write_result(meta_path, result)

        time.sleep(1)

    @classmethod
    def run_from_argv(cls):
        """从命令行参数运行任务"""
        if len(sys.argv) < 2:
            print("Usage: python action.py '<json_payload>'")
            sys.exit(1)

        payload = json.loads(sys.argv[1])
        action = cls()
        action.start_task(payload)
