"""
traffic_capture/action.py - BBC新闻流量捕获
继承BaseAction，添加BBC特定的404检测逻辑
"""
import sys
import os

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction


class BBCAction(BaseAction):
    """BBC新闻流量捕获Action"""

    # BBC的阈值设置
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000

    def check_page_not_found(self, html_path, domain):
        """
        检查BBC页面是否为404
        """
        if "bbc" not in domain:
            return False

        if not html_path or not os.path.exists(html_path):
            return False

        try:
            with open(html_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            error_messages = [
                "Sorry, we couldn't find that page",
                "Page cannot be found",
                "Check the page address or search for it below",
                "Sorry, we're unable to bring you the page you're looking for"
            ]

            for msg in error_messages:
                if msg in html_content:
                    self.logger.warning("页面未找到：HTML包含404错误信息")
                    return True

        except Exception as e:
            self.logger.error(f"读取HTML文件失败: {e}")

        return False


if __name__ == "__main__":
    BBCAction.run_from_argv()
