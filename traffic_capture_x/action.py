"""
traffic_capture_x/action.py - X(Twitter)流量捕获
继承BaseAction
"""
import sys
import os

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.base_action import BaseAction


class XCaptureAction(BaseAction):
    """X(Twitter)流量捕获Action"""

    # 使用默认阈值
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000


if __name__ == "__main__":
    XCaptureAction.run_from_argv()
