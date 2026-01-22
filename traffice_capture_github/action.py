"""
traffice_capture_github/action.py - GitHub流量捕获
继承BaseAction
"""
import sys
import os

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.base_action import BaseAction


class GitHubCaptureAction(BaseAction):
    """GitHub流量捕获Action"""

    # 使用默认阈值
    pcap_lowest_size = 100000
    ssl_key_lowest_size = 1000


if __name__ == "__main__":
    GitHubCaptureAction.run_from_argv()
