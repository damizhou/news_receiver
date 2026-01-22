"""
single_traffice_capture/action.py - 单次流量捕获
继承BaseAction，使用不同的阈值
"""
import sys
import os

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.base_action import BaseAction


class SingleCaptureAction(BaseAction):
    """单次流量捕获Action"""

    # 使用不同的阈值
    pcap_lowest_size = 250000
    ssl_key_lowest_size = 2000

    def validate_files(self, pcap_path, ssl_key_file_path, content_path, html_path):
        """
        验证文件是否有效
        使用OR逻辑：pcap或ssl_key任一满足条件即可
        """
        if not os.path.exists(pcap_path) or not os.path.exists(ssl_key_file_path):
            return False

        pcap_file_size = os.path.getsize(pcap_path)
        ssl_key_file_size = os.path.getsize(ssl_key_file_path)

        self.logger.info(f"pcap文件大小：{pcap_file_size}，ssl_key文件大小：{ssl_key_file_size}")

        # 注意：这里使用OR逻辑
        if (pcap_file_size > self.pcap_lowest_size or
            ssl_key_file_size > self.ssl_key_lowest_size):
            return True

        return False


if __name__ == "__main__":
    SingleCaptureAction.run_from_argv()
