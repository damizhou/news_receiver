"""
batch_traffice_capture/action_batch.py - 批量任务入口
调用共用的BatchAction
"""
import sys
import os

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.action_batch import BatchAction

if __name__ == "__main__":
    BatchAction.run_from_argv()
