"""
tools - 共用工具模块
包含浏览器驱动、日志、流量捕获等公共功能
"""
import os

# 项目根目录
project_path = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

# 导出公共模块
from .logger import setup_logging, get_logger
from .chrome import create_chrome_driver, open_url_and_save_content, screenshot_full_page, kill_chrome_processes
from .firefox import create_firefox_driver, kill_firefox_processes
from .capture import capture, stop_capture
from .base_action import BaseAction
