"""
统一日志模块
支持可选的容器名称参数
"""
from datetime import datetime
import logging
import logging.handlers
import os
import sys

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def setup_logging(container_name=None, logs_dir=None):
    """
    配置日志基本设置

    Args:
        container_name: 容器名称，用于区分不同容器的日志，默认从环境变量获取
        logs_dir: 日志目录，默认为当前目录下的logs文件夹

    Returns:
        logger: 配置好的logger实例
    """
    if logs_dir is None:
        logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # 尝试设置文件权限（仅在Docker环境中有效）
    try:
        host_uid = os.getenv('HOST_UID')
        host_gid = os.getenv('HOST_GID')
        if host_uid and host_gid:
            os.chown(logs_dir, int(host_uid), int(host_gid))
    except (OSError, ValueError):
        pass  # 非Docker环境或无权限时忽略

    # 获取当前时间
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y%m%d")

    # 获取容器名称
    if container_name is None:
        container_name = os.getenv('CONTAINER_NAME')

    # 构建日志文件名
    if container_name:
        filename = f"{formatted_time}_{container_name}.log"
    else:
        filename = f"{formatted_time}.log"

    # 创建一个logger
    logger_name = filename.split(".")[0]
    logger = logging.getLogger(logger_name)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # 创建一个handler，用于写入日志文件
    log_file = os.path.join(logs_dir, filename)

    # 用于写入日志文件，当文件大小超过100MB时进行滚动
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=100 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)

    # 创建一个handler，用于将日志输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 给logger添加handler
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger(name=None):
    """
    获取logger实例

    Args:
        name: logger名称，默认使用当天日期

    Returns:
        logger: logger实例
    """
    if name is None:
        name = datetime.now().strftime("%Y%m%d")
    return logging.getLogger(name)


# 默认logger实例（延迟初始化）
_default_logger = None


def get_default_logger():
    """获取默认logger实例"""
    global _default_logger
    if _default_logger is None:
        _default_logger = setup_logging()
    return _default_logger


# 为了向后兼容，提供一个模块级的logger
logger = None

def _init_module_logger():
    global logger
    if logger is None:
        logger = setup_logging()
    return logger
