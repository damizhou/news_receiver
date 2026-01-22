"""
统一的流量捕获模块
"""
import os
import sys
import subprocess
import psutil
from datetime import datetime

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 全局进程变量
_capture_process = None


def capture(task_name, formatted_time, parsers, data_base_dir=None, logger=None):
    """
    启动流量捕获

    Args:
        task_name: 任务名称
        formatted_time: 格式化的时间字符串
        parsers: 解析器名称/前缀
        data_base_dir: 数据基础目录
        logger: 日志记录器

    Returns:
        traffic_name: pcap文件路径
    """
    global _capture_process

    if data_base_dir is None:
        data_base_dir = _project_root

    current_time = datetime.now()
    current_data = current_time.strftime("%Y%m%d")
    data_dir = os.path.join(data_base_dir, "data", current_data)
    os.makedirs(data_dir, exist_ok=True)

    filename_prefix = f'{parsers}_' if parsers else ''
    traffic_name = os.path.join(data_dir, f"{filename_prefix}{formatted_time}_{task_name}.pcap")

    # 设置tcpdump命令的参数
    tcpdump_command = [
        "tcpdump",
        "-w",
        traffic_name,
    ]

    if logger:
        logger.info(f'tcpdump_command:{tcpdump_command}')

    # 开流量收集
    _capture_process = subprocess.Popen(tcpdump_command)

    if logger:
        logger.info("开始捕获流量")

    return traffic_name


def stop_capture(logger=None) -> str:
    """
    停止流量捕获

    Args:
        logger: 日志记录器

    Returns:
        file_path: pcap文件路径
    """
    global _capture_process

    if _capture_process is None:
        return ""

    # 获取当前进程的PID
    pid = _capture_process.pid
    try:
        p = psutil.Process(pid)
        cmdline = p.cmdline()
        file_path = cmdline[-1]

        # 尝试设置文件权限
        try:
            host_uid = os.getenv('HOST_UID')
            host_gid = os.getenv('HOST_GID')
            if host_uid and host_gid:
                os.chown(file_path, int(host_uid), int(host_gid))
        except (OSError, ValueError):
            pass

    except psutil.NoSuchProcess:
        file_path = ""

    # 先优雅终止，再等待；若不退出再 kill，并最终 wait()，确保不会留僵尸
    try:
        _capture_process.terminate()
        _capture_process.wait(timeout=5)
    except Exception:
        try:
            _capture_process.kill()
        finally:
            try:
                _capture_process.wait(timeout=3)
            except Exception:
                pass

    _capture_process = None
    return file_path


def kill_tcpdump_processes():
    """清理流量捕获进程"""
    try:
        subprocess.run(
            ['sudo', 'pkill', '-f', 'tcpdump'],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")
