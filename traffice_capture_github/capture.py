import os
from logger import logger
import shutil
import subprocess
import psutil
from datetime import datetime

def capture(task_name, formatted_time, parsers):
    current_time = datetime.now()
    current_data = current_time.strftime("%Y%m%d")
    data_dir = os.path.join("/app", "data", current_data)
    os.makedirs(data_dir, exist_ok=True)
    filename = f'{parsers}_'

    traffic_name = os.path.join(data_dir, f"{filename}{formatted_time}_{task_name}.pcap")

    # 设置tcpdump命令的参数
    tcpdump_command = [
        "tcpdump",
        "-w",
        traffic_name,  # 输出文件的路径
    ]

    logger.info(f'tcpdump_command:{tcpdump_command}')
    global process
    # 开流量收集
    process = subprocess.Popen(tcpdump_command)
    #
    logger.info("开始捕获流量")
    return traffic_name


def stop_capture() -> str:
    global process
    # 获取当前进程的PID
    pid = process.pid
    p = psutil.Process(pid)
    cmdline = p.cmdline()
    file_path = cmdline[-1]
    os.chown(file_path, int(os.getenv('HOST_UID')), int(os.getenv('HOST_GID')))
    # 先优雅终止，再等待；若不退出再 kill，并最终 wait()，确保不会留僵尸
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
        finally:
            try:
                process.wait(timeout=3)
            except Exception:
                pass
    return file_path
