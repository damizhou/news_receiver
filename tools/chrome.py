"""
统一的Chrome浏览器驱动模块
合并了所有功能：SSL密钥日志、截图、内容提取等
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
import os
import sys
import base64
import json
import subprocess
import re
import time
import math
from pathlib import Path
from datetime import datetime
from typing import Optional

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# JavaScript代码：全选并复制页面内容（模拟 Ctrl+A + Ctrl+C）
JS_SELECT_ALL_AND_COPY_CAPTURE = r"""
function __select_all_and_copy_capture(){
  try {
    const sel = window.getSelection();

    // 备份原选区
    const saved = [];
    for (let i = 0; i < sel.rangeCount; i++) saved.push(sel.getRangeAt(i).cloneRange());

    // 全选整个文档（模拟 Ctrl+A）
    sel.removeAllRanges();
    const range = document.createRange();
    range.selectNodeContents(document.documentElement);
    sel.addRange(range);

    // 获取纯文本（等同于 Ctrl+C 复制后粘贴的效果）
    const plain = sel.toString();

    // 获取 HTML
    const box = document.createElement('div');
    box.appendChild(range.cloneContents());
    const html = box.innerHTML;

    // 恢复选区
    sel.removeAllRanges();
    for (const r of saved) sel.addRange(r);

    return { plain, html };
  } catch(e) {
    return { error: String(e) };
  }
}
"""


def is_docker():
    """检测是否在Docker环境中运行"""
    # 检查cgroup文件
    try:
        with open('/proc/1/cgroup', 'r') as f:
            for line in f:
                if 'docker' in line or 'kubepods' in line:
                    return True
    except FileNotFoundError:
        pass

    # 检查环境变量
    if os.path.exists('/.dockerenv'):
        return True

    return False


def create_chrome_driver(task_name=None, formatted_time=None, parsers=None,
                          enable_ssl_key_log=True, data_base_dir=None):
    """
    创建Chrome浏览器驱动

    Args:
        task_name: 任务名称，用于生成文件名
        formatted_time: 格式化的时间字符串
        parsers: 解析器名称/前缀
        enable_ssl_key_log: 是否启用SSL密钥日志（默认True）
        data_base_dir: 数据基础目录，默认为项目根目录下的相对路径

    Returns:
        browser: WebDriver实例
        ssl_key_file_path: SSL密钥日志文件路径（如果启用）
    """
    # 确定数据基础目录（使用相对路径）
    if data_base_dir is None:
        data_base_dir = _project_root

    ssl_key_file_path = None

    if enable_ssl_key_log and task_name and formatted_time:
        current_time = datetime.now()
        current_data = current_time.strftime("%Y%m%d")
        ssl_key_dir = os.path.join(data_base_dir, "ssl_key", current_data)
        os.makedirs(ssl_key_dir, exist_ok=True)

        filename_prefix = f'{parsers}_' if parsers else ''
        ssl_key_file_path = os.path.join(
            ssl_key_dir,
            f"{filename_prefix}{formatted_time}_{task_name}_ssl_key.log"
        )

    # 在当前目录中创建download文件夹
    download_folder = os.path.join(os.getcwd(), 'download')
    os.makedirs(download_folder, exist_ok=True)

    os.environ["SE_OFFLINE"] = "true"
    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"
    _LANG_PRIMARY = "zh-CN"

    # 创建 ChromeOptions 实例
    chrome_options = Options()

    # Docker环境设置
    if is_docker():
        chrome_options.binary_location = "/usr/bin/google-chrome"

    chrome_options.add_argument('--headless')  # 无界面模式
    chrome_options.add_argument("--disable-gpu")  # 禁用 GPU 加速
    chrome_options.add_argument("--disable-features=AsyncDns")  # 禁用Chrome异步DNS，使用系统DNS
    chrome_options.add_argument("--disable-async-dns")  # 备用参数
    chrome_options.add_argument("--no-sandbox")  # 禁用沙盒
    chrome_options.add_argument("--disable-dev-shm-usage")  # 限制使用/dev/shm
    chrome_options.add_argument("--incognito")  # 隐身模式
    chrome_options.add_argument("--disable-application-cache")  # 禁用应用缓存
    chrome_options.add_argument("--disable-extensions")  # 禁用扩展
    chrome_options.add_argument("--disable-infobars")  # 禁用信息栏
    chrome_options.add_argument("--disable-software-rasterizer")  # 禁用软件光栅化
    chrome_options.add_argument("--autoplay-policy=no-user-gesture-required")  # 允许自动播放
    chrome_options.add_argument(f"--lang={_LANG_PRIMARY}")  # 启动语言
    chrome_options.add_argument("--disable-background-networking")  # 降低背景"噪音"联网
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--homepage=about:blank")
    chrome_options.add_argument("--log-net-log=/tmp/netlog.json")
    chrome_options.add_argument("--net-log-capture-mode=Everything")

    # SSL密钥日志
    if ssl_key_file_path:
        chrome_options.add_argument(f"--ssl-key-log-file={ssl_key_file_path}")
        print(f"SSL 密钥日志文件路径: {ssl_key_file_path}")

    # 设置实验性首选项
    prefs = {
        "profile.default_content_settings.popups": 0,
        "credentials_enable_service": False,  # 禁用密码管理器弹窗
        "profile.password_manager_enabled": False,  # 禁用密码管理器
        "download.default_directory": download_folder,  # 默认下载目录
        "download.prompt_for_download": False,  # 不提示下载
        "download.directory_upgrade": True,  # 升级下载目录
        "safebrowsing.enabled": True,  # 启用安全浏览
        "intl.accept_languages": _ACCEPT_LANGUAGE,  # 首选语言
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # 启用性能日志记录
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # 创建 WebDriver 实例
    if is_docker():
        service = Service(executable_path="/usr/local/bin/chromedriver")
    else:
        service = Service()

    browser = webdriver.Chrome(service=service, options=chrome_options)
    browser.execute_cdp_cmd('Network.enable', {})
    browser.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': {'Accept-Language': _ACCEPT_LANGUAGE}})
    browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                            {'source': '''
                            Object.defineProperty(navigator,"webdriver",{get:()=>undefined});
                            Object.defineProperty(navigator,"language",{get:()=> "zh-CN"});
                            Object.defineProperty(navigator,"languages",{get:()=> ["zh-CN","zh"]});
                            '''.strip()})

    if ssl_key_file_path:
        return browser, ssl_key_file_path
    return browser


def open_url_and_save_content(driver, url, ssl_key_file_path, wait_secs=8,
                               save_screenshot=True, data_base_dir=None):
    """
    打开URL并保存内容

    Args:
        driver: WebDriver实例
        url: 要访问的URL
        ssl_key_file_path: SSL密钥日志文件路径
        wait_secs: 等待页面加载的超时时间
        save_screenshot: 是否保存截图
        data_base_dir: 数据基础目录

    Returns:
        content_path: 文本内容文件路径
        html_path: HTML文件路径
        screenshot_path: 截图文件路径（如果启用）
        current_url: 重定向后的真实URL
    """
    if data_base_dir is None:
        data_base_dir = _project_root

    driver.get(url)
    WebDriverWait(driver, wait_secs).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(15)

    screenshot_path = None
    if save_screenshot:
        screenshot_path = ssl_key_file_path.replace("_ssl_key.log", ".png").replace("/ssl_key/", "/screenshot/")
        os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
        screenshot_full_page(driver, Path(screenshot_path), dpr=2.0)

    script = JS_SELECT_ALL_AND_COPY_CAPTURE + "\nreturn __select_all_and_copy_capture();"
    res = driver.execute_script(script)
    if not isinstance(res, dict) or res.get("error"):
        raise RuntimeError(f"JS失败: {res}")

    plain = re.sub(r'(?:[ \t\f\u00A0\u3000\u200B\u200C\u200D\uFEFF\u2060\u00AD\v]*\r?\n)+', '\n', res.get("plain", ""))
    content_path = ssl_key_file_path.replace("_ssl_key.log", ".text").replace("/ssl_key/", "/content/")
    html_path = ssl_key_file_path.replace("_ssl_key.log", ".html").replace("/ssl_key/", "/html/")

    os.makedirs(os.path.dirname(content_path), exist_ok=True)
    with open(content_path, "w", encoding="utf-8") as f:
        f.write(plain)

    html = driver.page_source  # 此刻的 DOM（包含已渲染的动态内容）
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    # 获取重定向后的真实URL
    current_url = driver.current_url

    return content_path, html_path, screenshot_path, current_url


def screenshot_full_page(driver: webdriver.Chrome, out_path: Path, dpr: Optional[float] = None) -> None:
    """
    整页长截图：通过 CDP 获取内容尺寸并原生捕获，不做滚动拼接。

    Args:
        driver: WebDriver实例
        out_path: 输出文件路径
        dpr: 设备像素比
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 计算页面内容尺寸
    metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
    content_size = metrics.get("contentSize", {})
    width = int(math.ceil(content_size.get("width", 0) or 0))
    height = int(math.ceil(content_size.get("height", 0) or 0))

    if width == 0 or height == 0:
        # 退路：用 JS 获取 body 尺寸
        width = int(driver.execute_script(
            "return Math.ceil(document.documentElement.scrollWidth||document.body.scrollWidth||0);"
        ))
        height = int(driver.execute_script(
            "return Math.ceil(document.documentElement.scrollHeight||document.body.scrollHeight||0);"
        ))

    device_scale = float(dpr) if dpr and dpr > 0 else 1.0

    # 覆盖设备度量，扩大视窗到整页尺寸
    driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
        "mobile": False,
        "width": width,
        "height": height,
        "deviceScaleFactor": device_scale,
        "screenOrientation": {"type": "landscapePrimary", "angle": 0},
    })

    # 捕获位图（b64）
    data = driver.execute_cdp_cmd("Page.captureScreenshot", {
        "fromSurface": True,
        "captureBeyondViewport": True
    })
    png_b64 = data.get("data")
    out_path.write_bytes(base64.b64decode(png_b64))

    # 恢复度量，避免影响后续操作
    driver.execute_cdp_cmd("Emulation.clearDeviceMetricsOverride", {})


def kill_chrome_processes():
    """清除浏览器进程"""
    try:
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False,
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['pkill', '-f', 'google-chrome'], check=False,
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr.decode('utf-8')}")


def add_cookies(browser, cookie_file):
    """
    添加cookies到浏览器

    Args:
        browser: WebDriver实例
        cookie_file: cookie文件路径
    """
    with open(cookie_file, "r", encoding="utf-8") as f:
        raw_cookies = json.load(f)

    for ck in raw_cookies:
        try:
            browser.add_cookie(_sanitize_cookie(ck))
        except Exception as e:
            print("跳过无效 cookie:", ck.get("name"), e)


def _sanitize_cookie(raw: dict) -> dict:
    """把 DevTools 导出的 cookie → Selenium 可接受格式"""
    c = {}

    # ===== 必选键 =====
    c["name"] = raw["name"]
    c["value"] = raw["value"]

    # ===== 可选键 =====
    if "domain" in raw:
        c["domain"] = raw["domain"].lstrip(".")  # 去掉前导点
    c["path"] = raw.get("path", "/")

    # secure / httpOnly
    c["secure"] = bool(raw.get("secure", False))
    c["httpOnly"] = bool(raw.get("httpOnly", False))

    # SameSite：枚举映射
    samesite_map = {
        "no_restriction": "None",
        "unspecified": None,
        "lax": "Lax",
        "strict": "Strict",
        "none": "None",
    }
    ss = raw.get("sameSite")
    ss_fixed = samesite_map.get(str(ss).lower())
    if ss_fixed:
        c["sameSite"] = ss_fixed

    # expiry
    if "expirationDate" in raw:
        c["expiry"] = int(raw["expirationDate"])
    elif "expiry" in raw:
        c["expiry"] = int(raw["expiry"])

    return c
