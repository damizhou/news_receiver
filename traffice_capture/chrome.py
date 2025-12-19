from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import base64
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import re
import time
import math
from pathlib import Path
from typing import Optional
from selenium.webdriver.support.ui import WebDriverWait  # 从selenium.webdriver.support.wait改为支持ui

JS_SELECT_ALL_AND_COPY_CAPTURE = r"""
function __select_all_and_copy_capture(){
  try{
    const sel = window.getSelection();
    // 备份原选区
    const saved = [];
    for (let i=0;i<sel.rangeCount;i++){ saved.push(sel.getRangeAt(i).cloneRange()); }
    function restore(){
      sel.removeAllRanges();
      for (const r of saved) sel.addRange(r);
    }
    // Ctrl+A：全选 <body>（尽量贴近浏览器行为）
    sel.removeAllRanges();
    const root = document.body || document.documentElement;
    const range = document.createRange();
    range.selectNodeContents(root);
    sel.addRange(range);

    function selectionPlain(){ return sel.toString(); }
    function selectionHTML(){
      const box = document.createElement('div');
      for (let i=0;i<sel.rangeCount;i++) box.appendChild(sel.getRangeAt(i).cloneContents());
      return box.innerHTML;
    }
    const defaultPlain = selectionPlain();
    const defaultHtml  = selectionHTML();

    // 监听 copy，尽量捕获站点可能改写的内容（若站点在 copy 里 setData）
    let copiedPlain = null, copiedHtml = null;
    function onCopyCapture(e){ /* 预留 */ }
    function onCopyBubble(e){
      try{ copiedHtml  = e.clipboardData.getData('text/html')  || null; }catch(_){}
      try{ copiedPlain = e.clipboardData.getData('text/plain') || null; }catch(_){}
    }
    document.addEventListener('copy', onCopyCapture, true);
    document.addEventListener('copy', onCopyBubble, false);

    let execOk = false;
    try { execOk = document.execCommand('copy'); } catch(_){}

    document.removeEventListener('copy', onCopyCapture, true);
    document.removeEventListener('copy', onCopyBubble, false);
    restore();

    // 如果站点没改写，则 copied* 可能是空，就用默认选区内容兜底
    return {
      execOk,
      plain: copiedPlain  != null && copiedPlain  !== '' ? copiedPlain  : defaultPlain,
      html:  copiedHtml   != null && copiedHtml   !== '' ? copiedHtml   : defaultHtml,
      // 也把默认的带上，便于对比
      _defaultPlain: defaultPlain,
      _defaultHtml:  defaultHtml
    };
  }catch(e){
    return { error: String(e) };
  }
}
"""

def create_chrome_driver(task_name, formatted_time, parsers):
    current_time = datetime.now()
    current_data = current_time.strftime("%Y%m%d")
    data_dir = os.path.join('/app', "ssl_key", current_data)
    os.makedirs(data_dir, exist_ok=True)
    filename = f'{parsers}_'

    ssl_key_file_path = os.path.join(data_dir, f"{filename}{formatted_time}_{task_name}_ssl_key.log")

    # 在当前目录中创建download文件夹
    download_folder = os.path.join(os.getcwd(), 'download')
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    os.environ["SE_OFFLINE"] = "true"
    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"
    _LANG_PRIMARY = "zh-CN"

    # 创建 ChromeOptions 实例
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/google-chrome"  # 固定 Chrome 路径，避免联网查询
    chrome_options.add_argument('--headless')  # 无界面模式
    chrome_options.add_argument("--disable-gpu")  # 禁用 GPU 加速
    chrome_options.add_argument("--no-sandbox")  # 禁用沙盒
    chrome_options.add_argument("--disable-dev-shm-usage")  # 限制使用/dev/shm
    chrome_options.add_argument("--incognito")  # 隐身模式
    chrome_options.add_argument("--disable-application-cache")  # 禁用应用缓存
    chrome_options.add_argument("--disable-extensions")  # 禁用扩展
    chrome_options.add_argument("--disable-infobars")  # 禁用信息栏
    chrome_options.add_argument("--disable-software-rasterizer")  # 禁用软件光栅化
    chrome_options.add_argument("--autoplay-policy=no-user-gesture-required")  # 允许自动播放
    chrome_options.add_argument(f"--lang={_LANG_PRIMARY}") # ✅ 启动语言
    chrome_options.add_argument(f"--ssl-key-log-file={ssl_key_file_path}")
    chrome_options.add_argument("--disable-background-networking")  # 降低背景“噪音”联网
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--homepage=about:blank")
    chrome_options.add_argument("--log-net-log=/tmp/netlog.json")
    chrome_options.add_argument("--net-log-capture-mode=Everything")
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
        "intl.accept_languages": _ACCEPT_LANGUAGE,  # ✅ 首选语言
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # 启用性能日志记录
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # 创建 WebDriver 实例
    service = Service(executable_path="/usr/local/bin/chromedriver")
    browser = webdriver.Chrome(service=service, options=chrome_options)
    browser.execute_cdp_cmd('Network.enable', {})
    browser.execute_cdp_cmd('Network.setExtraHTTPHeaders', {'headers': {'Accept-Language': _ACCEPT_LANGUAGE}})
    browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                            {'source': '''
                            Object.defineProperty(navigator,"webdriver",{get:()=>undefined});
                            Object.defineProperty(navigator,"language",{get:()=> "zh-CN"});
                            Object.defineProperty(navigator,"languages",{get:()=> ["zh-CN","zh"]});
                            '''.strip()})
    return browser, ssl_key_file_path

def open_url_and_save_content(driver, url, ssl_key_file_path, wait_secs=8):
    driver.get(url)
    WebDriverWait(driver, wait_secs).until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(15)
    screenshot_path = ssl_key_file_path.replace("_ssl_key.log", ".png").replace("/ssl_key/", "/screenshot/")
    if not os.path.exists(os.path.dirname(screenshot_path)):
        os.makedirs(os.path.dirname(screenshot_path))
    screenshot_full_page(driver, Path(screenshot_path), dpr=2.0)
    script = JS_SELECT_ALL_AND_COPY_CAPTURE + "\nreturn __select_all_and_copy_capture();"
    res = driver.execute_script(script)
    if not isinstance(res, dict) or res.get("error"):
        raise RuntimeError(f"JS失败: {res}")
    plain = re.sub(r'(?:[ \t\f\u00A0\u3000\u200B\u200C\u200D\uFEFF\u2060\u00AD\v]*\r?\n)+', '\n', res.get("plain", ""))
    content_path = ssl_key_file_path.replace("_ssl_key.log", ".text").replace("/ssl_key/", "/content/")
    html_path = ssl_key_file_path.replace("_ssl_key.log", ".html").replace("/ssl_key/", "/html/")
    if not os.path.exists(os.path.dirname(content_path)):
        os.makedirs(os.path.dirname(content_path))
    with open(content_path, "w", encoding="utf-8") as f:
        f.write(plain)
    html = driver.page_source  # 此刻的 DOM（包含已渲染的动态内容）
    if not os.path.exists(os.path.dirname(html_path)):
        os.makedirs(os.path.dirname(html_path))
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    return content_path, html_path, screenshot_path

def screenshot_full_page(driver: webdriver.Chrome, out_path: Path, dpr: Optional[float] = None) -> None:
    """整页长截图：通过 CDP 获取内容尺寸并原生捕获，不做滚动拼接。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 计算页面内容尺寸
    metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
    # contentSize 比 visualViewport 更可靠，含整个文档内容区域
    content_size = metrics.get("contentSize", {})
    width = int(math.ceil(content_size.get("width", 0) or 0))
    height = int(math.ceil(content_size.get("height", 0) or 0))
    if width == 0 or height == 0:
        # 退路：用 JS 获取 body 尺寸
        width = int(driver.execute_script("return Math.ceil(document.documentElement.scrollWidth||document.body.scrollWidth||0);"))
        height = int(driver.execute_script("return Math.ceil(document.documentElement.scrollHeight||document.body.scrollHeight||0);"))

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