"""
统一的Firefox浏览器驱动模块
"""
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
import os
import sys
import subprocess
import re
import time
from datetime import datetime

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# JavaScript代码：全选并复制页面内容
JS_SELECT_ALL_AND_COPY_CAPTURE = r"""
function __select_all_and_copy_capture(){
  try{
    const sel = window.getSelection();
    const saved = [];
    for (let i=0;i<sel.rangeCount;i++){ saved.push(sel.getRangeAt(i).cloneRange()); }
    function restore(){
      sel.removeAllRanges();
      for (const r of saved) sel.addRange(r);
    }
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

    return {
      execOk,
      plain: copiedPlain  != null && copiedPlain  !== '' ? copiedPlain  : defaultPlain,
      html:  copiedHtml   != null && copiedHtml   !== '' ? copiedHtml   : defaultHtml,
      _defaultPlain: defaultPlain,
      _defaultHtml:  defaultHtml
    };
  }catch(e){
    return { error: String(e) };
  }
}
"""


def kill_firefox_processes() -> None:
    """
    结束 Linux 上的 Firefox/GeckoDriver 进程。
    """
    patterns = ("geckodriver", "firefox-esr", "firefox")

    try:
        for p in patterns:
            subprocess.run(
                ["pkill", "-KILL", "-f", p],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception as e:
        print(f"Error occurred: {e}")


def create_firefox_driver(task_name, formatted_time, parsers, data_base_dir=None):
    """
    创建Firefox浏览器驱动

    Args:
        task_name: 任务名称
        formatted_time: 格式化的时间字符串
        parsers: 解析器名称/前缀
        data_base_dir: 数据基础目录

    Returns:
        browser: WebDriver实例
        ssl_key_file_path: SSL密钥日志文件路径
    """
    kill_firefox_processes()

    if data_base_dir is None:
        data_base_dir = _project_root

    current_time = datetime.now()
    current_data = current_time.strftime("%Y%m%d")
    ssl_key_dir = os.path.join(data_base_dir, "ssl_key", current_data)
    os.makedirs(ssl_key_dir, exist_ok=True)

    filename_prefix = f'{parsers}_' if parsers else ''
    ssl_key_file_path = os.path.join(
        ssl_key_dir,
        f"{filename_prefix}{formatted_time}_{task_name}_ssl_key.log"
    )

    # download 目录
    download_folder = os.path.join(os.getcwd(), 'download')
    os.makedirs(download_folder, exist_ok=True)

    # 环境变量设置
    os.environ["SE_OFFLINE"] = "true"
    # Firefox/NSS 的 TLS 密钥日志用环境变量 SSLKEYLOGFILE
    os.environ["SSLKEYLOGFILE"] = ssl_key_file_path

    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"

    opts = Options()
    opts.binary_location = "/usr/bin/firefox"
    opts.add_argument("-headless")
    opts.add_argument("-private")

    # --- 传输层：TLS1.2 + 禁 HTTP/3/Alt-Svc + 禁 DoH ---
    opts.set_preference("security.tls.version.min", 4)  # TLS1.3
    opts.set_preference("security.tls.version.max", 4)
    opts.set_preference("network.http.http3.enabled", False)
    opts.set_preference("network.http.altsvc.enabled", False)
    opts.set_preference("network.trr.mode", 5)  # 禁 DoH
    opts.set_preference("network.trr.uri", "")

    # --- 降噪：遥测/实验/上报 ---
    opts.set_preference("toolkit.telemetry.unified", False)
    opts.set_preference("toolkit.telemetry.enabled", False)
    opts.set_preference("toolkit.telemetry.server", "")
    opts.set_preference("toolkit.telemetry.archive.enabled", False)
    opts.set_preference("toolkit.telemetry.updatePing.enabled", False)
    opts.set_preference("toolkit.telemetry.firstShutdownPing.enabled", False)
    opts.set_preference("datareporting.healthreport.uploadEnabled", False)
    opts.set_preference("datareporting.policy.dataSubmissionEnabled", False)
    opts.set_preference("app.normandy.enabled", False)
    opts.set_preference("app.normandy.api_url", "")
    opts.set_preference("app.shield.optoutstudies.enabled", False)

    # --- 连通性/门户探测 ---
    opts.set_preference("network.connectivity-service.enabled", False)
    opts.set_preference("network.captive-portal-service.enabled", False)

    # --- 预取/预连接/预测 ---
    opts.set_preference("network.prefetch-next", False)
    opts.set_preference("network.dns.disablePrefetch", True)
    opts.set_preference("network.predictor.enabled", False)
    opts.set_preference("network.predictor.enable-prefetch", False)
    opts.set_preference("network.http.speculative-parallel-limit", 0)

    # --- Remote Settings ---
    opts.set_preference("services.settings.enabled", False)
    opts.set_preference("services.settings.server", "http://127.0.0.1:65535")
    opts.set_preference("services.settings.poll_interval", 31536000)
    opts.set_preference("security.remote_settings.crlite_filters.enabled", False)
    opts.set_preference("security.remote_settings.intermediates.enabled", False)
    opts.set_preference("services.blocklist.update_enabled", False)
    opts.set_preference("extensions.blocklist.enabled", False)

    # --- 新标签页/首页外呼 ---
    opts.set_preference("browser.newtabpage.activity-stream.feeds.system.topstories", False)
    opts.set_preference("browser.newtabpage.activity-stream.showSponsored", False)
    opts.set_preference("browser.newtabpage.activity-stream.showSponsoredTopSites", False)
    opts.set_preference("extensions.pocket.enabled", False)
    opts.set_preference("browser.newtabpage.enabled", False)
    opts.set_preference("browser.startup.page", 0)
    opts.set_preference("browser.startup.homepage", "about:blank")
    opts.set_preference("browser.shell.checkDefaultBrowser", False)

    # --- 语言 & 下载 ---
    opts.set_preference("intl.accept_languages", _ACCEPT_LANGUAGE)
    opts.set_preference("browser.download.folderList", 2)
    opts.set_preference("browser.download.dir", download_folder)
    opts.set_preference("browser.download.useDownloadDir", True)
    opts.set_preference("browser.download.manager.showWhenStarting", False)
    opts.set_preference("browser.helperApps.neverAsk.saveToDisk",
                        "application/octet-stream,application/pdf,text/plain,text/html,application/json")
    opts.set_preference("pdfjs.disabled", True)

    # 创建 WebDriver（geckodriver）
    service = Service(executable_path="/usr/local/bin/geckodriver")
    browser = webdriver.Firefox(service=service, options=opts)
    return browser, ssl_key_file_path


def open_url_and_save_content(driver, url, ssl_key_file_path, logger=None, data_base_dir=None):
    """
    打开URL并保存内容

    Args:
        driver: WebDriver实例
        url: 要访问的URL
        ssl_key_file_path: SSL密钥日志文件路径
        logger: 日志记录器
        data_base_dir: 数据基础目录

    Returns:
        content_path: 文本内容文件路径
        html_path: HTML文件路径
    """
    if data_base_dir is None:
        data_base_dir = _project_root

    driver.get(url)
    time.sleep(30)

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

    html = driver.page_source
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    if logger:
        logger.info(f"爬取数据结束, 等待10秒.让浏览器加载完所有已请求的页面")
    time.sleep(10)

    kill_firefox_processes()
    if logger:
        logger.info(f"等待TCP结束挥手完成，耗时60秒")
    time.sleep(60)

    return content_path, html_path
