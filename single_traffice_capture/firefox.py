from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
import os
from datetime import datetime
import subprocess
import re
import time

from logger import logger

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
    force=False -> SIGTERM；force=True -> SIGKILL
    """
    patterns = ("geckodriver", "firefox-esr", "firefox")

    try:
        for p in patterns:
            subprocess.run(["pkill", "-KILL", "-f", p],check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,)
    except Exception as e:
        print(f"Error occurred: {e}")

def create_firefox_driver(task_name, formatted_time, parsers):
    kill_firefox_processes()
    current_time = datetime.now()
    current_data = current_time.strftime("%Y%m%d")
    data_dir = os.path.join('/app', "ssl_key", current_data)
    os.makedirs(data_dir, exist_ok=True)
    filename = f'{parsers}_'
    ssl_key_file_path = os.path.join(data_dir, f"{filename}{formatted_time}_{task_name}_ssl_key.log")

    # download 目录（与原版一致）
    download_folder = os.path.join(os.getcwd(), 'download')
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # 与原版一致的环境变量
    os.environ["SE_OFFLINE"] = "true"

    # Firefox/NSS 的 TLS 密钥日志用环境变量 SSLKEYLOGFILE（与 Chrome 的 --ssl-key-log-file 不同）
    os.environ["SSLKEYLOGFILE"] = ssl_key_file_path

    _ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"
    _LANG_PRIMARY = "zh-CN"

    opts = Options()
    opts.binary_location = "/usr/bin/firefox"
    opts.add_argument("-headless")
    opts.add_argument("-private")

    # --- 传输层：TLS1.2 + 禁 HTTP/3/Alt-Svc + 禁 DoH ---
    opts.set_preference("security.tls.version.min", 4)  # 3=TLS1.2; 4=TLS1.3
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

    # --- Remote Settings 及其附件（关键，避免访问 firefox.settings/services + CDN）---
    opts.set_preference("services.settings.enabled", False)
    opts.set_preference("services.settings.server", "http://127.0.0.1:65535")  # 黑洞，不回退默认
    opts.set_preference("services.settings.poll_interval", 31536000)
    opts.set_preference("security.remote_settings.crlite_filters.enabled", False)
    opts.set_preference("security.remote_settings.intermediates.enabled", False)
    opts.set_preference("services.blocklist.update_enabled", False)  # 可接受轻微安全性下降
    opts.set_preference("extensions.blocklist.enabled", False)

    # --- 新标签页/首页外呼 ---
    opts.set_preference("browser.newtabpage.activity-stream.feeds.system.topstories", False)
    opts.set_preference("browser.newtabpage.activity-stream.showSponsored", False)
    opts.set_preference("browser.newtabpage.activity-stream.showSponsoredTopSites", False)
    opts.set_preference("extensions.pocket.enabled", False)
    opts.set_preference("browser.newtabpage.enabled", False)
    opts.set_preference("browser.startup.page", 0)  # about:blank
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

def open_url_and_save_content(driver, url, ssl_key_file_path):
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

    html = driver.page_source  # 渲染后的 DOM
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"爬取数据结束, 等待10秒.让浏览器加载完所有已请求的页面")
    time.sleep(10)

    kill_firefox_processes()
    logger.info(f"等待TCP结束挥手完成，耗时60秒")
    time.sleep(60)
    return content_path, html_path
