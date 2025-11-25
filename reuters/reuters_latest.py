#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
路透社采用了反爬机制，目前的使用docker的方式无法解决这个问题。
获取的页面数据
<html lang="zh"><head><title>reuters.com</title><style>#cmsg{animation: A 1.5s;}@keyframes A{0%{opacity:0;}99%{opacity:0;}100%{opacity:1;}}</style><meta name="viewport" content="width=device-width, initial-scale=1.0"></head><body style="margin:0"><script data-cfasync="false">var dd={'rt':'i','cid':'AHrlqAAAAAMA_Pc2D5Crkr8A8TuNZA==','hsh':'2013457ADA70C67D6A4123E0A76873','b':1637,'s':46743,'e':'74f2c51dd2edc81f253c337eb33eb6010b47559dc3bccb4bbf7e93d10a0141df37538353c801e569dba2abceff5da361','qp':'','host':'geo.captcha-delivery.com','cookie':'wtNlssJy3LcIoxqis7jdMv_W6aCBPQ~qQvGbZD821SH6LZXowBid8FlzJr73rMAQt2c~vZhYrahYTFS5gyPdJB1~qXIPXqnt14uf02h4FUdB0V_yKvlGKPPJJ0n7fvy5'}</script><script data-cfasync="false" src="https://ct.captcha-delivery.com/i.js"></script><iframe src="https://geo.captcha-delivery.com/interstitial/?initialCid=AHrlqAAAAAMA_Pc2D5Crkr8A8TuNZA%3D%3D&amp;hash=2013457ADA70C67D6A4123E0A76873&amp;cid=wtNlssJy3LcIoxqis7jdMv_W6aCBPQ~qQvGbZD821SH6LZXowBid8FlzJr73rMAQt2c~vZhYrahYTFS5gyPdJB1~qXIPXqnt14uf02h4FUdB0V_yKvlGKPPJJ0n7fvy5&amp;referer=https%3A%2F%2Fwww.reuters.com%2Fworld%2Fchina%2Fchinas-new-home-prices-fall-fastest-pace-11-months-2025-10-20%2F&amp;s=46743&amp;b=1637&amp;dm=cd" sandbox="allow-scripts allow-same-origin allow-forms" allow="accelerometer; gyroscope; magnetometer" title="DataDome Device Check" width="100%" height="100%" style="height:100vh;" frameborder="0" border="0" scrolling="yes"></iframe></body></html>
docker run --volume ~/news_receiver:/app -e HOST_UID=$(id -u $USER) -e HOST_GID=$(id -g $USER) --privileged -itd --name news_receiver chuanzhoupan/trace_spider:250912 /bin/bash
docker exec -it news_receiver /bin/bash
"""
import json
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # 把 /app 加进来
from tools.chrome import create_chrome_driver, kill_chrome_processes, add_cookies
# ========== 主流程 ==========
def main():
    kill_chrome_processes()
    chrome = create_chrome_driver()
    base_url = 'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-section-alias-or-id-v1?query={"arc-site":"reuters","fetch_type":"collection",'
    extra_parameters = '"section_id":"/world/china/","size":"20","uri":"/world/china/","website":"reuters"}&d=324&mxId=00000000&_website=reuters'
    blank_count = 0
    index = 0
    while True:
        try:
            # url = base_url + f'"offset":{index * 20},"requestId":{index + 1},' + extra_parameters
            url = 'https://www.reuters.com/world/china/chinas-new-home-prices-fall-fastest-pace-11-months-2025-10-20/'
            print(f"访问: {url}")
            chrome.get(url)
            index += 1
            print('chrome.page_source', chrome.page_source)
            data = json.loads(chrome.page_source)
            result = data.get('result')
            articles = result.get('articles')
            if not articles:
                blank_count += 1
                if blank_count >=3:
                    print("连续3页无数据，结束抓取")
                    break
                continue
            else:
                for article in articles:
                    print(json.dumps(article, ensure_ascii=False))
        except Exception as e:
            print(f"访问出错，重试: {e}")

        break

if __name__ == "__main__":
    main()
