import csv
import re
from urllib.parse import urlparse

def extract_domain(url):
    """从URL中提取域名"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        # 移除www.前缀
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ''

def main():
    input_file = 'github_repose_10w.csv'
    output_file = 'github_repos_1000.csv'

    rows = []

    # 读取源文件（制表符分隔）
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for i, row in enumerate(reader):
            if i >= 1000:
                break

            id_val = row['id']
            url = row['html_url']
            domain = extract_domain(url)

            rows.append({
                'id': id_val,
                'url': url,
                'domain': domain
            })

    # 写入新文件（逗号分隔，与missing_pcap.csv格式一致）
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'url', 'domain'])
        writer.writeheader()
        writer.writerows(rows)

    print(f'成功从 {input_file} 提取 {len(rows)} 条数据到 {output_file}')

if __name__ == '__main__':
    main()
