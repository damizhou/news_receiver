#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_csv.py

将 collected_request_urls_temp.csv 中新的 domain 数据合并到 collected_request_urls_all.csv 中。
- 只合并 temp 中存在但 all 中不存在的 domain
- 已存在的 domain 跳过，不做任何处理
"""

import csv
from pathlib import Path
from typing import Set, List, Dict
from datetime import datetime
import shutil

# ============== 配置 ==============
ALL_CSV_PATH = '/home/pcz/code/news_receiver/trafficIngestor/collected_request_urls_all.csv'
TEMP_CSV_PATH = '/home/pcz/code/news_receiver/trafficIngestor/collected_request_urls_temp.csv'
# =================================


def read_csv_data(csv_path: str) -> tuple[List[Dict], Set[str], List[str]]:
    """
    读取 CSV 文件数据

    Returns:
        (rows, domains_set, header)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"错误: 文件不存在: {csv_path}")
        return [], set(), []

    rows = []
    domains = set()

    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames

        for row in reader:
            # 处理可能的拼写错误: domin vs domain
            domain = row.get('domain') or row.get('domin', '')
            domain = domain.strip()

            if domain:
                domains.add(domain)
                # 标准化 row，确保使用 'domain' 作为键
                normalized_row = {
                    'id': row.get('id', '').strip(),
                    'url': row.get('url', '').strip(),
                    'domain': domain
                }
                rows.append(normalized_row)

    return rows, domains, header


def merge_csv_files(all_path: str, temp_path: str, new_path: str = None, backup: bool = True) -> Dict:
    """
    合并 CSV 文件

    Args:
        all_path: 主 CSV 文件路径
        temp_path: 临时 CSV 文件路径（要合并的新数据）
        new_path: 新增 domain 数据输出文件路径
        backup: 是否备份原文件

    Returns:
        合并结果统计
    """
    result = {
        "all_original_rows": 0,
        "all_original_domains": 0,
        "temp_rows": 0,
        "temp_domains": 0,
        "new_domains": [],
        "skipped_domains": [],
        "added_rows": 0,
        "final_rows": 0,
        "final_domains": 0,
        "new_csv_path": None
    }

    # 读取主文件
    all_rows, all_domains, all_header = read_csv_data(all_path)
    result["all_original_rows"] = len(all_rows)
    result["all_original_domains"] = len(all_domains)

    print(f"主文件 (all): {len(all_rows)} 行, {len(all_domains)} 个 domain")

    # 读取临时文件
    temp_rows, temp_domains, temp_header = read_csv_data(temp_path)
    result["temp_rows"] = len(temp_rows)
    result["temp_domains"] = len(temp_domains)

    print(f"临时文件 (temp): {len(temp_rows)} 行, {len(temp_domains)} 个 domain")

    # 找出新的 domain（temp 中有但 all 中没有的）
    new_domains = temp_domains - all_domains
    skipped_domains = temp_domains & all_domains

    result["new_domains"] = sorted(list(new_domains))
    result["skipped_domains"] = sorted(list(skipped_domains))

    print(f"\n新增 domain: {len(new_domains)} 个")
    print(f"跳过 domain (已存在): {len(skipped_domains)} 个")

    if not new_domains:
        print("\n没有新的 domain 需要添加")
        return result

    # 打印新增的 domain
    print(f"\n将添加以下 domain:")
    for domain in sorted(new_domains):
        print(f"  + {domain}")

    # 过滤出新 domain 对应的行
    new_rows = [row for row in temp_rows if row['domain'] in new_domains]
    result["added_rows"] = len(new_rows)

    print(f"\n将添加 {len(new_rows)} 行数据")

    # 备份原文件
    if backup:
        backup_path = all_path + f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        shutil.copy2(all_path, backup_path)
        print(f"\n已备份原文件: {backup_path}")

    # 合并数据
    merged_rows = all_rows + new_rows
    result["final_rows"] = len(merged_rows)
    result["final_domains"] = len(all_domains | new_domains)

    # 写入合并后的数据
    header = ['id', 'url', 'domain']
    with open(all_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(merged_rows)

    # 将新增的 domain 数据写入单独的 CSV 文件
    if new_path is None:
        new_path = all_path.replace('.csv', f'_new_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

    with open(new_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(new_rows)

    result["new_csv_path"] = new_path

    print(f"\n合并完成!")
    print(f"  原始: {result['all_original_rows']} 行, {result['all_original_domains']} 个 domain")
    print(f"  新增: {result['added_rows']} 行, {len(new_domains)} 个 domain")
    print(f"  最终: {result['final_rows']} 行, {result['final_domains']} 个 domain")
    print(f"\n新增数据已写入: {new_path}")

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description='合并 CSV 文件，添加新的 domain 数据')
    parser.add_argument('--all', '-a', default=ALL_CSV_PATH,
                        help=f'主 CSV 文件路径 (默认: {ALL_CSV_PATH})')
    parser.add_argument('--temp', '-t', default=TEMP_CSV_PATH,
                        help=f'临时 CSV 文件路径 (默认: {TEMP_CSV_PATH})')
    parser.add_argument('--new', '-o', default=None,
                        help='新增 domain 数据输出文件路径 (默认: 自动生成带时间戳的文件名)')
    parser.add_argument('--no-backup', action='store_true',
                        help='不备份原文件')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='只显示将要做的操作，不实际执行')

    args = parser.parse_args()

    print("="*80)
    print("CSV 合并工具")
    print("="*80)
    print(f"主文件: {args.all}")
    print(f"临时文件: {args.temp}")
    if args.new:
        print(f"新增数据输出: {args.new}")
    print("="*80 + "\n")

    if args.dry_run:
        print("[DRY RUN 模式 - 不会实际修改文件]\n")

        # 只读取并显示统计信息
        all_rows, all_domains, _ = read_csv_data(args.all)
        temp_rows, temp_domains, _ = read_csv_data(args.temp)

        new_domains = temp_domains - all_domains
        skipped_domains = temp_domains & all_domains

        print(f"主文件: {len(all_rows)} 行, {len(all_domains)} 个 domain")
        print(f"临时文件: {len(temp_rows)} 行, {len(temp_domains)} 个 domain")
        print(f"\n新增 domain: {len(new_domains)} 个")
        print(f"跳过 domain: {len(skipped_domains)} 个")

        if new_domains:
            new_rows = [row for row in temp_rows if row['domain'] in new_domains]
            print(f"\n将添加以下 domain ({len(new_rows)} 行数据):")
            for domain in sorted(new_domains):
                print(f"  + {domain}")

        if skipped_domains:
            print(f"\n将跳过以下 domain (已存在):")
            for domain in sorted(skipped_domains):
                print(f"  - {domain}")
    else:
        merge_csv_files(
            all_path=args.all,
            temp_path=args.temp,
            new_path=args.new,
            backup=not args.no_backup
        )


if __name__ == "__main__":
    main()
