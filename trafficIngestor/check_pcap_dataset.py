#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_pcap_dataset.py

检查 /netdisk/dataset/ablation_study/single/ 下的 pcap 文件数量是否达标。

规则：
- 每个 domain 目录下应有 10 个不同的 URL（以文件名第一个 _ 前的数字区分）
- 每个 URL 应有 100 个以上的 pcap 副本（不同时间戳）

文件命名格式：{url_id}_{timestamp}_{domain}.pcap
例如：1_20251220_22_11_34_numerade.com.pcap
"""

import os
import re
import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Set
import argparse

# ============== 配置 ==============
BASE_PATH = '/netdisk/dataset/ablation_study/single'
CSV_PATH = '/home/pcz/code/news_receiver/trafficIngestor/collected_request_urls_all.csv'
EXPECTED_URL_COUNT = 10        # 每个 domain 期望的 URL 数量
EXPECTED_COPY_COUNT = 100      # 每个 URL 期望的最小副本数量
# =================================


def parse_pcap_filename(filename: str) -> Tuple[str, str, str]:
    """
    解析 pcap 文件名，提取 url_id、timestamp 和 domain

    Args:
        filename: pcap 文件名，如 1_20251220_22_11_34_numerade.com.pcap

    Returns:
        (url_id, timestamp, domain) 或 (None, None, None) 如果解析失败
    """
    if not filename.endswith('.pcap'):
        return None, None, None

    # 去掉 .pcap 后缀
    name = filename[:-5]

    # 按第一个 _ 分割获取 url_id
    parts = name.split('_', 1)
    if len(parts) < 2:
        return None, None, None

    url_id = parts[0]
    rest = parts[1]

    # 尝试提取 timestamp (格式: YYYYMMDD_HH_MM_SS)
    # 剩余部分格式: 20251220_22_11_34_numerade.com
    timestamp_pattern = r'^(\d{8}_\d{2}_\d{2}_\d{2})_(.+)$'
    match = re.match(timestamp_pattern, rest)

    if match:
        timestamp = match.group(1)
        domain = match.group(2)
        return url_id, timestamp, domain

    return url_id, None, rest


def scan_domain_pcaps(domain_path: Path) -> Dict[str, List[str]]:
    """
    扫描单个 domain 目录下的 pcap 文件

    Returns:
        {url_id: [pcap_filenames...]}
    """
    pcap_dir = domain_path / 'pcap'
    if not pcap_dir.exists():
        return {}

    url_copies = defaultdict(list)

    for f in pcap_dir.iterdir():
        if f.is_file() and f.name.endswith('.pcap'):
            url_id, timestamp, _ = parse_pcap_filename(f.name)
            if url_id:
                url_copies[url_id].append(f.name)

    return dict(url_copies)


def check_dataset(base_path: str, expected_urls: int = 10, expected_copies: int = 100,
                  verbose: bool = True) -> Dict:
    """
    检查数据集完整性

    Returns:
        检查结果汇总
    """
    base = Path(base_path)
    if not base.exists():
        print(f"错误: 基础路径不存在: {base_path}")
        return {"error": "path_not_found"}

    results = {
        "total_domains": 0,
        "qualified_domains": 0,
        "total_urls": 0,
        "qualified_urls": 0,
        "total_pcaps": 0,
        "domain_details": {},
        "issues": []
    }

    # 遍历所有 domain 目录
    domains = sorted([d for d in base.iterdir() if d.is_dir()])
    results["total_domains"] = len(domains)

    print(f"\n{'='*80}")
    print(f"检查路径: {base_path}")
    print(f"期望每个 domain 有 {expected_urls} 个 URL")
    print(f"期望每个 URL 有 {expected_copies}+ 个副本")
    print(f"{'='*80}\n")

    for domain_path in domains:
        domain_name = domain_path.name
        url_copies = scan_domain_pcaps(domain_path)

        url_count = len(url_copies)
        total_copies = sum(len(copies) for copies in url_copies.values())
        results["total_urls"] += url_count
        results["total_pcaps"] += total_copies

        # 检查每个 URL 的副本数
        url_status = {}
        qualified_url_count = 0

        for url_id in sorted(url_copies.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
            copies = url_copies[url_id]
            copy_count = len(copies)
            is_qualified = copy_count >= expected_copies

            if is_qualified:
                qualified_url_count += 1
                results["qualified_urls"] += 1

            url_status[url_id] = {
                "count": copy_count,
                "qualified": is_qualified
            }

        # 判断 domain 是否达标
        domain_qualified = (url_count >= expected_urls and
                           qualified_url_count >= expected_urls)

        if domain_qualified:
            results["qualified_domains"] += 1

        results["domain_details"][domain_name] = {
            "url_count": url_count,
            "total_pcaps": total_copies,
            "qualified_urls": qualified_url_count,
            "qualified": domain_qualified,
            "urls": url_status
        }

        # 记录问题
        if url_count < expected_urls:
            results["issues"].append({
                "domain": domain_name,
                "type": "insufficient_urls",
                "expected": expected_urls,
                "actual": url_count
            })

        for url_id, status in url_status.items():
            if not status["qualified"]:
                results["issues"].append({
                    "domain": domain_name,
                    "url_id": url_id,
                    "type": "insufficient_copies",
                    "expected": expected_copies,
                    "actual": status["count"]
                })

        # 打印详情
        if verbose:
            status_icon = "✓" if domain_qualified else "✗"
            print(f"\n[{status_icon}] {domain_name}")
            print(f"    URL 数量: {url_count}/{expected_urls}")
            print(f"    总 pcap 数: {total_copies}")

            # 打印每个 URL 的情况
            for url_id in sorted(url_status.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
                status = url_status[url_id]
                url_icon = "✓" if status["qualified"] else "✗"
                count = status["count"]
                shortfall = max(0, expected_copies - count)

                if shortfall > 0:
                    print(f"      [{url_icon}] URL {url_id}: {count} 个 (缺 {shortfall})")
                else:
                    print(f"      [{url_icon}] URL {url_id}: {count} 个")

    return results


def print_summary(results: Dict, expected_urls: int, expected_copies: int):
    """打印检查结果汇总"""
    print(f"\n{'='*80}")
    print("检查结果汇总")
    print(f"{'='*80}")

    total_domains = results["total_domains"]
    qualified_domains = results["qualified_domains"]
    total_urls = results["total_urls"]
    qualified_urls = results["qualified_urls"]
    total_pcaps = results["total_pcaps"]

    print(f"\n总体统计:")
    print(f"  - Domain 数量: {total_domains}")
    print(f"  - 达标 Domain: {qualified_domains}/{total_domains} ({qualified_domains/max(total_domains,1)*100:.1f}%)")
    print(f"  - URL 总数: {total_urls}")
    print(f"  - 达标 URL: {qualified_urls}/{total_urls} ({qualified_urls/max(total_urls,1)*100:.1f}%)")
    print(f"  - PCAP 总数: {total_pcaps}")

    # 分类统计问题
    insufficient_urls = [i for i in results["issues"] if i["type"] == "insufficient_urls"]
    insufficient_copies = [i for i in results["issues"] if i["type"] == "insufficient_copies"]

    if insufficient_urls:
        print(f"\n[问题] URL 数量不足的 Domain ({len(insufficient_urls)} 个):")
        for issue in insufficient_urls:
            print(f"  - {issue['domain']}: {issue['actual']}/{issue['expected']} 个 URL")

    if insufficient_copies:
        print(f"\n[问题] 副本数量不足的 URL ({len(insufficient_copies)} 个):")
        # 按 domain 分组显示
        by_domain = defaultdict(list)
        for issue in insufficient_copies:
            by_domain[issue['domain']].append(issue)

        for domain, issues in sorted(by_domain.items()):
            print(f"  {domain}:")
            for issue in sorted(issues, key=lambda x: int(x['url_id']) if x['url_id'].isdigit() else float('inf')):
                shortfall = issue['expected'] - issue['actual']
                print(f"    - URL {issue['url_id']}: {issue['actual']}/{issue['expected']} (缺 {shortfall})")

    if not results["issues"]:
        print(f"\n✓ 所有数据均已达标！")

    print(f"\n{'='*80}")


def export_report(results: Dict, output_path: str):
    """导出详细报告到文件"""
    import json
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n详细报告已导出: {output_path}")


def get_unqualified_domains(results: Dict) -> Set[str]:
    """获取所有不合格的 domain 列表"""
    unqualified = set()
    for domain, details in results.get("domain_details", {}).items():
        if not details.get("qualified", False):
            unqualified.add(domain)
    return unqualified


def remove_domains_from_csv(csv_path: str, domains_to_remove: Set[str],
                            backup: bool = True) -> Tuple[int, int]:
    """
    从 CSV 文件中删除指定 domain 的数据

    Args:
        csv_path: CSV 文件路径
        domains_to_remove: 要删除的 domain 集合
        backup: 是否备份原文件

    Returns:
        (原始行数, 删除后行数)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"错误: CSV 文件不存在: {csv_path}")
        return 0, 0

    # 读取所有数据
    rows = []
    header = None
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        for row in reader:
            rows.append(row)

    original_count = len(rows)

    # 过滤掉要删除的 domain
    filtered_rows = [
        row for row in rows
        if row.get('domain', '').strip() not in domains_to_remove
    ]

    removed_count = original_count - len(filtered_rows)

    if removed_count == 0:
        print(f"没有需要删除的数据")
        return original_count, original_count

    # 备份原文件
    if backup:
        import shutil
        from datetime import datetime
        backup_path = csv_path + f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        shutil.copy2(csv_path, backup_path)
        print(f"已备份原文件: {backup_path}")

    # 写入过滤后的数据
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(filtered_rows)

    print(f"已从 CSV 删除 {removed_count} 行数据 (涉及 {len(domains_to_remove)} 个 domain)")
    print(f"原始行数: {original_count}, 删除后行数: {len(filtered_rows)}")

    return original_count, len(filtered_rows)


def remove_domain_folders(base_path: str, domains_to_remove: Set[str]) -> int:
    """
    删除指定 domain 对应的文件夹

    Args:
        base_path: 数据集根路径
        domains_to_remove: 要删除的 domain 集合

    Returns:
        成功删除的文件夹数量
    """
    import shutil

    base = Path(base_path)
    if not base.exists():
        print(f"错误: 数据集路径不存在: {base_path}")
        return 0

    deleted_count = 0
    failed = []

    for domain in sorted(domains_to_remove):
        domain_path = base / domain
        if domain_path.exists() and domain_path.is_dir():
            try:
                shutil.rmtree(domain_path)
                print(f"  已删除: {domain_path}")
                deleted_count += 1
            except Exception as e:
                print(f"  删除失败: {domain_path} -> {e}")
                failed.append(domain)
        else:
            print(f"  跳过(不存在): {domain_path}")

    print(f"\n成功删除 {deleted_count} 个文件夹")
    if failed:
        print(f"删除失败: {failed}")

    return deleted_count


def main():
    parser = argparse.ArgumentParser(description='检查 pcap 数据集完整性')
    parser.add_argument('--path', '-p', default=BASE_PATH,
                        help=f'数据集根路径 (默认: {BASE_PATH})')
    parser.add_argument('--urls', '-u', type=int, default=EXPECTED_URL_COUNT,
                        help=f'每个 domain 期望的 URL 数量 (默认: {EXPECTED_URL_COUNT})')
    parser.add_argument('--copies', '-c', type=int, default=EXPECTED_COPY_COUNT,
                        help=f'每个 URL 期望的最小副本数量 (默认: {EXPECTED_COPY_COUNT})')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='静默模式，只显示汇总')
    parser.add_argument('--export', '-e', type=str, default=None,
                        help='导出详细报告到 JSON 文件')
    parser.add_argument('--csv', type=str, default=CSV_PATH,
                        help=f'CSV 文件路径 (默认: {CSV_PATH})')
    parser.add_argument('--remove-unqualified', '-r', action='store_true',
                        help='从 CSV 和数据集中删除不合格的 domain')
    parser.add_argument('--no-backup', action='store_true',
                        help='删除时不备份原 CSV 文件')

    args = parser.parse_args()

    results = check_dataset(
        base_path=args.path,
        expected_urls=args.urls,
        expected_copies=args.copies,
        verbose=not args.quiet
    )

    if "error" not in results:
        print_summary(results, args.urls, args.copies)

        if args.export:
            export_report(results, args.export)

        # 处理删除不合格 domain 的逻辑
        if args.remove_unqualified:
            unqualified = get_unqualified_domains(results)
            if unqualified:
                print(f"\n{'='*80}")
                print(f"准备删除以下 {len(unqualified)} 个不合格的 domain:")
                print(f"{'='*80}")
                for domain in sorted(unqualified):
                    print(f"  - {domain}")

                # 1. 删除数据集中的文件夹
                print(f"\n[1/2] 删除数据集文件夹...")
                remove_domain_folders(
                    base_path=args.path,
                    domains_to_remove=unqualified
                )

                # 2. 从 CSV 中删除对应数据
                print(f"\n[2/2] 从 CSV 中删除对应数据...")
                remove_domains_from_csv(
                    csv_path=args.csv,
                    domains_to_remove=unqualified,
                    backup=not args.no_backup
                )

                print(f"\n{'='*80}")
                print("清理完成!")
                print(f"{'='*80}")
            else:
                print("\n所有 domain 均已达标，无需删除")


if __name__ == "__main__":
    main()
