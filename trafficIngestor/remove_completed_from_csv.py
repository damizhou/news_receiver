#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
remove_completed_from_csv.py

根据已完成的 pcap 文件，从 CSV 中剔除对应的任务记录。
pcap 文件名格式: ne8666_20260123_00_44_20_x.com.pcap
其中 ne8666 是 CSV 中的 id。
"""

import os
import csv
import argparse
from pathlib import Path


def extract_id_from_pcap(filename: str) -> str:
    """从 pcap 文件名中提取 id

    例如: ne8666_20260123_00_44_20_x.com.pcap -> ne8666
    使用 _2026 作为分隔符，因为 id 中可能包含下划线
    """
    basename = os.path.basename(filename)
    # 使用 _2026 分割，取前面的部分作为 id
    parts = basename.split('_2026')
    if parts:
        return parts[0]
    return ""


def get_completed_ids(pcap_dir: str) -> set:
    """扫描 pcap 目录，获取所有已完成任务的 id 集合"""
    completed_ids = set()
    pcap_path = Path(pcap_dir)

    if not pcap_path.exists():
        print(f"警告: pcap 目录不存在: {pcap_dir}")
        return completed_ids

    for f in pcap_path.iterdir():
        if f.is_file() and f.suffix == '.pcap':
            task_id = extract_id_from_pcap(f.name)
            if task_id:
                completed_ids.add(task_id)

    return completed_ids


def get_completed_id_counts(pcap_dir: str) -> dict:
    """扫描 pcap 目录，获取每个 id 完成的次数

    返回: {id: count} 字典
    """
    from collections import Counter
    id_counts = Counter()
    pcap_path = Path(pcap_dir)

    if not pcap_path.exists():
        print(f"警告: pcap 目录不存在: {pcap_dir}")
        return id_counts

    for f in pcap_path.iterdir():
        if f.is_file() and f.suffix == '.pcap':
            task_id = extract_id_from_pcap(f.name)
            if task_id:
                id_counts[task_id] += 1

    return id_counts


def remove_completed_from_csv(csv_path: str, completed_id_counts: dict, output_path: str = None) -> tuple:
    """从 CSV 中移除已完成的任务

    每个 pcap 文件只移除一条对应 id 的记录（而不是所有同 id 的记录）

    返回: (原始记录数, 剩余记录数, 移除记录数)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {csv_path}")

    # 读取 CSV
    with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV 文件没有表头")

        header_fields = list(reader.fieldnames)
        rows = list(reader)

    original_count = len(rows)

    # 找到 id 列名（不区分大小写）
    id_column = None
    for field in header_fields:
        if field.lower() == 'id':
            id_column = field
            break

    if id_column is None:
        raise ValueError("CSV 文件中没有 'id' 列")

    # 复制一份计数器，用于追踪每个 id 还需要移除多少条
    remaining_to_remove = dict(completed_id_counts)

    # 过滤掉已完成的任务（每个 pcap 只移除一条记录）
    remaining_rows = []
    removed_ids = []

    for row in rows:
        row_id = (row.get(id_column) or "").strip()
        # 如果这个 id 还有待移除的配额，就移除这条记录
        if remaining_to_remove.get(row_id, 0) > 0:
            removed_ids.append(row_id)
            remaining_to_remove[row_id] -= 1
        else:
            remaining_rows.append(row)

    remaining_count = len(remaining_rows)
    removed_count = len(removed_ids)

    # 统计无法移除的情况（pcap 数量超过 CSV 中该 ID 的记录数）
    unmatched_stats = {}
    for task_id, remaining in remaining_to_remove.items():
        if remaining > 0:
            unmatched_stats[task_id] = {
                'pcap_count': completed_id_counts[task_id],
                'unmatched': remaining
            }

    # 写入输出文件
    output_file = output_path or csv_path
    with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header_fields)
        writer.writeheader()
        writer.writerows(remaining_rows)

    return original_count, remaining_count, removed_count, removed_ids, unmatched_stats


def main():
    parser = argparse.ArgumentParser(
        description="根据已完成的 pcap 文件，从 CSV 中剔除对应的任务记录"
    )
    parser.add_argument(
        "--csv", "-c",
        required=True,
        help="CSV 文件路径"
    )
    parser.add_argument(
        "--pcap-dir", "-p",
        required=True,
        help="pcap 文件目录路径"
    )
    parser.add_argument(
        "--output", "-o",
        help="输出 CSV 文件路径（默认覆盖原文件）"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="仅显示将要移除的记录，不实际修改文件"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="显示详细信息"
    )

    args = parser.parse_args()

    # 获取已完成的 id 及其次数
    print(f"扫描 pcap 目录: {args.pcap_dir}")
    completed_id_counts = get_completed_id_counts(args.pcap_dir)
    total_pcap_count = sum(completed_id_counts.values())
    print(f"找到 {total_pcap_count} 个 pcap 文件，涉及 {len(completed_id_counts)} 个不同的 ID")

    if not completed_id_counts:
        print("没有找到已完成的任务，无需处理")
        return

    if args.verbose:
        print(f"已完成的 ID 及次数（前20个）:")
        for task_id, count in sorted(completed_id_counts.items())[:20]:
            print(f"  {task_id}: {count} 次")
        if len(completed_id_counts) > 20:
            print(f"  ... 还有 {len(completed_id_counts) - 20} 个")

    # 处理 CSV
    print(f"处理 CSV 文件: {args.csv}")

    if args.dry_run:
        # 仅预览，不修改
        original, remaining, removed, removed_ids, unmatched_stats = remove_completed_from_csv(
            args.csv, completed_id_counts, output_path="/dev/null"
        )
        print(f"\n[预览模式 - 不会实际修改文件]")
    else:
        original, remaining, removed, removed_ids, unmatched_stats = remove_completed_from_csv(
            args.csv, completed_id_counts, output_path=args.output
        )

    print(f"\n结果统计:")
    print(f"  原始记录数: {original}")
    print(f"  移除记录数: {removed}")
    print(f"  剩余记录数: {remaining}")

    # 显示不匹配的统计
    if unmatched_stats:
        total_unmatched = sum(s['unmatched'] for s in unmatched_stats.values())
        print(f"\n警告: 有 {total_unmatched} 个 pcap 文件在 CSV 中找不到对应记录")
        print(f"  涉及 {len(unmatched_stats)} 个不同的 ID")
        if args.verbose:
            print(f"\n  不匹配的 ID 详情（pcap数 vs 实际移除数）:")
            for task_id, stats in sorted(unmatched_stats.items())[:30]:
                actual_removed = stats['pcap_count'] - stats['unmatched']
                print(f"    {task_id}: pcap={stats['pcap_count']}, CSV中只有={actual_removed}")
            if len(unmatched_stats) > 30:
                print(f"    ... 还有 {len(unmatched_stats) - 30} 个")

    if args.verbose and removed_ids:
        print(f"\n移除的 ID 列表:")
        for rid in sorted(removed_ids)[:50]:
            print(f"  - {rid}")
        if len(removed_ids) > 50:
            print(f"  ... 还有 {len(removed_ids) - 50} 个")

    if not args.dry_run:
        output_file = args.output or args.csv
        print(f"\n已保存到: {output_file}")


if __name__ == "__main__":
    main()
