#!/usr/bin/env python3
"""
脚本功能：
1. 检查 /netdisk/dataset/ablation_study/batch/ 下所有类别中 pcap 文件夹的总体积
2. 保留体积最大的前100个类别
3. 将剩余类别移动到 /netdisk/dataset/ablation_study/more/batch
4. 同时将对应的 single 目录下的文件夹也移动到 /netdisk/dataset/ablation_study/more/single
"""

import os
import shutil
from pathlib import Path

# 源目录
BATCH_SRC = "/netdisk/dataset/ablation_study/batch"
SINGLE_SRC = "/netdisk/dataset/ablation_study/single"

# 目标目录
BATCH_DST = "/netdisk/dataset/ablation_study/more/batch"
SINGLE_DST = "/netdisk/dataset/ablation_study/more/single"

# 保留的类别数量
TOP_N = 100


def get_dir_size(path: Path) -> int:
    """计算目录总大小（字节）"""
    total = 0
    try:
        for entry in path.rglob('*'):
            if entry.is_file():
                total += entry.stat().st_size
    except (PermissionError, OSError) as e:
        print(f"警告: 无法访问 {path}: {e}")
    return total


def get_pcap_size(category_path: Path) -> int:
    """获取类别下 pcap 文件夹的大小"""
    pcap_path = category_path / "pcap"
    if pcap_path.exists() and pcap_path.is_dir():
        return get_dir_size(pcap_path)
    return 0


def main():
    batch_path = Path(BATCH_SRC)
    single_path = Path(SINGLE_SRC)

    # 检查源目录是否存在
    if not batch_path.exists():
        print(f"错误: 源目录不存在 {BATCH_SRC}")
        return

    # 创建目标目录
    os.makedirs(BATCH_DST, exist_ok=True)
    os.makedirs(SINGLE_DST, exist_ok=True)

    # 获取所有类别
    categories = [d for d in batch_path.iterdir() if d.is_dir()]
    print(f"找到 {len(categories)} 个类别")

    # 计算每个类别的 pcap 大小
    print("正在计算各类别 pcap 文件夹大小...")
    category_sizes = []
    for i, cat in enumerate(categories):
        size = get_pcap_size(cat)
        category_sizes.append((cat.name, size))
        if (i + 1) % 100 == 0:
            print(f"已处理 {i + 1}/{len(categories)} 个类别")

    # 按大小降序排序
    category_sizes.sort(key=lambda x: x[1], reverse=True)

    # 打印前100个类别的信息
    print("\n体积最大的前100个类别:")
    print("-" * 60)
    for i, (name, size) in enumerate(category_sizes[:TOP_N]):
        size_mb = size / (1024 * 1024)
        print(f"{i+1:3d}. {name}: {size_mb:.2f} MB")

    # 获取需要保留和移动的类别
    top_categories = set(name for name, _ in category_sizes[:TOP_N])
    categories_to_move = [name for name, _ in category_sizes[TOP_N:]]

    print(f"\n需要移动的类别数量: {len(categories_to_move)}")

    if len(categories_to_move) == 0:
        print("没有需要移动的类别")
        return

    # 确认是否继续
    confirm = input("\n是否继续移动？(y/n): ").strip().lower()
    if confirm != 'y':
        print("操作已取消")
        return

    # 移动类别
    moved_count = 0
    for cat_name in categories_to_move:
        # 移动 batch 目录
        batch_src_cat = batch_path / cat_name
        batch_dst_cat = Path(BATCH_DST) / cat_name

        if batch_src_cat.exists():
            print(f"移动 batch: {cat_name}")
            shutil.move(str(batch_src_cat), str(batch_dst_cat))

        # 移动 single 目录（如果存在）
        single_src_cat = single_path / cat_name
        single_dst_cat = Path(SINGLE_DST) / cat_name

        if single_src_cat.exists():
            print(f"移动 single: {cat_name}")
            shutil.move(str(single_src_cat), str(single_dst_cat))

        moved_count += 1
        if moved_count % 100 == 0:
            print(f"已移动 {moved_count}/{len(categories_to_move)} 个类别")

    print(f"\n完成! 共移动 {moved_count} 个类别")
    print(f"保留在原位置的类别: {len(top_categories)}")


if __name__ == "__main__":
    main()
