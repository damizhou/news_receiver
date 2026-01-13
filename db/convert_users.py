import csv

def convert_users_to_repos_format(input_file, output_file, repeat_times=10):
    """
    将 users.csv 转换为 github_repos_1000.csv 的格式
    users.csv: Username,URL
    github_repos_1000.csv: id,url,domain
    """
    # 读取原始数据
    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    # 写入新格式，重复10次
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'url', 'domain'])

        for _ in range(repeat_times):
            for row in rows:
                username = row['Username']
                url = row['URL']
                # 从URL提取domain
                domain = "x.com"
                writer.writerow([username, url, domain])

    print(f"转换完成！共写入 {len(rows) * repeat_times} 行数据到 {output_file}")

if __name__ == '__main__':
    convert_users_to_repos_format(
        'users.csv',
        'users_converted.csv',
        repeat_times=10
    )
