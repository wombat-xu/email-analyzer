"""全量下载脚本 - 逐账号顺序下载，自动跳过已下载"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import sqlite3
from config.settings import DB_PATH
from modules.email_fetcher import create_task, finish_task, fetch_all_emails, get_all_accounts
from modules.email_parser import process_all

accounts = get_all_accounts()
task_id = create_task(f"全量下载 {len(accounts)} 个账号（约16万封）", task_type='full_download')

total_new = 0
for i, (acc_email, acc_pwd, acc_imap, acc_name, _, _) in enumerate(accounts):
    print(f"\n{'='*50}")
    print(f"[{i+1}/{len(accounts)}] 下载 {acc_email} ({acc_name})")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
              (i, len(accounts), f"正在下载 {acc_email}（{i+1}/{len(accounts)}）", task_id))
    conn.commit()
    conn.close()

    # 最多重试3次
    for attempt in range(3):
        try:
            count = fetch_all_emails(acc_email, acc_pwd, limit_per_folder=None)
            total_new += count
            print(f"  {acc_email} 完成，新增 {count} 封")
            break
        except Exception as e:
            print(f"  第{attempt+1}次失败: {e}")
            if attempt < 2:
                import time
                time.sleep(5)
                print("  重试中...")

print(f"\n下载完成，总新增: {total_new} 封")
print("正在解析邮件...")
process_all()
finish_task(task_id, f"完成！新增 {total_new} 封")
print(f"全部完成！新增 {total_new} 封")
