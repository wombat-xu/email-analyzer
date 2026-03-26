"""增量同步脚本 - 只拉取每个账号最新的邮件（INBOX + 已发送，各500封）"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import sqlite3
import time
from config.settings import DB_PATH
from modules.email_fetcher import create_task, finish_task, fail_task, cleanup_zombie_tasks, fetch_all_emails, get_all_accounts, get_db_conn
from modules.email_parser import process_all

# 同时输出到终端和 worker.log
LOG_PATH = os.path.join(os.path.dirname(__file__), 'data', 'worker.log')
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

class TeeOutput:
    def __init__(self, log_path):
        self.terminal = sys.__stdout__
        self.log = open(log_path, 'a', encoding='utf-8')
    def write(self, msg):
        if msg:
            self.terminal.write(msg)
            self.log.write(msg)
            self.log.flush()
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = TeeOutput(LOG_PATH)
sys.stderr = sys.stdout

# 支持 --account 参数指定单个账号
target_account = None
if '--account' in sys.argv:
    idx = sys.argv.index('--account')
    target_account = sys.argv[idx + 1]

# 清理僵尸任务
cleaned = cleanup_zombie_tasks()
if cleaned:
    print(f"已清理 {cleaned} 个残留任务")

accounts = get_all_accounts()
if target_account:
    accounts = [a for a in accounts if a[0] == target_account]
    if not accounts:
        print(f"未找到账号: {target_account}")
        sys.exit(1)

desc = f"增量同步 {accounts[0][0]}" if target_account else f"增量同步 {len(accounts)} 个账号"
task_id = create_task(desc, task_type='incremental_sync')

total_new = 0
try:
    for i, (acc_email, acc_pwd, acc_imap, acc_name, _, _) in enumerate(accounts):
        print(f"\n{'='*50}")
        print(f"[{i+1}/{len(accounts)}] 增量同步 {acc_email} ({acc_name})")

        conn = get_db_conn()
        conn.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                     (i, len(accounts), f"正在同步 {acc_email}（{i+1}/{len(accounts)}）", task_id))
        conn.commit()
        conn.close()

        for attempt in range(3):
            try:
                # 增量：每个文件夹只拉取最新 500 封
                count = fetch_all_emails(acc_email, acc_pwd, limit_per_folder=500, task_id=task_id)
                total_new += count
                print(f"  {acc_email} 完成，新增 {count} 封")
                break
            except Exception as e:
                print(f"  第{attempt+1}次失败: {e}")
                if attempt < 2:
                    time.sleep(10)
                    print("  重试中...")

    print(f"\n增量同步完成，总新增: {total_new} 封")
    if total_new > 0:
        print("正在解析邮件...")
        process_all()
    finish_task(task_id, f"完成！新增 {total_new} 封")
    print(f"全部完成！新增 {total_new} 封")

except KeyboardInterrupt:
    print(f"\n用户中断，已同步 {total_new} 封")
    fail_task(task_id, f"用户中断，已同步 {total_new} 封")
except Exception as e:
    print(f"\n脚本异常: {e}")
    fail_task(task_id, f"异常: {str(e)[:200]}")
