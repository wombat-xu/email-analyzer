"""后台任务执行器 - 独立进程运行，不受网页刷新影响"""
import sys
import os
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.settings import DB_PATH
from modules.email_fetcher import (
    fetch_customer_emails, get_all_accounts, create_task, finish_task, fail_task
)
from modules.email_parser import process_all
from modules.ai_analyzer import (
    analyze_customer, analyze_customer_group, init_analysis_tables,
    find_related_emails_by_keyword
)


def run_fetch_and_analyze(customer_emails, do_analyze=True, merge_keyword=None):
    """后台拉取指定客户邮箱的全部邮件，然后AI合并分析
    customer_emails: 客户邮箱地址列表
    merge_keyword: 如果提供，将所有邮箱合并为一个客户进行分析
    """
    accounts = get_all_accounts()
    if not accounts:
        print("错误：没有配置任何邮箱账号")
        return

    emails_str = ", ".join(customer_emails)
    desc = f"拉取并合并分析「{merge_keyword or emails_str[:40]}」({len(customer_emails)}个邮箱)"
    task_id = create_task(desc, task_type='fetch_analyze')

    try:
        # 去重搜索词：多个邮箱可能有相同的域名/前缀
        all_search_keywords = set()
        for ce in customer_emails:
            if '@' in ce:
                all_search_keywords.add(ce.split('@')[1])
                prefix = ce.split('@')[0]
                if len(prefix) >= 4:
                    all_search_keywords.add(prefix)
        if merge_keyword:
            all_search_keywords.add(merge_keyword)

        # 只需要用第一个邮箱拉取（因为搜索关键词会覆盖所有相关邮箱）
        # 但每个账号都要搜
        total_steps = len(accounts) + 1  # 拉取 + 分析
        current_step = 0

        # 第一阶段：拉取邮件
        total_new = 0
        primary_email = customer_emails[0]

        for acc_email, acc_pwd, acc_imap, acc_name, _, _ in accounts:
            current_step += 1
            progress_text = f"[{current_step}/{total_steps}] 从 {acc_email} 搜索「{merge_keyword or primary_email}」的所有邮件..."
            print(progress_text)

            conn = sqlite3.connect(DB_PATH, timeout=30)
            cursor = conn.cursor()
            cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                           (current_step, total_steps, progress_text, task_id))
            conn.commit()
            conn.close()

            try:
                new_count = fetch_customer_emails(
                    acc_email, acc_pwd, primary_email,
                    task_id=task_id,
                    search_keywords=list(all_search_keywords)
                )
                total_new += new_count
            except Exception as e:
                print(f"  从 {acc_email} 拉取失败: {e}")

        # 重新解析
        print("正在解析邮件...")
        process_all()

        # 第二阶段：合并AI分析
        if do_analyze:
            current_step += 1
            progress_text = f"[{current_step}/{total_steps}] AI 合并分析「{merge_keyword or primary_email}」的 {len(customer_emails)} 个邮箱..."
            print(progress_text)

            conn = sqlite3.connect(DB_PATH, timeout=30)
            cursor = conn.cursor()
            cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                           (current_step, total_steps, progress_text, task_id))
            conn.commit()

            init_analysis_tables(conn)
            try:
                keyword = merge_keyword or primary_email
                analyze_customer_group(conn, keyword, customer_emails)
            except Exception as e:
                print(f"  合并分析失败: {e}")
            conn.close()

        result = f"完成！拉取 {total_new} 封新邮件，合并分析 {len(customer_emails)} 个邮箱"
        print(result)
        finish_task(task_id, result)

    except Exception as e:
        print(f"任务失败: {e}")
        fail_task(task_id, str(e))


if __name__ == '__main__':
    # 用法:
    #   python background_worker.py --keyword topodom email1 email2 ...
    #   python background_worker.py email1 email2 ...
    #   python background_worker.py --keyword topodom  (自动搜索相关邮箱)
    args = sys.argv[1:]
    do_analyze = True
    merge_keyword = None

    if '--no-analyze' in args:
        do_analyze = False
        args.remove('--no-analyze')

    if '--keyword' in args:
        idx = args.index('--keyword')
        merge_keyword = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    # 如果有关键词但没有指定邮箱，自动搜索
    if merge_keyword and not args:
        conn = sqlite3.connect(DB_PATH)
        args = find_related_emails_by_keyword(conn, merge_keyword)
        conn.close()
        if args:
            print(f"关键词「{merge_keyword}」找到 {len(args)} 个相关邮箱: {args}")
        else:
            print(f"未找到与「{merge_keyword}」相关的邮箱")
            sys.exit(1)

    if not args:
        print("用法:")
        print("  python background_worker.py --keyword topodom")
        print("  python background_worker.py --keyword topodom email1@x.com email2@x.com")
        print("  python background_worker.py email1@x.com email2@x.com")
        sys.exit(1)

    run_fetch_and_analyze(args, do_analyze=do_analyze, merge_keyword=merge_keyword)
