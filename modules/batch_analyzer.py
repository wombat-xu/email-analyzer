"""批量客户分析 - 依次分析多个客户，支持中断续跑"""
import sys
import os
import sqlite3
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.settings import DB_PATH
from modules.email_fetcher import create_task, finish_task, fail_task
from modules.ai_analyzer import analyze_customer, init_analysis_tables


def get_top_unanalyzed(limit=10):
    """获取邮件数最多的未分析客户"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.email, c.name, c.domain, c.email_count
        FROM customers c
        LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
        WHERE c.is_internal = 0 AND c.contact_type = 'customer'
          AND c.email_count >= 3 AND cp.id IS NULL
        ORDER BY c.email_count DESC
        LIMIT ?
    """, (limit,))
    results = cursor.fetchall()
    conn.close()
    return results


def run_batch_analysis(customer_emails=None, limit=10):
    """批量分析客户

    customer_emails: 指定客户列表，为空则自动选择 TOP N 未分析客户
    limit: 自动选择时的数量上限
    """
    # 自动选择未分析客户
    if not customer_emails:
        unanalyzed = get_top_unanalyzed(limit)
        if not unanalyzed:
            print("没有待分析的客户")
            return
        customer_emails = [r[0] for r in unanalyzed]
        print(f"自动选择 TOP {len(customer_emails)} 未分析客户：")
        for email, name, domain, count in unanalyzed:
            print(f"  {email} ({name or domain}) - {count} 封邮件")

    total = len(customer_emails)
    task_id = create_task(f"批量分析 {total} 个客户", task_type='batch_analyze')

    conn = sqlite3.connect(DB_PATH, timeout=30)
    init_analysis_tables(conn)

    succeeded = 0
    failed_list = []
    start_time = time.time()

    try:
        for i, email in enumerate(customer_emails):
            # 检查是否已分析（支持中断续跑）
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM customer_profiles WHERE customer_email = ?", (email,))
            if cursor.fetchone():
                print(f"[{i+1}/{total}] {email} — 已分析，跳过")
                succeeded += 1
                continue

            # 更新进度
            elapsed = time.time() - start_time
            avg_per = elapsed / max(i, 1)
            eta = avg_per * (total - i)
            eta_str = f"{int(eta//60)}分{int(eta%60)}秒" if eta > 60 else f"{int(eta)}秒"
            progress_text = f"[{i+1}/{total}] 正在分析 {email}（预计剩余 {eta_str}）"
            print(progress_text)

            conn.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                         (i, total, progress_text, task_id))
            conn.commit()

            # 分析
            try:
                result = analyze_customer(conn, email)
                if result:
                    succeeded += 1
                    print(f"  ✓ 分析完成")
                else:
                    print(f"  - 无足够数据，跳过")
            except Exception as e:
                print(f"  ✗ 分析失败: {e}")
                failed_list.append(email)

            # API 限流保护
            if i < total - 1:
                time.sleep(2)

        # 完成
        result_msg = f"完成！成功 {succeeded}/{total}"
        if failed_list:
            result_msg += f"，失败 {len(failed_list)}: {', '.join(failed_list[:5])}"
        print(result_msg)
        finish_task(task_id, result_msg)

    except KeyboardInterrupt:
        msg = f"用户中断，已完成 {succeeded}/{total}"
        print(msg)
        fail_task(task_id, msg)
    except Exception as e:
        msg = f"批量分析异常: {str(e)[:200]}"
        print(msg)
        fail_task(task_id, msg)
    finally:
        conn.close()


if __name__ == '__main__':
    args = sys.argv[1:]
    limit = 10

    if '--limit' in args:
        idx = args.index('--limit')
        limit = int(args[idx + 1])
        args = args[:idx] + args[idx + 2:]

    if args:
        # 指定邮箱列表
        run_batch_analysis(customer_emails=args)
    else:
        # 自动选择 TOP N
        run_batch_analysis(limit=limit)
