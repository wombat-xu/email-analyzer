"""一键运行脚本 - 外贸邮件智能分析系统"""
import sys
import os
import sqlite3

sys.path.insert(0, os.path.dirname(__file__))

from config.settings import DB_PATH, ANTHROPIC_API_KEY
from modules.email_fetcher import fetch_all_emails, get_email_stats, init_database
from modules.email_parser import process_all
from modules.ai_analyzer import analyze_all_customers


def step1_fetch_emails():
    """第一步：采集邮件"""
    print("\n" + "=" * 60)
    print("第一步：邮件采集")
    print("=" * 60)

    email_addr = input("\n请输入邮箱地址: ").strip()
    password = input("请输入密码（或授权码）: ").strip()

    limit = input("每个文件夹拉取多少封？（直接回车=全部，输入数字=限制数量）: ").strip()
    limit = int(limit) if limit else None

    print(f"\n开始拉取 {email_addr} 的邮件...")
    fetch_all_emails(email_addr, password, limit)

    stats = get_email_stats()
    if stats:
        print(f"\n📊 当前数据库统计:")
        print(f"  总邮件数: {stats['total']}")
        print(f"  独立发件人: {stats['unique_senders']}")


def step2_parse_emails():
    """第二步：解析邮件和构建线程"""
    print("\n" + "=" * 60)
    print("第二步：邮件解析与对话线程重组")
    print("=" * 60)
    process_all()


def step3_ai_analysis():
    """第三步：AI客户分析"""
    print("\n" + "=" * 60)
    print("第三步：AI 客户分析")
    print("=" * 60)

    if not ANTHROPIC_API_KEY:
        api_key = input("\n请输入 Anthropic API Key: ").strip()
        os.environ["ANTHROPIC_API_KEY"] = api_key
        # 重新加载配置
        from config import settings
        settings.ANTHROPIC_API_KEY = api_key

    max_customers = input("分析多少个客户？（直接回车=全部，输入数字=限制数量）: ").strip()
    max_customers = int(max_customers) if max_customers else None

    min_emails = input("最少多少封邮件的客户才分析？（直接回车=3封）: ").strip()
    min_emails = int(min_emails) if min_emails else 3

    analyze_all_customers(min_emails=min_emails, max_customers=max_customers)


def step4_launch_web():
    """第四步：启动Web界面"""
    print("\n" + "=" * 60)
    print("第四步：启动 Web 知识库界面")
    print("=" * 60)
    print("\n正在启动 Web 界面...")
    print("浏览器会自动打开，如果没有，请手动打开: http://localhost:8501")

    web_app = os.path.join(os.path.dirname(__file__), "web", "app.py")
    os.system(f'python3 -m streamlit run "{web_app}"')


def main():
    print("""
╔══════════════════════════════════════════════╗
║      外贸邮件智能分析系统                       ║
║      Personal Care Products Email Analyzer   ║
╚══════════════════════════════════════════════╝
    """)

    if len(sys.argv) > 1:
        step = sys.argv[1]
        if step == '1' or step == 'fetch':
            step1_fetch_emails()
        elif step == '2' or step == 'parse':
            step2_parse_emails()
        elif step == '3' or step == 'analyze':
            step3_ai_analysis()
        elif step == '4' or step == 'web':
            step4_launch_web()
        elif step == 'all':
            step1_fetch_emails()
            step2_parse_emails()
            step3_ai_analysis()
            step4_launch_web()
        else:
            print(f"未知步骤: {step}")
        return

    print("请选择操作:")
    print("  1 - 采集邮件（从邮箱服务器拉取）")
    print("  2 - 解析邮件（构建对话线程和客户列表）")
    print("  3 - AI分析（生成客户画像和商机）")
    print("  4 - 启动Web界面（可搜索的知识库）")
    print("  5 - 全部执行（1→2→3→4 依次运行）")
    print("  6 - 查看统计信息")
    print()

    choice = input("请输入选项 (1-6): ").strip()

    if choice == '1':
        step1_fetch_emails()
    elif choice == '2':
        step2_parse_emails()
    elif choice == '3':
        step3_ai_analysis()
    elif choice == '4':
        step4_launch_web()
    elif choice == '5':
        step1_fetch_emails()
        step2_parse_emails()
        step3_ai_analysis()
        step4_launch_web()
    elif choice == '6':
        stats = get_email_stats()
        if stats:
            print(f"\n📊 数据库统计:")
            print(f"  总邮件数: {stats['total']}")
            print(f"  独立发件人: {stats['unique_senders']}")
            print(f"\n  按账号分布:")
            for acc, cnt in stats['by_account'].items():
                print(f"    {acc}: {cnt} 封")
            print(f"\n  按文件夹分布:")
            for folder, cnt in stats['by_folder'].items():
                print(f"    {folder}: {cnt} 封")
        else:
            print("数据库为空，请先运行邮件采集")
    else:
        print("无效选项")


if __name__ == '__main__':
    main()
