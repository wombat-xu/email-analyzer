"""Web知识库界面 - 基于Streamlit"""
import streamlit as st
import sqlite3
import json
import os
import sys
import subprocess
import pandas as pd
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH, OPENROUTER_API_KEY, COMPANY_PRODUCTS, DORMANT_MONTHS
from modules.ai_analyzer import chat_with_knowledge, find_dormant_customers, find_inquired_not_ordered
from modules.email_parser import get_customer_threads, get_email_text

st.set_page_config(page_title="外贸邮件智能分析系统", page_icon="📧", layout="wide")

CONTACT_TYPE_LABELS = {
    'customer': '✅ 客户',
    'platform': '🏪 平台/系统',
    'logistics': '🚢 货代/物流',
    'inspection': '🔍 验厂/检测',
    'advertisement': '📢 广告/展会',
    'supplier': '🏭 供应商',
    'finance': '🏦 银行/金融',
    'government': '🏛️ 政府/海关',
    'internal': '🏠 内部',
}


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def format_date(date_str):
    """将邮件日期统一格式化为 YYYY-MM-DD"""
    if not date_str or date_str == '-':
        return '-'
    from email.utils import parsedate_to_datetime
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        # 尝试直接截取
        s = str(date_str).strip()[:10]
        return s if len(s) >= 8 else '-'


def main():
    st.title("📧 外贸邮件智能分析系统")
    st.caption("个人护理产品 | 客户知识库 & 商机挖掘")

    if not os.path.exists(DB_PATH):
        st.error("数据库不存在，请先运行邮件采集程序！")
        return

    pages = [
        "📊 仪表盘",
        "⚙️ 邮箱账号管理",
        "📬 全部邮件",
        "🏆 TOP客户（优先分析）",
        "👥 客户列表",
        "🔍 客户详情",
        "💡 商机看板",
        "🤖 AI 助手",
        "📥 数据导出"
    ]
    page = st.sidebar.radio("功能导航", pages)

    if page == "📊 仪表盘":
        show_dashboard()
    elif page == "⚙️ 邮箱账号管理":
        show_account_management()
    elif page == "📬 全部邮件":
        show_all_emails()
    elif page == "🏆 TOP客户（优先分析）":
        show_top_customers()
    elif page == "👥 客户列表":
        show_customer_list()
    elif page == "🔍 客户详情":
        show_customer_detail()
    elif page == "💡 商机看板":
        show_opportunities()
    elif page == "🤖 AI 助手":
        show_ai_chat()
    elif page == "📥 数据导出":
        show_export()


def show_account_management():
    """邮箱账号管理"""
    from modules.email_fetcher import (add_email_account, get_all_accounts, remove_email_account,
                                       get_sync_status, get_running_tasks, get_recent_tasks)

    st.subheader("⚙️ 邮箱账号管理")
    st.caption("配置公司业务员的邮箱账号，系统会从所有账号中拉取邮件进行分析")

    # 显示已配置的账号
    accounts = get_all_accounts()
    if accounts:
        st.markdown("### 已配置的邮箱账号")
        data = [{
            "邮箱": a[0],
            "业务员": a[3] or "-",
            "IMAP服务器": a[2],
            "最后同步": (a[4] or "未同步")[:19],
        } for a in accounts]
        st.dataframe(pd.DataFrame(data), use_container_width=True)

        # 删除账号
        del_email = st.selectbox("选择要删除的账号", [a[0] for a in accounts])
        if st.button("🗑️ 删除选中账号"):
            remove_email_account(del_email)
            st.success(f"已删除 {del_email}")
            st.rerun()
    else:
        st.info("还没有配置任何邮箱账号，请在下方添加。")

    st.divider()

    # 添加新账号
    st.markdown("### 添加邮箱账号")
    col1, col2 = st.columns(2)
    with col1:
        new_email = st.text_input("邮箱地址", placeholder="sales@meinuo.com")
        new_password = st.text_input("密码/授权码", type="password")
    with col2:
        new_name = st.text_input("业务员姓名", placeholder="张三")
        new_imap = st.text_input("IMAP服务器", value="imaphz.qiye.163.com")

    if st.button("➕ 添加账号", type="primary"):
        if new_email and new_password:
            # 测试连接
            with st.spinner("正在测试连接..."):
                try:
                    from modules.email_fetcher import connect_imap
                    import imaplib
                    if new_imap != "imaphz.qiye.163.com":
                        mail = imaplib.IMAP4_SSL(new_imap, 993)
                        mail.login(new_email, new_password)
                        mail.logout()
                    else:
                        mail = connect_imap(new_email, new_password)
                        mail.logout()
                    add_email_account(new_email, new_password, new_name, new_imap)
                    st.success(f"✅ {new_email} 添加成功！连接测试通过。")
                    st.rerun()
                except Exception as e:
                    st.error(f"连接失败: {e}。请检查邮箱地址、密码和IMAP服务器是否正确。")
        else:
            st.warning("请填写邮箱地址和密码")

    st.divider()

    # 同步状态
    st.markdown("### 📊 同步状态")
    sync_data = get_sync_status()
    if sync_data:
        rows = []
        for s in sync_data:
            pct = round(s[3] / s[2] * 100, 1) if s[2] > 0 else 0
            rows.append({
                "账号": s[0], "文件夹": s[1],
                "服务器总数": s[2], "已拉取": s[3],
                "进度": f"{pct}%",
                "最后同步": (s[4] or "-")[:19]
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.caption("还没有同步过邮件")

    st.divider()

    # 后台任务监控（只显示最新的一个）
    running = get_running_tasks()
    if running:
        t = running[-1]
        task_id, task_type, desc, cur, total, text, created = t
        st.markdown("### 🔄 正在进行的任务")
        st.markdown(f"**{desc}**")
        if total > 0:
            st.progress(min(cur / total, 1.0), text=text or f"{cur}/{total}")
        else:
            st.info(text or "进行中...")
        if st.button("🔄 刷新进度"):
            st.rerun()

    recent = get_recent_tasks(5)
    done_tasks = [t for t in recent if t[3] == 'done']
    if done_tasks:
        st.markdown("### ✅ 最近完成的任务")
        for t in done_tasks[:3]:
            st.success(f"**{t[2]}** — {t[7] or '完成'} （{(t[9] or '')[:19]}）")

    st.divider()

    # 批量拉取邮件
    st.markdown("### 📥 拉取邮件")
    if accounts:
        col1, col2 = st.columns(2)
        with col1:
            sync_account = st.selectbox("选择账号", ["全部账号"] + [a[0] for a in accounts])
        with col2:
            sync_limit = st.number_input("每个文件夹拉取封数（0=全部）", min_value=0, value=500)

        if st.button("📥 开始拉取邮件", type="primary"):
            limit = sync_limit if sync_limit > 0 else None
            from modules.email_fetcher import fetch_all_emails, create_task, finish_task, fail_task

            accs_to_sync = accounts if sync_account == "全部账号" else [next(a for a in accounts if a[0] == sync_account)]

            for acc_email, acc_pwd, acc_imap, acc_name, _, _ in accs_to_sync:
                task_id = create_task(f"拉取 {acc_email} ({acc_name}) 的邮件")
                progress_bar = st.progress(0, text=f"正在连接 {acc_email}...")
                status_text = st.empty()

                try:
                    # 连接并获取文件夹
                    from modules.email_fetcher import connect_imap, list_folders, fetch_emails_from_folder, init_database
                    conn = init_database()
                    mail = connect_imap(acc_email, acc_pwd)
                    folders = list_folders(mail)

                    total_fetched = 0
                    for fi, folder in enumerate(folders):
                        pct = (fi) / len(folders)
                        status_text.text(f"📂 正在处理文件夹 {fi+1}/{len(folders)}: {folder}")
                        progress_bar.progress(pct, text=f"文件夹 {fi+1}/{len(folders)}: {folder}")

                        def update_progress(cur, total, text):
                            inner_pct = fi / len(folders) + (cur / total) / len(folders) if total > 0 else pct
                            progress_bar.progress(min(inner_pct, 1.0), text=text)

                        fetched, mail = fetch_emails_from_folder(
                            mail, folder, acc_email, conn, limit,
                            progress_callback=update_progress, task_id=task_id,
                            password=acc_pwd
                        )
                        total_fetched += fetched

                    try:
                        mail.logout()
                    except Exception:
                        pass
                    conn.close()
                    progress_bar.progress(1.0, text=f"✅ {acc_email} 完成！新增 {total_fetched} 封")
                    status_text.empty()
                    finish_task(task_id, f"新增 {total_fetched} 封邮件")
                    st.success(f"✅ {acc_email} 拉取完成，新增 {total_fetched} 封邮件")

                except Exception as e:
                    fail_task(task_id, str(e))
                    st.error(f"❌ {acc_email} 拉取失败: {e}")

            # 重新解析
            with st.spinner("正在解析邮件..."):
                from modules.email_parser import process_all
                process_all()
            st.success("邮件解析完成！")


def show_all_emails():
    """全部邮件浏览"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("📬 全部邮件")

    # 获取筛选选项
    cursor.execute('SELECT DISTINCT account FROM emails ORDER BY account')
    accounts = [r[0] for r in cursor.fetchall()]
    cursor.execute('SELECT DISTINCT folder FROM emails ORDER BY folder')
    folders = [r[0] for r in cursor.fetchall()]

    # 筛选区
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        search = st.text_input("搜索（主题/发件人/收件人）", "", key="email_search")
    with col2:
        account_filter = st.selectbox("账号", ["全部"] + accounts, key="email_account")
    with col3:
        folder_filter = st.selectbox("文件夹", ["全部"] + folders, key="email_folder")
    with col4:
        sort_order = st.selectbox("排序", ["最新优先", "最旧优先"], key="email_sort")

    # 构建查询
    where_clauses = []
    params = []
    if search:
        where_clauses.append('(subject LIKE ? OR from_addr LIKE ? OR from_name LIKE ? OR to_addr LIKE ?)')
        params.extend([f'%{search}%'] * 4)
    if account_filter != "全部":
        where_clauses.append('account = ?')
        params.append(account_filter)
    if folder_filter != "全部":
        where_clauses.append('folder = ?')
        params.append(folder_filter)

    where_sql = (' WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''
    order = 'DESC' if sort_order == "最新优先" else 'ASC'

    # 总数
    cursor.execute(f'SELECT COUNT(*) FROM emails{where_sql}', params)
    total = cursor.fetchone()[0]

    # 分页
    page_size = 50
    total_pages = max(1, (total + page_size - 1) // page_size)
    if 'email_page' not in st.session_state:
        st.session_state.email_page = 1
    # 筛选条件变化时重置页码
    filter_key = f"{search}|{account_filter}|{folder_filter}"
    if st.session_state.get('email_filter_key') != filter_key:
        st.session_state.email_page = 1
        st.session_state.email_filter_key = filter_key

    current_page = st.session_state.email_page
    offset = (current_page - 1) * page_size

    # 分页控制
    pcol1, pcol2, pcol3, pcol4, pcol5 = st.columns([1, 1, 2, 1, 1])
    with pcol1:
        if st.button("上一页", disabled=(current_page <= 1), key="prev_page"):
            st.session_state.email_page -= 1
            st.rerun()
    with pcol2:
        if st.button("下一页", disabled=(current_page >= total_pages), key="next_page"):
            st.session_state.email_page += 1
            st.rerun()
    with pcol3:
        st.caption(f"第 {current_page}/{total_pages} 页，共 {total:,} 封邮件")
    with pcol4:
        jump = st.number_input("跳转", min_value=1, max_value=total_pages, value=current_page, key="jump_page", label_visibility="collapsed")
    with pcol5:
        if st.button("跳转", key="do_jump"):
            st.session_state.email_page = jump
            st.rerun()

    # 查询当前页数据
    cursor.execute(f'''
        SELECT id, date, from_addr, from_name, to_addr, subject, folder, account, body_text, body_html
        FROM emails{where_sql}
        ORDER BY date {order}
        LIMIT ? OFFSET ?
    ''', params + [page_size, offset])
    rows = cursor.fetchall()

    if rows:
        # 表格概览
        data = []
        for r in rows:
            data.append({
                "日期": format_date(r[1]),
                "发件人": f"{r[3]} <{r[2]}>" if r[3] else r[2],
                "收件人": (r[4] or '')[:50],
                "主题": (r[5] or '')[:80],
                "文件夹": r[6],
                "账号": r[7],
            })
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=400)

        # 邮件详情展开
        st.divider()
        st.caption("点击展开查看邮件正文：")
        for r in rows:
            email_id, date_str, from_addr, from_name, to_addr, subject, folder, account, body_text, body_html = r
            label = f"📩 {format_date(date_str)} | {from_name or from_addr} → {(to_addr or '')[:30]} | {(subject or '(无主题)')[:60]}"
            with st.expander(label):
                ecol1, ecol2 = st.columns(2)
                with ecol1:
                    st.markdown(f"**发件人：** {from_name} &lt;{from_addr}&gt;")
                    st.markdown(f"**收件人：** {to_addr}")
                with ecol2:
                    st.markdown(f"**日期：** {date_str}")
                    st.markdown(f"**文件夹：** {folder} ({account})")
                st.markdown(f"**主题：** {subject}")
                st.divider()
                body = get_email_text(body_text, body_html)
                if body:
                    st.markdown(
                        f'<div style="background:#f5f5f5;padding:12px 16px;border-left:4px solid #9e9e9e;'
                        f'border-radius:4px;font-size:13px;line-height:1.7;white-space:pre-wrap;max-height:500px;overflow-y:auto">'
                        f'{body[:5000]}</div>',
                        unsafe_allow_html=True
                    )
                else:
                    st.info("该邮件无文本正文")
    else:
        st.info("没有找到符合条件的邮件")

    conn.close()


def show_dashboard():
    """仪表盘"""
    conn = get_db()
    cursor = conn.cursor()

    # 下载进程实时状态
    log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'worker.log')
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
                last_lines = [l.strip() for l in lines[-8:] if l.strip()]
            if last_lines:
                # 检查是否还在活跃（最后修改时间在5分钟内）
                import time
                mtime = os.path.getmtime(log_file)
                is_active = (time.time() - mtime) < 300

                if is_active:
                    st.markdown("#### 🔄 全量下载进行中")
                    st.code('\n'.join(last_lines[-5:]), language=None)
                    if st.button("🔄 刷新状态"):
                        st.rerun()
                    st.divider()
        except Exception:
            pass

    col1, col2, col3, col4 = st.columns(4)

    cursor.execute('SELECT COUNT(*) FROM emails')
    total_emails = cursor.fetchone()[0]
    col1.metric("总邮件数", f"{total_emails:,}")

    cursor.execute("SELECT COUNT(*) FROM customers WHERE is_internal = 0 AND contact_type = 'customer'")
    total_customers = cursor.fetchone()[0]
    col2.metric("真实客户", f"{total_customers:,}")

    cursor.execute('SELECT COUNT(*) FROM threads')
    total_threads = cursor.fetchone()[0]
    col3.metric("对话线程", f"{total_threads:,}")

    cursor.execute('SELECT COUNT(*) FROM customer_profiles')
    analyzed = cursor.fetchone()[0]
    col4.metric("已分析客户", f"{analyzed:,}")

    st.divider()

    # 联系人分类统计
    st.subheader("📋 联系人分类统计")
    cursor.execute("""
        SELECT contact_type, COUNT(*), SUM(email_count)
        FROM customers WHERE is_internal = 0
        GROUP BY contact_type ORDER BY SUM(email_count) DESC
    """)
    type_stats = cursor.fetchall()
    if type_stats:
        data = [{
            "类型": CONTACT_TYPE_LABELS.get(t[0], t[0]),
            "联系人数": t[1],
            "邮件总数": t[2] or 0
        } for t in type_stats]
        st.dataframe(pd.DataFrame(data), use_container_width=True)

    st.divider()

    # 沉睡客户预警
    dormant = find_dormant_customers(conn)
    if dormant:
        st.subheader(f"⚠️ 沉睡客户预警（超过{DORMANT_MONTHS}个月未联系）")
        dormant_data = []
        for d in dormant[:10]:
            dormant_data.append({
                "邮箱": d[0], "姓名": d[1] or "-",
                "公司": d[5] or "-", "国家": d[6] or "-",
                "最后联系": d[3] or "-", "邮件数": d[4]
            })
        st.dataframe(pd.DataFrame(dormant_data), use_container_width=True)

    conn.close()


def launch_background_task(customer_emails, do_analyze=True, merge_keyword=None):
    """启动后台任务（独立进程，不受页面刷新影响）"""
    import subprocess
    project_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    worker = os.path.join(project_dir, 'modules', 'background_worker.py')
    log_file = os.path.join(project_dir, 'data', 'worker.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    cmd = [sys.executable, worker]
    if merge_keyword:
        cmd.extend(['--keyword', merge_keyword])
    cmd.extend(customer_emails)
    if not do_analyze:
        cmd.append('--no-analyze')
    subprocess.Popen(
        cmd, cwd=project_dir,
        stdout=open(log_file, 'a'),
        stderr=subprocess.STDOUT,
        start_new_session=True
    )


def show_top_customers():
    """TOP客户列表 - 供用户选择优先分析"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("🏆 TOP 客户（按邮件数量排名）")

    # === 后台任务状态（只显示最新的一个running任务） ===
    from modules.email_fetcher import get_running_tasks, get_recent_tasks
    running = get_running_tasks()
    if running:
        # 只取最新的一个
        t = running[-1]
        task_id, task_type, desc, cur, total, text, created = t
        st.markdown("#### 🔄 正在进行的后台任务")
        st.markdown(f"**{desc}**")
        if total > 0:
            st.progress(min(cur / total, 1.0), text=text or f"{cur}/{total}")
        else:
            st.info(text or "进行中...")
        if st.button("🔄 刷新任务进度"):
            st.rerun()
        st.divider()

    recent = get_recent_tasks(3)
    done_tasks = [t for t in recent if t[3] == 'done']
    if done_tasks:
        t = done_tasks[0]
        st.success(f"✅ **{t[2]}** — {t[7] or '完成'} （{(t[9] or '')[:19]}）")
        st.divider()

    # === TOP 客户表格 ===
    cursor.execute("""
        SELECT c.email, c.name, c.domain, c.email_count, c.first_contact, c.last_contact,
               CASE WHEN cp.id IS NOT NULL THEN '✅ 已分析' ELSE '⏳ 待分析' END as status,
               cp.company_name, cp.country
        FROM customers c
        LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
        WHERE c.is_internal = 0 AND c.contact_type = 'customer' AND c.email_count >= 3
        ORDER BY c.email_count DESC
        LIMIT 50
    """)
    top_customers = cursor.fetchall()

    if top_customers:
        data = []
        for i, r in enumerate(top_customers):
            data.append({
                "排名": i + 1,
                "邮箱": r[0], "姓名": r[1] or "-", "域名": r[2],
                "邮件数": r[3],
                "首次联系": format_date(r[4]), "最后联系": format_date(r[5]),
                "状态": r[6],
                "公司": r[7] or "-", "国家": r[8] or "-"
            })
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=400)

    st.divider()

    # === 方式一：关键词搜索客户邮箱 ===
    st.subheader("🔍 方式一：按关键词搜索并合并分析")
    st.caption("输入公司名/关键词，自动找到该公司所有邮箱，合并为一个客户进行完整分析")

    search_keyword = st.text_input("输入关键词搜索", placeholder="如: topodom, acillc, nevada 等")

    if search_keyword:
        kw = f"%{search_keyword}%"
        cursor.execute("""
            SELECT c.email, c.name, c.domain, c.email_count,
                   CASE WHEN cp.id IS NOT NULL THEN '✅ 已分析' ELSE '⏳ 待分析' END as status
            FROM customers c
            LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
            WHERE c.is_internal = 0
              AND (c.email LIKE ? OR c.name LIKE ? OR c.domain LIKE ?)
            ORDER BY c.email_count DESC
            LIMIT 30
        """, (kw, kw, kw))
        search_results = cursor.fetchall()

        if search_results:
            total_emails_found = sum(r[3] for r in search_results)
            st.markdown(f"找到 **{len(search_results)}** 个相关邮箱，共 **{total_emails_found}** 封邮件：")
            search_data = [{
                "邮箱": r[0], "姓名": r[1] or "-", "域名": r[2],
                "邮件数": r[3], "状态": r[4]
            } for r in search_results]
            st.dataframe(pd.DataFrame(search_data), use_container_width=True)

            # 默认全选
            search_options = [f"{r[0]} ({r[1] or '-'}) - {r[3]}封" for r in search_results]
            search_selected = st.multiselect(
                "选择要合并分析的邮箱（默认全选，同一公司的邮箱会合并为一个客户分析）",
                search_options, default=search_options, key="search_select"
            )

            # 费用预估
            if search_selected:
                emails_for_cost = [s.split(" (")[0] for s in search_selected]
                from modules.ai_analyzer import estimate_customer_cost
                try:
                    cost = estimate_customer_cost(conn, emails_for_cost)
                    if cost:
                        st.markdown("#### 💰 费用预估")
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("对话线程", f"{cost['thread_count']} 个")
                        col2.metric("邮件数", f"{cost['email_count']} 封")
                        col3.metric("预估输入Token", f"{cost['input_tokens']:,}")
                        col4.metric("预估费用", f"¥{cost['total_cost_rmb']:.2f}")
                        st.caption(f"明细：输入 ${cost['input_cost']:.4f} + 输出 ${cost['output_cost']:.4f} = ${cost['total_cost']:.4f}（按 Opus 4.6 价格）")
                    else:
                        st.caption("暂无邮件数据，需先拉取邮件后才能预估费用")
                except Exception:
                    st.caption("费用预估需要邮件数据已下载到本地")

            if st.button("🚀 后台拉取 + 合并分析", type="primary", key="btn_search"):
                if search_selected:
                    emails = [s.split(" (")[0] for s in search_selected]
                    launch_background_task(emails, merge_keyword=search_keyword)
                    st.success(f"✅ 已提交后台任务！")
                    st.markdown(f"**关键词**: {search_keyword}")
                    st.markdown(f"**合并分析 {len(emails)} 个邮箱**：")
                    for e in emails:
                        st.write(f"  - {e}")
                    st.info("所有邮箱的邮件将合并为一个完整的客户画像。任务在后台运行，不会因刷新页面中断。")
                    st.rerun()
                else:
                    st.warning("请先选择至少一个邮箱")
        else:
            st.warning(f"没有找到包含「{search_keyword}」的邮箱。如果是新客户，请先在「邮箱账号管理」中拉取邮件。")

    st.divider()

    # === 方式二：直接输入邮箱地址 ===
    st.subheader("📝 方式二：直接输入邮箱地址")
    st.caption("输入同一客户的多个邮箱地址（每行一个），系统会合并为一个客户进行完整分析")

    merge_name = st.text_input("客户/公司名称（用于标识）", placeholder="如: Topodom", key="merge_name")
    manual_emails = st.text_area("输入客户邮箱（每行一个）", height=100,
                                  placeholder="topodom@intnet.mu\nsandra.to@topodom.mu\njonathan.to@topodom.mu")

    # 费用预估
    if manual_emails.strip():
        manual_list = [e.strip() for e in manual_emails.strip().split('\n') if e.strip() and '@' in e]
        if manual_list:
            from modules.ai_analyzer import estimate_customer_cost
            try:
                cost = estimate_customer_cost(conn, manual_list)
                if cost:
                    st.markdown("#### 💰 费用预估")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("对话线程", f"{cost['thread_count']} 个")
                    col2.metric("邮件数", f"{cost['email_count']} 封")
                    col3.metric("预估输入Token", f"{cost['input_tokens']:,}")
                    col4.metric("预估费用", f"¥{cost['total_cost_rmb']:.2f}")
                    st.caption(f"明细：输入 ${cost['input_cost']:.4f} + 输出 ${cost['output_cost']:.4f} = ${cost['total_cost']:.4f}")
                else:
                    st.caption("这些邮箱暂无本地邮件数据，需先拉取后才能预估")
            except Exception:
                pass

    if st.button("🚀 后台拉取 + 合并分析（手动输入）", type="primary", key="btn_manual"):
        if manual_emails.strip():
            emails = [e.strip() for e in manual_emails.strip().split('\n') if e.strip() and '@' in e]
            if emails:
                keyword = merge_name.strip() or emails[0].split('@')[0]
                launch_background_task(emails, merge_keyword=keyword)
                st.success(f"✅ 已提交后台任务！以「{keyword}」为客户名，合并分析 {len(emails)} 个邮箱：")
                for e in emails:
                    st.write(f"  - {e}")
                st.info("所有邮箱的邮件将合并为一个完整的客户画像。任务在后台运行。")
                st.rerun()
            else:
                st.warning("请输入有效的邮箱地址")
        else:
            st.warning("请先输入邮箱地址")

    st.divider()

    # === 方式三：从TOP列表选择 ===
    st.subheader("🏆 方式三：从TOP列表选择")
    if top_customers:
        options = [f"{r[0]} ({r[1] or r[2]}) - {r[3]}封邮件 {r[6]}" for r in top_customers]
        selected = st.multiselect("从TOP客户中选择（可多选）", options, key="top_select")

        if st.button("🚀 后台拉取并分析（TOP客户）", type="primary", key="btn_top"):
            if selected:
                emails = [s.split(" (")[0] for s in selected]
                launch_background_task(emails)
                st.success(f"✅ 已提交后台任务！正在拉取并分析 {len(emails)} 个客户，任务在后台运行。")
                st.rerun()
            else:
                st.warning("请先选择至少一个客户")
    else:
        st.info("没有可选择的客户")

    conn.close()


def show_customer_list():
    """客户列表 - 支持分类筛选"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("👥 客户列表")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        search = st.text_input("搜索（邮箱/姓名/公司）", "")
    with col2:
        type_filter = st.selectbox("联系人类型", [
            "全部", "✅ 客户", "🏪 平台/系统", "🚢 货代/物流",
            "🔍 验厂/检测", "📢 广告/展会", "🏭 供应商"
        ])
    with col3:
        country_filter = st.text_input("国家筛选", "")
    with col4:
        min_emails_filter = st.number_input("最少邮件数", min_value=1, value=3)

    # 反向映射
    type_reverse = {v: k for k, v in CONTACT_TYPE_LABELS.items()}

    query = '''
        SELECT c.email, c.name, c.domain, c.first_contact, c.last_contact, c.email_count,
               cp.company_name, cp.country,
               CASE WHEN cp.id IS NOT NULL THEN '已分析' ELSE '待分析' END as status,
               c.contact_type
        FROM customers c
        LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
        WHERE c.is_internal = 0 AND c.email_count >= ?
    '''
    params = [min_emails_filter]

    if type_filter != "全部":
        contact_type_key = type_reverse.get(type_filter, 'customer')
        query += ' AND c.contact_type = ?'
        params.append(contact_type_key)

    if search:
        query += ' AND (c.email LIKE ? OR c.name LIKE ? OR cp.company_name LIKE ?)'
        params.extend([f'%{search}%'] * 3)

    if country_filter:
        query += ' AND cp.country LIKE ?'
        params.append(f'%{country_filter}%')

    query += ' ORDER BY c.email_count DESC'

    cursor.execute(query, params)
    results = cursor.fetchall()

    if results:
        data = []
        for r in results:
            data.append({
                "邮箱": r[0], "姓名": r[1] or "-", "域名": r[2],
                "首次联系": format_date(r[3]), "最后联系": format_date(r[4]),
                "邮件数": r[5], "公司": r[6] or "-", "国家": r[7] or "-",
                "分析状态": r[8],
                "类型": CONTACT_TYPE_LABELS.get(r[9], r[9] or '-')
            })
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=600)
        st.caption(f"共 {len(results)} 个联系人")

        # 提供手动修改分类的功能
        st.divider()
        st.subheader("✏️ 手动修改联系人分类")
        st.caption("如果系统分类不准确，可以在这里手动调整")
        col1, col2 = st.columns(2)
        with col1:
            fix_email = st.text_input("输入要修改的邮箱地址")
        with col2:
            fix_type = st.selectbox("修改为", list(CONTACT_TYPE_LABELS.values()))
        if st.button("确认修改"):
            if fix_email:
                fix_type_key = type_reverse.get(fix_type, 'customer')
                cursor.execute('UPDATE customers SET contact_type = ? WHERE email = ?',
                               (fix_type_key, fix_email.strip()))
                conn.commit()
                st.success(f"已将 {fix_email} 修改为 {fix_type}")
                st.rerun()
    else:
        st.info("没有符合条件的联系人")

    conn.close()


def show_customer_detail():
    """客户详情 - 包含原始邮件内容和翻译"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("🔍 客户详情")

    cursor.execute('''
        SELECT customer_email, customer_name, company_name, country
        FROM customer_profiles ORDER BY customer_email
    ''')
    profiles = cursor.fetchall()

    if not profiles:
        st.warning("还没有分析过任何客户，请先到「TOP客户」页面选择分析。")
        conn.close()
        return

    options = [f"{p[0]} ({p[1] or ''} - {p[2] or ''})" for p in profiles]
    selected = st.selectbox("选择客户", options)

    if selected:
        email_addr = selected.split(" (")[0]
        cursor.execute('SELECT profile_json FROM customer_profiles WHERE customer_email = ?', (email_addr,))
        row = cursor.fetchone()

        if row:
            profile = json.loads(row[0])

            # 基本信息
            basic = profile.get('basic_info', {})
            st.markdown("### 基本信息")
            col1, col2, col3 = st.columns(3)
            col1.write(f"**姓名**: {basic.get('name', '未知')}")
            col1.write(f"**公司**: {basic.get('company', '未知')}")
            col2.write(f"**国家**: {basic.get('country', '未知')}")
            col2.write(f"**职位**: {basic.get('position', '未知')}")
            col3.write(f"**公司类型**: {basic.get('company_type', '未知')}")
            col3.write(f"**公司规模**: {basic.get('company_scale', '未知')}")

            # 感兴趣的产品
            products = profile.get('products_of_interest', [])
            if products:
                st.markdown("### 感兴趣的产品")
                st.write(", ".join(products))

            # 行为画像
            behavior = profile.get('behavior_profile', {})
            st.markdown("### 行为画像")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**价格敏感度**: {behavior.get('price_sensitivity', '未知')}")
                st.caption(behavior.get('price_sensitivity_evidence', ''))
                st.write(f"**决策模式**: {behavior.get('decision_pattern', '未知')}")
                st.caption(behavior.get('decision_evidence', ''))
                st.write(f"**付款方式**: {behavior.get('payment_preference', '未知')}")
            with col2:
                st.write(f"**沟通风格**: {behavior.get('communication_style', '未知')}")
                st.write(f"**回复速度**: {behavior.get('response_speed', '未知')}")
                st.write(f"**下单频率**: {behavior.get('order_frequency', '未知')}")
                st.write(f"**平均订单金额**: {behavior.get('average_order_value', '未知')}")

            # 关系状态
            rel = profile.get('relationship_status', {})
            st.markdown("### 关系状态")
            col1, col2 = st.columns(2)
            col1.write(f"**当前状态**: {rel.get('current_status', '未知')}")
            col1.write(f"**关系质量**: {rel.get('relationship_quality', '未知')}")
            col2.write(f"**最后联系**: {rel.get('last_contact_date', '未知')}")
            col2.write(f"**信任度**: {rel.get('trust_level', '未知')}")

            # 关键对话 - 双方博弈视角
            convos = profile.get('key_conversations', [])
            if convos:
                st.markdown("### 🎯 关键对话复盘")
                st.caption("展示客户与业务员之间的核心博弈过程，帮助学习谈判技巧")
                for convo in convos:
                    with st.expander(f"📌 {convo.get('topic', '对话')} ({convo.get('date', '')})", expanded=False):
                        st.markdown(f"**📋 概况**: {convo.get('summary', '')}")
                        st.markdown(f"**🏁 结果**: {convo.get('outcome', '')}")

                        # 博弈回合展示
                        rounds = convo.get('negotiation_rounds', [])
                        if rounds:
                            st.markdown("---")
                            st.markdown("**⚔️ 交锋过程：**")
                            for rnd in rounds:
                                round_num = rnd.get('round', '')
                                st.markdown(f"#### 第 {round_num} 轮")

                                # 客户邮件
                                customer_said = rnd.get('customer_said', '')
                                customer_cn = rnd.get('customer_said_cn', '')
                                c_from = rnd.get('customer_from', '')
                                c_to = rnd.get('customer_to', '')
                                c_date = rnd.get('customer_date', '')
                                if customer_said:
                                    header = f"🔵 **客户**"
                                    if c_from or c_to:
                                        header += f"　`{c_from}` → `{c_to}`"
                                    if c_date:
                                        header += f"　_{c_date}_"
                                    st.markdown(header)
                                    st.markdown(
                                        f'<div style="background:#e8f4fd;padding:12px 16px;border-left:4px solid #2196F3;'
                                        f'border-radius:4px;margin:4px 0 8px 0;font-size:14px;line-height:1.7;white-space:pre-wrap">'
                                        f'{customer_said}</div>',
                                        unsafe_allow_html=True
                                    )
                                    if customer_cn:
                                        st.markdown(
                                            f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                            f'margin:0 0 12px 0;font-size:13px;color:#555;line-height:1.6">'
                                            f'💬 {customer_cn}</div>',
                                            unsafe_allow_html=True
                                        )

                                # 我方回复
                                our_resp = rnd.get('our_response', '')
                                our_cn = rnd.get('our_response_cn', '')
                                o_from = rnd.get('our_from', '')
                                o_to = rnd.get('our_to', '')
                                o_date = rnd.get('our_date', '')
                                if our_resp:
                                    header = f"🟢 **我方业务员**"
                                    if o_from or o_to:
                                        header += f"　`{o_from}` → `{o_to}`"
                                    if o_date:
                                        header += f"　_{o_date}_"
                                    st.markdown(header)
                                    st.markdown(
                                        f'<div style="background:#e8f5e9;padding:12px 16px;border-left:4px solid #4CAF50;'
                                        f'border-radius:4px;margin:4px 0 8px 0;font-size:14px;line-height:1.7;white-space:pre-wrap">'
                                        f'{our_resp}</div>',
                                        unsafe_allow_html=True
                                    )
                                    if our_cn:
                                        st.markdown(
                                            f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                            f'margin:0 0 12px 0;font-size:13px;color:#555;line-height:1.6">'
                                            f'💬 {our_cn}</div>',
                                            unsafe_allow_html=True
                                        )

                                # 高光要点
                                highlight = rnd.get('highlight', '')
                                if highlight:
                                    st.success(f"💡 **要点**: {highlight}")

                                st.markdown("---")

                        # 兼容旧格式（original_excerpt）
                        elif convo.get('original_excerpt'):
                            st.markdown("---")
                            st.markdown(
                                f'<div style="background:#fff3e0;padding:12px 16px;border-left:4px solid #FF9800;'
                                f'border-radius:4px;font-size:14px;line-height:1.7;white-space:pre-wrap">'
                                f'{convo["original_excerpt"]}</div>',
                                unsafe_allow_html=True
                            )
                            if convo.get('translation'):
                                st.markdown(
                                    f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                    f'margin:4px 0;font-size:13px;color:#555;line-height:1.6">'
                                    f'💬 {convo["translation"]}</div>',
                                    unsafe_allow_html=True
                                )

                        # 经验总结
                        lesson = convo.get('lesson_learned', '')
                        if lesson:
                            st.info(f"📚 **经验总结**: {lesson}")

            # 策略建议
            strat = profile.get('strategy_recommendation', {})
            if strat:
                st.markdown("### 应对策略")
                st.info(strat.get('approach', ''))

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**✅ 应该做的：**")
                    for item in strat.get('dos', []):
                        st.write(f"- {item}")
                with col2:
                    st.markdown("**❌ 不应该做的：**")
                    for item in strat.get('donts', []):
                        st.write(f"- {item}")

                st.markdown("**📋 建议下一步：**")
                for item in strat.get('next_steps', []):
                    st.write(f"- {item}")

            # 商机
            opps = profile.get('opportunities', [])
            if opps:
                st.markdown("### 商机")
                for opp in opps:
                    priority_color = {"高": "🔴", "中": "🟡", "低": "🟢"}.get(opp.get('priority', ''), '⚪')
                    st.write(f"{priority_color} **[{opp.get('type', '')}]** {opp.get('description', '')} (优先级: {opp.get('priority', '')})")

            # 原始邮件记录
            st.divider()
            st.markdown("### 📬 原始邮件记录")
            threads = get_customer_threads(conn, email_addr)
            if threads:
                for thread in threads[:20]:
                    with st.expander(f"📨 {thread['subject']} ({thread['email_count']}封, {(thread['first_date'] or '')[:10]} ~ {(thread['last_date'] or '')[:10]})"):
                        for em in thread['emails']:
                            body = get_email_text(em.get('body', ''), '')
                            if not body and 'body' not in em:
                                body = ''
                            st.markdown(f"**[{(em['date'] or '')[:19]}]** `{em['from']}` → `{em['to']}`")
                            st.markdown(f"**主题**: {em['subject']}")
                            if body:
                                st.text_area("邮件内容", body[:3000], height=150,
                                             key=f"email_{thread['thread_id']}_{em['date']}_{em['from']}",
                                             disabled=True)
                            st.markdown("---")

    conn.close()


def show_opportunities():
    """商机看板"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("💡 商机看板")

    tab1, tab2, tab3 = st.tabs(["🔴 沉睡客户", "📋 询价未成交", "🎯 全部商机"])

    with tab1:
        dormant = find_dormant_customers(conn)
        if dormant:
            st.write(f"以下客户超过 {DORMANT_MONTHS} 个月未联系：")
            data = [{
                "邮箱": d[0], "姓名": d[1] or "-",
                "公司": d[5] or "-", "国家": d[6] or "-",
                "最后联系": d[3] or "-", "历史邮件数": d[4]
            } for d in dormant]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else:
            st.success("没有沉睡客户")

    with tab2:
        inquired = find_inquired_not_ordered(conn)
        if inquired:
            st.write("以下客户曾询价/要样品但未下单：")
            data = [{"邮箱": d[0], "姓名": d[1] or "-", "域名": d[2]} for d in inquired]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else:
            st.info("未找到符合条件的客户")

    with tab3:
        cursor.execute('''
            SELECT bo.customer_email, cp.customer_name, cp.company_name,
                   bo.opportunity_type, bo.description, bo.priority
            FROM business_opportunities bo
            LEFT JOIN customer_profiles cp ON bo.customer_email = cp.customer_email
            ORDER BY
                CASE bo.priority WHEN '高' THEN 1 WHEN '中' THEN 2 ELSE 3 END,
                bo.created_at DESC
        ''')
        opps = cursor.fetchall()
        if opps:
            data = [{
                "客户邮箱": o[0], "姓名": o[1] or "-", "公司": o[2] or "-",
                "商机类型": o[3], "描述": o[4], "优先级": o[5]
            } for o in opps]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else:
            st.info("暂无商机数据，请先运行AI分析")

    conn.close()


def show_ai_chat():
    """AI对话助手"""
    st.subheader("🤖 AI 知识库助手")
    st.caption("基于邮件分析数据回答你的问题，例如：'John上次投诉了什么？'、'哪些客户买过shampoo？'")

    if not OPENROUTER_API_KEY:
        st.error("请先设置 OPENROUTER_API_KEY 环境变量！")
        return

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    if prompt := st.chat_input("问我任何关于客户的问题..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("正在思考..."):
                response = chat_with_knowledge(prompt)
                st.write(response)
                st.session_state.messages.append({"role": "assistant", "content": response})


def show_export():
    """数据导出"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("📥 数据导出")

    export_type = st.selectbox("选择导出内容", [
        "客户画像总表",
        "商机列表",
        "沉睡客户列表",
        "全部邮件统计"
    ])

    if st.button("生成导出文件"):
        if export_type == "客户画像总表":
            cursor.execute('''
                SELECT customer_email, customer_name, company_name, country,
                       strategy, opportunities, analyzed_at, thread_count, email_count
                FROM customer_profiles ORDER BY email_count DESC
            ''')
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=[
                "邮箱", "姓名", "公司", "国家", "策略建议", "商机",
                "分析时间", "对话数", "邮件数"
            ])
        elif export_type == "商机列表":
            cursor.execute('''
                SELECT bo.customer_email, cp.customer_name, cp.company_name,
                       bo.opportunity_type, bo.description, bo.priority
                FROM business_opportunities bo
                LEFT JOIN customer_profiles cp ON bo.customer_email = cp.customer_email
            ''')
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=["邮箱", "姓名", "公司", "商机类型", "描述", "优先级"])
        elif export_type == "沉睡客户列表":
            dormant = find_dormant_customers(conn)
            df = pd.DataFrame(dormant, columns=["邮箱", "姓名", "域名", "最后联系", "邮件数", "公司", "国家"])
        else:
            cursor.execute('''
                SELECT account, folder, COUNT(*) as cnt,
                       MIN(date) as earliest, MAX(date) as latest
                FROM emails GROUP BY account, folder ORDER BY cnt DESC
            ''')
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=["账号", "文件夹", "邮件数", "最早日期", "最新日期"])

        if not df.empty:
            st.dataframe(df, use_container_width=True)

            excel_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", f"export_{export_type}.xlsx")
            df.to_excel(excel_path, index=False)
            with open(excel_path, "rb") as f:
                st.download_button(
                    label="下载 Excel 文件",
                    data=f,
                    file_name=f"{export_type}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.warning("没有数据可导出")

    conn.close()


if __name__ == "__main__":
    main()
