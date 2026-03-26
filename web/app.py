"""Web知识库界面 - 基于Streamlit"""
import streamlit as st
import sqlite3
import json
import os
import sys
import subprocess
import pandas as pd
import time
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


def get_email_date_range(cursor, where_clause="1=1", params=None):
    """获取邮件的真实最早/最晚日期（解析原始日期字符串，过滤异常日期）"""
    from email.utils import parsedate_to_datetime
    from datetime import datetime as dt, timezone
    params = params or []
    now = dt.now(timezone.utc)
    min_date = dt(2000, 1, 1, tzinfo=timezone.utc)
    # 从头尾各取500封采样
    dates = []
    for order in ['id ASC', 'id DESC']:
        cursor.execute(f"SELECT date FROM emails WHERE {where_clause} AND date != '' ORDER BY {order} LIMIT 500", params)
        for r in cursor.fetchall():
            try:
                d = parsedate_to_datetime(r[0])
                # 统一转为 UTC 比较
                if d.tzinfo is None:
                    d = d.replace(tzinfo=timezone.utc)
                else:
                    d = d.astimezone(timezone.utc)
                if min_date <= d <= now:
                    dates.append(d)
            except Exception:
                pass
    if not dates:
        return '-', '-'
    return min(dates).strftime('%Y-%m-%d'), max(dates).strftime('%Y-%m-%d')


def _show_api_status_sidebar():
    """侧边栏显示 API Key 状态（带缓存）"""
    from modules.ai_analyzer import get_ai_config, test_api_key

    st.sidebar.divider()
    config = get_ai_config()

    # 缓存检测结果 5 分钟
    cache_key = 'api_status_cache'
    cache_time_key = 'api_status_time'
    now = time.time()

    if (cache_key in st.session_state and
        cache_time_key in st.session_state and
        now - st.session_state[cache_time_key] < 300):
        ok, msg = st.session_state[cache_key]
    else:
        if config['api_key']:
            ok, msg = test_api_key()
            st.session_state[cache_key] = (ok, msg)
            st.session_state[cache_time_key] = now
        else:
            ok, msg = False, "未配置 API Key"

    if ok:
        st.sidebar.success(f"🟢 AI API 正常")
    else:
        st.sidebar.error(f"🔴 {msg}")

    st.sidebar.caption(f"模型: {config['model'].split('/')[-1]}")


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

    # === 侧边栏 API 状态指示器 ===
    _show_api_status_sidebar()

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
    """邮箱账号管理 - Tabs 分组"""
    from modules.email_fetcher import (add_email_account, get_all_accounts, remove_email_account,
                                       get_running_tasks, get_recent_tasks, cancel_task, delete_old_tasks)

    st.subheader("⚙️ 系统管理")

    tab_acc, tab_sync, tab_task, tab_ai, tab_backup = st.tabs([
        "📮 邮箱账号", "📥 邮件同步", "📋 任务管理", "🤖 AI 配置", "💾 数据库备份"
    ])

    accounts = get_all_accounts()

    # ====== Tab 1: 邮箱账号 ======
    with tab_acc:
        if accounts:
            st.markdown("#### 已配置的邮箱账号")
            # 每个账号一行，带统计和操作
            conn = get_db()
            cursor = conn.cursor()
            for a in accounts:
                acc_email, acc_pwd, acc_imap, acc_name, last_sync, is_active = a
                cursor.execute("SELECT COUNT(*) FROM emails WHERE account = ?", (acc_email,))
                email_count = cursor.fetchone()[0]
                earliest, latest = get_email_date_range(cursor, "account = ?", [acc_email])

                with st.container():
                    c1, c2, c3, c4 = st.columns([3, 2, 3, 2])
                    c1.markdown(f"**{acc_email}**")
                    c1.caption(f"业务员: {acc_name or '-'}　|　IMAP: {acc_imap}")
                    c2.metric("本地邮件数", f"{email_count:,}")
                    c3.caption(f"时间范围: {earliest} ~ {latest}")
                    c3.caption(f"最后同步: {(last_sync or '未同步')[:19]}")
                    c4.write("")  # 占位
                st.divider()
            conn.close()
        else:
            st.info("还没有配置任何邮箱账号，请在下方添加。")

        # 添加新账号
        st.markdown("#### 添加邮箱账号")
        col1, col2 = st.columns(2)
        with col1:
            new_email = st.text_input("邮箱地址", placeholder="sales@meinuo.com")
            new_password = st.text_input("密码/授权码", type="password")
        with col2:
            new_name = st.text_input("业务员姓名", placeholder="张三")
            new_imap = st.text_input("IMAP服务器", value="imaphz.qiye.163.com")

        if st.button("➕ 添加账号", type="primary"):
            if new_email and new_password:
                with st.spinner("正在测试连接..."):
                    try:
                        from modules.email_fetcher import connect_imap
                        mail = connect_imap(new_email, new_password)
                        mail.logout()
                        add_email_account(new_email, new_password, new_name, new_imap)
                        st.success(f"✅ {new_email} 添加成功！连接测试通过。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"连接失败: {e}")
            else:
                st.warning("请填写邮箱地址和密码")

        # 删除账号（需确认）
        if accounts:
            st.divider()
            with st.expander("🗑️ 删除账号（危险操作）"):
                del_email = st.selectbox("选择要删除的账号", [a[0] for a in accounts], key="del_acc")
                confirm_email = st.text_input("输入邮箱地址确认删除", placeholder="请输入完整邮箱地址", key="del_confirm")
                if st.button("确认删除", key="del_btn"):
                    if confirm_email == del_email:
                        remove_email_account(del_email)
                        st.success(f"已删除 {del_email}")
                        st.rerun()
                    else:
                        st.error("输入的邮箱地址不匹配")

    # ====== Tab 2: 邮件同步 ======
    with tab_sync:
        if not accounts:
            st.info("请先在「邮箱账号」标签页添加邮箱账号")
        else:
            # 运行中任务
            running = get_running_tasks()
            if running:
                t = running[-1]
                task_id_r, _, desc, cur, total, text, created = t
                st.markdown("#### 🔄 正在同步")
                elapsed = ""
                try:
                    from datetime import datetime as dt
                    mins = int((dt.now() - dt.fromisoformat(created)).total_seconds() // 60)
                    elapsed = f"（已运行 {mins} 分钟）"
                except Exception:
                    pass
                st.markdown(f"**{desc}** {elapsed}")
                if total > 0:
                    st.progress(min(cur / total, 1.0), text=text or f"{cur}/{total}")
                else:
                    st.info(text or "进行中...")
                rcol1, rcol2 = st.columns([1, 5])
                with rcol1:
                    if st.button("🔄 刷新", key="sync_refresh"):
                        st.rerun()
                with rcol2:
                    if st.button("⏹️ 取消", key="sync_cancel"):
                        cancel_task(task_id_r)
                        st.warning("已取消")
                        st.rerun()
                st.divider()

            # 每个邮箱账号的同步操作
            import subprocess as _sp
            project_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
            log_file = os.path.join(project_dir, 'data', 'worker.log')
            has_running = bool(running)

            conn = get_db()
            cursor = conn.cursor()
            total_all = 0

            for idx, a in enumerate(accounts):
                acc_email, acc_pwd, acc_imap, acc_name, last_sync, _ = a
                cursor.execute("SELECT COUNT(*) FROM emails WHERE account = ?", (acc_email,))
                cnt = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(DISTINCT folder) FROM emails WHERE account = ?", (acc_email,))
                folder_cnt = cursor.fetchone()[0]
                acc_earliest, acc_latest = get_email_date_range(cursor, "account = ?", [acc_email])
                total_all += cnt

                st.markdown(f"#### {acc_email}（{acc_name or '-'}）")
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("本地邮件数", f"{cnt:,}")
                mc2.metric("文件夹数", folder_cnt)
                mc3.caption(f"最早: {acc_earliest}")
                mc3.caption(f"最晚: {acc_latest}")
                mc4.caption(f"最后同步: {(last_sync or '未同步')[:19]}")

                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button(f"🔄 全量同步", key=f"full_{idx}", disabled=has_running):
                        cmd = [sys.executable, os.path.join(project_dir, 'run_full_download.py'), '--account', acc_email]
                        _sp.Popen(cmd, cwd=project_dir, stdout=open(log_file, 'a'), stderr=_sp.STDOUT, start_new_session=True)
                        st.success(f"✅ {acc_email} 全量同步已启动！")
                        time.sleep(1)
                        st.rerun()
                with bc2:
                    if st.button(f"⚡ 增量同步", key=f"incr_{idx}", disabled=has_running):
                        cmd = [sys.executable, os.path.join(project_dir, 'run_incremental_sync.py'), '--account', acc_email]
                        _sp.Popen(cmd, cwd=project_dir, stdout=open(log_file, 'a'), stderr=_sp.STDOUT, start_new_session=True)
                        st.success(f"✅ {acc_email} 增量同步已启动！")
                        time.sleep(1)
                        st.rerun()
                st.divider()

            conn.close()
            if has_running:
                st.caption("有任务在运行中，请等待完成后再提交")
            st.caption(f"本地邮件总数: {total_all:,}")

    # ====== Tab 3: 任务管理 ======
    with tab_task:
        all_tasks = get_recent_tasks(30)
        if all_tasks:
            task_data = []
            for t in all_tasks:
                icon = {"done": "✅", "failed": "❌", "running": "🔄", "cancelled": "⏹️"}.get(t[3], "❓")
                task_data.append({
                    "ID": t[0], "状态": f"{icon} {t[3]}", "类型": t[1],
                    "描述": t[2], "结果": (t[7] or "")[:80],
                    "创建时间": (t[8] or "")[:19], "完成时间": (t[9] or "")[:19]
                })
            st.dataframe(pd.DataFrame(task_data), use_container_width=True, height=400)
            st.caption(f"共 {len(all_tasks)} 条记录")
            if st.button("🗑️ 清理历史任务（保留最近20条）", key="clean_tasks_mgmt"):
                deleted = delete_old_tasks(keep=20)
                st.success(f"已清理 {deleted} 条历史任务")
                st.rerun()
        else:
            st.info("暂无任务记录")

    # ====== Tab 4: AI 配置 ======
    with tab_ai:
        st.caption("配置 OpenRouter API，用于客户 AI 分析功能")

        from modules.ai_analyzer import get_ai_config, test_api_key
        from modules.email_fetcher import get_setting, save_setting

        current_config = get_ai_config()

        ai_col1, ai_col2 = st.columns(2)
        with ai_col1:
            new_api_key = st.text_input(
                "OpenRouter API Key", value=current_config['api_key'] or "",
                type="password", key="ai_api_key", placeholder="sk-or-v1-..."
            )
            new_base_url = st.text_input("API Base URL", value=current_config['base_url'], key="ai_base_url")
        with ai_col2:
            model_options = [
                "anthropic/claude-opus-4-6", "anthropic/claude-sonnet-4-6",
                "anthropic/claude-haiku-4-5-20251001",
                "openai/gpt-4o", "openai/gpt-4o-mini", "google/gemini-2.5-pro-preview",
            ]
            current_model = current_config['model']
            if current_model not in model_options:
                model_options.insert(0, current_model)
            new_model = st.selectbox("AI 模型", model_options,
                                     index=model_options.index(current_model), key="ai_model_select")
            new_max_tokens = st.number_input("每次分析最大 Token",
                                             min_value=1000, max_value=100000,
                                             value=current_config['max_tokens'], step=1000, key="ai_max_tokens")

        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("🔍 检测 Key 有效性", key="test_key"):
                with st.spinner("正在检测..."):
                    ok, msg = test_api_key(api_key=new_api_key, base_url=new_base_url)
                if ok:
                    st.success(f"🟢 {msg}")
                    st.session_state.pop('api_status_cache', None)
                else:
                    st.error(f"🔴 {msg}")
        with btn_col2:
            if st.button("💾 保存配置", type="primary", key="save_ai_config"):
                save_setting('openrouter_api_key', new_api_key)
                save_setting('openrouter_base_url', new_base_url)
                save_setting('ai_model', new_model)
                save_setting('max_tokens', str(new_max_tokens))
                st.success("配置已保存！")
                st.session_state.pop('api_status_cache', None)
                st.rerun()

    # ====== Tab 5: 数据库备份 ======
    with tab_backup:
        from modules.db_backup import create_backup, restore_backup, list_backups

        col1, col2 = st.columns(2)
        with col1:
            if st.button("📸 立即创建备份", type="primary"):
                with st.spinner("正在创建备份..."):
                    path = create_backup(reason="manual")
                st.success(f"备份完成: {os.path.basename(path)}")
                st.rerun()

        backups = list_backups()
        if backups:
            data = [{"文件名": name, "大小": f"{size:.0f} MB", "时间": mtime, "位置": loc}
                    for name, path, size, mtime, loc in backups]
            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)

            with col2:
                restore_options = {name: path for name, path, _, _, _ in backups}
                selected_bk = st.selectbox("选择要恢复的备份", list(restore_options.keys()), key="restore_select")
                if st.button("⚠️ 恢复此备份", key="restore_btn"):
                    with st.spinner("正在恢复..."):
                        count = restore_backup(restore_options[selected_bk])
                    st.success(f"恢复完成！邮件数: {count}")
                    st.rerun()
        else:
            st.info("暂无备份，点击上方按钮创建第一份备份")


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
    """启动后台任务（独立进程，不受页面刷新影响）
    自动检测本地是否有邮件，有则跳过 IMAP 拉取直接分析"""
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

    # === 分析统计概览 ===
    cursor.execute("SELECT COUNT(*) FROM customers WHERE is_internal=0 AND contact_type='customer' AND email_count>=3")
    total_customers = cursor.fetchone()[0]
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.customer_email) FROM customer_profiles cp
        JOIN customers c ON cp.customer_email = c.email
        WHERE c.is_internal=0 AND c.contact_type='customer'
    """)
    analyzed_count = cursor.fetchone()[0]
    pending_count = total_customers - analyzed_count

    mcol1, mcol2, mcol3 = st.columns(3)
    mcol1.metric("符合条件客户", f"{total_customers}")
    mcol2.metric("已分析", f"{analyzed_count}")
    mcol3.metric("待分析", f"{pending_count}")

    # === 后台任务状态 ===
    from modules.email_fetcher import get_running_tasks, get_recent_tasks, cancel_task, delete_old_tasks
    running = get_running_tasks()
    if running:
        for t in running:
            task_id, task_type, desc, cur, total, text, created = t
            st.markdown("#### 🔄 正在进行的后台任务")
            # 计算耗时
            elapsed = ""
            if created:
                try:
                    from datetime import datetime as dt
                    start = dt.fromisoformat(created)
                    mins = int((dt.now() - start).total_seconds() // 60)
                    elapsed = f"（已运行 {mins} 分钟）"
                except Exception:
                    pass
            st.markdown(f"**{desc}** {elapsed}")
            if total > 0:
                st.progress(min(cur / total, 1.0), text=text or f"{cur}/{total}")
            else:
                st.info(text or "进行中...")
            tcol1, tcol2 = st.columns([1, 5])
            with tcol1:
                if st.button("🔄 刷新", key=f"refresh_{task_id}"):
                    st.rerun()
            with tcol2:
                if st.button("⏹️ 取消任务", key=f"cancel_{task_id}"):
                    cancel_task(task_id)
                    st.warning("已取消任务")
                    st.rerun()
        st.divider()

    recent = get_recent_tasks(10)
    done_tasks = [t for t in recent if t[3] in ('done', 'failed', 'cancelled')]
    # 只显示有意义的任务（排除僵尸清理的）
    meaningful = [t for t in done_tasks if t[7] and '进程异常退出' not in (t[7] or '')]
    if meaningful:
        with st.expander(f"📋 最近任务记录（{len(meaningful)} 个）", expanded=False):
            for t in meaningful:
                icon = "✅" if t[3] == 'done' else ("❌" if t[3] == 'failed' else "⏹️")
                st.markdown(f"- {icon} **{t[2]}** — {t[7] or ''} （{(t[9] or '')[:19]}）")
            if st.button("🗑️ 清理历史任务（保留最近20条）", key="clean_tasks"):
                deleted = delete_old_tasks(keep=20)
                st.success(f"已清理 {deleted} 条历史任务")
                st.rerun()
        st.divider()

    # === 一键批量分析 ===
    st.subheader("⚡ 一键批量分析")
    st.caption("自动选择邮件数最多的未分析客户，依次进行 AI 分析（跳过已分析的）")

    bcol1, bcol2 = st.columns([1, 3])
    with bcol1:
        batch_size = st.selectbox("分析数量", [10, 20, 50], key="batch_size")
    with bcol2:
        has_running = bool(running)
        if st.button(f"🚀 批量分析 TOP {batch_size} 待分析客户", type="primary", key="btn_batch", disabled=has_running):
            import subprocess
            project_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
            log_file = os.path.join(project_dir, 'data', 'worker.log')
            cmd = [sys.executable, os.path.join(project_dir, 'modules', 'batch_analyzer.py'), '--limit', str(batch_size)]
            subprocess.Popen(cmd, cwd=project_dir, stdout=open(log_file, 'a'), stderr=subprocess.STDOUT, start_new_session=True)
            st.success(f"✅ 已启动批量分析任务！将依次分析 TOP {batch_size} 未分析客户")
            st.info("任务在后台运行，刷新页面查看进度。每个客户约 1-2 分钟。")
            time.sleep(1)
            st.rerun()
        if has_running:
            st.caption("有任务在运行中，请等待完成后再提交")

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


def _get_cached_date_range(cursor, cache_key, where_clause, params):
    """带 session_state 缓存的日期范围查询"""
    full_key = f"date_range_{cache_key}"
    if full_key in st.session_state:
        return st.session_state[full_key]
    result = get_email_date_range(cursor, where_clause, params)
    st.session_state[full_key] = result
    return result


def show_customer_detail():
    """客户详情 - 支持搜索所有客户，含数据来源说明"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("🔍 客户详情")

    # === 搜索框（统一入口）===
    search = st.text_input("搜索客户（邮箱/姓名/公司/域名）", "", key="detail_search", placeholder="输入关键词搜索...")

    if not search:
        # 未搜索时：显示提示 + 已分析客户快捷选择
        cursor.execute('''
            SELECT customer_email, customer_name, company_name, country
            FROM customer_profiles ORDER BY customer_email
        ''')
        profiles = cursor.fetchall()
        if profiles:
            # 加一个空选项避免默认加载第一个客户
            options = ["-- 请选择或搜索客户 --"] + [f"{p[0]} ({p[1] or ''} - {p[2] or ''})" for p in profiles]
            selected = st.selectbox("已分析客户快捷选择", options)
            if selected == "-- 请选择或搜索客户 --":
                st.info("请在上方搜索框输入关键词，或从下拉列表选择已分析的客户。")
                conn.close()
                return
            email_addr = selected.split(" (")[0]
        else:
            st.info("还没有分析过任何客户。请输入邮箱搜索，或到「TOP客户」页面批量分析。")
            conn.close()
            return
    else:
        kw = f"%{search}%"
        cursor.execute("""
            SELECT c.email, c.name, c.domain, c.email_count,
                   CASE WHEN cp.id IS NOT NULL THEN '✅ 已分析' ELSE '⏳ 待分析' END as status,
                   cp.company_name
            FROM customers c
            LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
            WHERE c.is_internal = 0
              AND (c.email LIKE ? OR c.name LIKE ? OR c.domain LIKE ? OR cp.company_name LIKE ?)
            ORDER BY c.email_count DESC LIMIT 20
        """, (kw, kw, kw, kw))
        results = cursor.fetchall()
        if not results:
            st.warning(f"未找到与「{search}」相关的客户")
            conn.close()
            return
        options = [f"{r[0]} ({r[1] or r[2]}) - {r[3]}封 {r[4]}" for r in results]
        selected = st.selectbox("选择客户", options)
        email_addr = selected.split(" (")[0] if selected else None

    if not email_addr:
        conn.close()
        return

    # === 数据来源说明（带缓存）===
    cursor.execute("SELECT COUNT(*) FROM emails WHERE from_addr LIKE ? OR to_addr LIKE ?",
                   (f"%{email_addr}%", f"%{email_addr}%"))
    total_count = cursor.fetchone()[0]
    earliest, latest = _get_cached_date_range(
        cursor, email_addr,
        "from_addr LIKE ? OR to_addr LIKE ?",
        [f"%{email_addr}%", f"%{email_addr}%"]
    )

    # 按公司邮箱拆分统计
    cursor.execute("SELECT DISTINCT account FROM emails ORDER BY account")
    accounts_list = [r[0] for r in cursor.fetchall()]

    # 检查是否已分析
    cursor.execute('SELECT profile_json, analyzed_at, thread_count, email_count FROM customer_profiles WHERE customer_email = ?', (email_addr,))
    profile_row = cursor.fetchone()
    has_profile = profile_row is not None

    st.divider()

    # 数据来源信息
    if total_count > 0:
        earliest_fmt = earliest if earliest != '-' else "未知"
        latest_fmt = latest if latest != '-' else "未知"
        if has_profile:
            analyzed_at = (profile_row[1] or '')[:19]
            st.markdown(f"### 📊 基于 **{earliest_fmt}** 至 **{latest_fmt}** 的 **{total_count:,}** 封邮件分析所得")
            st.caption(f"分析时间：{analyzed_at}　|　分析线程数：{profile_row[2]}　|　分析邮件数：{profile_row[3]}")
        else:
            st.markdown(f"### 📊 共有 **{total_count:,}** 封相关邮件（{earliest_fmt} ~ {latest_fmt}）")
            st.caption("该客户尚未进行 AI 分析")

        # 按邮箱账号拆分
        account_stats = []
        for acc in accounts_list:
            cursor.execute("SELECT COUNT(*) FROM emails WHERE account = ? AND (from_addr LIKE ? OR to_addr LIKE ?)",
                           (acc, f"%{email_addr}%", f"%{email_addr}%"))
            cnt = cursor.fetchone()[0]
            if cnt > 0:
                acc_e, acc_l = _get_cached_date_range(
                    cursor, f"{email_addr}_{acc}",
                    "account = ? AND (from_addr LIKE ? OR to_addr LIKE ?)",
                    [acc, f"%{email_addr}%", f"%{email_addr}%"]
                )
                account_stats.append({
                    "公司邮箱": acc, "邮件数": cnt,
                    "最早邮件": acc_e, "最晚邮件": acc_l,
                })
        if account_stats:
            st.dataframe(pd.DataFrame(account_stats), use_container_width=True, hide_index=True)

        # === 同域名联系人 ===
        domain = email_addr.split('@')[1] if '@' in email_addr else ''
        if domain:
            cursor.execute("""
                SELECT c.email, c.name, c.email_count,
                       CASE WHEN cp.id IS NOT NULL THEN '✅' ELSE '⏳' END
                FROM customers c
                LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
                WHERE c.domain = ? AND c.email != ? AND c.is_internal = 0
                ORDER BY c.email_count DESC LIMIT 10
            """, (domain, email_addr))
            same_domain = cursor.fetchall()
            if same_domain:
                with st.expander(f"🏢 同公司联系人（@{domain}，共 {len(same_domain)} 人）"):
                    sd_data = [{"邮箱": s[0], "姓名": s[1] or "-", "邮件数": s[2], "分析": s[3]} for s in same_domain]
                    st.dataframe(pd.DataFrame(sd_data), use_container_width=True, hide_index=True)
                    # 合并分析按钮
                    all_emails = [email_addr] + [s[0] for s in same_domain]
                    if st.button(f"🔗 合并分析 @{domain} 全部 {len(all_emails)} 个邮箱", key="merge_domain"):
                        launch_background_task(all_emails, merge_keyword=domain.split('.')[0])
                        st.success(f"已提交合并分析任务！将合并 {len(all_emails)} 个邮箱为一个客户画像。")
                        st.rerun()
    else:
        st.warning("本地数据库中没有该客户的邮件")

    # === 分析/重新分析按钮 ===
    if not has_profile:
        if st.button("🚀 立即分析此客户", type="primary", key="analyze_now"):
            launch_background_task([email_addr])
            st.success("已提交分析任务！刷新页面查看进度。")
            st.rerun()
    else:
        # 已分析 — 用 tabs 展示
        profile = json.loads(profile_row[0])

        tab1, tab2, tab3 = st.tabs(["📋 客户概览", "🎯 关键对话复盘", "📮 原始邮件"])

        with tab1:
            # 基本信息
            basic = profile.get('basic_info', {})
            st.markdown("#### 基本信息")
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
                st.markdown("#### 感兴趣的产品")
                st.write(", ".join(products))

            # 行为画像
            behavior = profile.get('behavior_profile', {})
            st.markdown("#### 行为画像")
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
            st.markdown("#### 关系状态")
            col1, col2 = st.columns(2)
            col1.write(f"**当前状态**: {rel.get('current_status', '未知')}")
            col1.write(f"**关系质量**: {rel.get('relationship_quality', '未知')}")
            col2.write(f"**最后联系**: {rel.get('last_contact_date', '未知')}")
            col2.write(f"**信任度**: {rel.get('trust_level', '未知')}")

            # 策略建议
            strat = profile.get('strategy_recommendation', {})
            if strat:
                st.markdown("#### 应对策略")
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
                st.markdown("#### 商机")
                for opp in opps:
                    priority_color = {"高": "🔴", "中": "🟡", "低": "🟢"}.get(opp.get('priority', ''), '⚪')
                    st.write(f"{priority_color} **[{opp.get('type', '')}]** {opp.get('description', '')} (优先级: {opp.get('priority', '')})")

        with tab2:
            convos = profile.get('key_conversations', [])
            if convos:
                st.caption("展示客户与业务员之间的核心博弈过程，帮助学习谈判技巧")
                for convo in convos:
                    with st.expander(f"📌 {convo.get('topic', '对话')} ({convo.get('date', '')})", expanded=False):
                        st.markdown(f"**📋 概况**: {convo.get('summary', '')}")
                        st.markdown(f"**🏁 结果**: {convo.get('outcome', '')}")

                        rounds = convo.get('negotiation_rounds', [])
                        if rounds:
                            st.markdown("---")
                            st.markdown("**⚔️ 交锋过程：**")
                            for rnd in rounds:
                                round_num = rnd.get('round', '')
                                st.markdown(f"##### 第 {round_num} 轮")

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
                                        f'{customer_said}</div>', unsafe_allow_html=True)
                                    if customer_cn:
                                        st.markdown(
                                            f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                            f'margin:0 0 12px 0;font-size:13px;color:#555;line-height:1.6">'
                                            f'💬 {customer_cn}</div>', unsafe_allow_html=True)

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
                                        f'{our_resp}</div>', unsafe_allow_html=True)
                                    if our_cn:
                                        st.markdown(
                                            f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                            f'margin:0 0 12px 0;font-size:13px;color:#555;line-height:1.6">'
                                            f'💬 {our_cn}</div>', unsafe_allow_html=True)

                                highlight = rnd.get('highlight', '')
                                if highlight:
                                    st.success(f"💡 **要点**: {highlight}")
                                st.markdown("---")

                        elif convo.get('original_excerpt'):
                            st.markdown("---")
                            st.markdown(
                                f'<div style="background:#fff3e0;padding:12px 16px;border-left:4px solid #FF9800;'
                                f'border-radius:4px;font-size:14px;line-height:1.7;white-space:pre-wrap">'
                                f'{convo["original_excerpt"]}</div>', unsafe_allow_html=True)
                            if convo.get('translation'):
                                st.markdown(
                                    f'<div style="background:#f5f5f5;padding:10px 16px;border-radius:4px;'
                                    f'margin:4px 0;font-size:13px;color:#555;line-height:1.6">'
                                    f'💬 {convo["translation"]}</div>', unsafe_allow_html=True)

                        lesson = convo.get('lesson_learned', '')
                        if lesson:
                            st.info(f"📚 **经验总结**: {lesson}")
            else:
                st.info("AI 分析中未提取到关键对话")

        with tab3:
            _show_customer_emails(conn, email_addr)

        # 底部操作区
        st.divider()
        bcol1, bcol2 = st.columns(2)
        with bcol1:
            if st.button("🔄 重新分析此客户", key="re_analyze"):
                launch_background_task([email_addr])
                st.success("已提交重新分析任务！")
                st.rerun()
        with bcol2:
            # 导出分析报告
            report_lines = []
            report_lines.append(f"客户分析报告 - {email_addr}")
            report_lines.append(f"分析时间: {(profile_row[1] or '')[:19]}")
            report_lines.append(f"邮件范围: {earliest_fmt} ~ {latest_fmt}，共 {total_count:,} 封")
            report_lines.append("=" * 60)
            basic = profile.get('basic_info', {})
            report_lines.append(f"\n【基本信息】")
            for k, v in basic.items():
                report_lines.append(f"  {k}: {v}")
            products = profile.get('products_of_interest', [])
            if products:
                report_lines.append(f"\n【感兴趣的产品】\n  {', '.join(products)}")
            behavior = profile.get('behavior_profile', {})
            if behavior:
                report_lines.append(f"\n【行为画像】")
                for k, v in behavior.items():
                    report_lines.append(f"  {k}: {v}")
            rel = profile.get('relationship_status', {})
            if rel:
                report_lines.append(f"\n【关系状态】")
                for k, v in rel.items():
                    report_lines.append(f"  {k}: {v}")
            strat = profile.get('strategy_recommendation', {})
            if strat:
                report_lines.append(f"\n【应对策略】\n  {strat.get('approach', '')}")
                report_lines.append("  应该做:")
                for item in strat.get('dos', []):
                    report_lines.append(f"    - {item}")
                report_lines.append("  不应该做:")
                for item in strat.get('donts', []):
                    report_lines.append(f"    - {item}")
                report_lines.append("  下一步:")
                for item in strat.get('next_steps', []):
                    report_lines.append(f"    - {item}")
            opps = profile.get('opportunities', [])
            if opps:
                report_lines.append(f"\n【商机】")
                for opp in opps:
                    report_lines.append(f"  [{opp.get('priority','')}] {opp.get('type','')}: {opp.get('description','')}")
            report_text = "\n".join(report_lines)
            st.download_button(
                "📥 导出分析报告", report_text,
                file_name=f"客户分析_{email_addr.split('@')[0]}_{earliest_fmt}.txt",
                mime="text/plain", key="export_report"
            )

    # 未分析客户：直接显示原始邮件
    if not has_profile and total_count > 0:
        st.divider()
        st.markdown("### 📮 原始邮件")
        _show_customer_emails(conn, email_addr)

    conn.close()


def _show_customer_emails(conn, email_addr):
    """显示客户的原始邮件线程（带搜索、分页、区分方向）"""
    threads = get_customer_threads(conn, email_addr)
    if not threads:
        st.info("没有找到相关邮件线程")
        return

    # 邮件搜索
    email_search = st.text_input("搜索邮件（主题/内容）", "", key="email_thread_search", placeholder="输入关键词过滤...")
    if email_search:
        kw = email_search.lower()
        filtered = []
        for t in threads:
            if kw in (t.get('subject') or '').lower():
                filtered.append(t)
                continue
            for em in t.get('emails', []):
                if kw in (em.get('body') or '').lower() or kw in (em.get('subject') or '').lower():
                    filtered.append(t)
                    break
        threads = filtered
        st.caption(f"找到 {len(threads)} 个匹配的对话线程")

    if not threads:
        st.info("没有匹配的邮件")
        return

    # 分页
    page_size = 10
    total_threads = len(threads)
    total_pages = max(1, (total_threads + page_size - 1) // page_size)
    if 'detail_email_page' not in st.session_state:
        st.session_state.detail_email_page = 1
    current_page = st.session_state.detail_email_page

    pcol1, pcol2, pcol3 = st.columns([1, 1, 3])
    with pcol1:
        if st.button("上一页", disabled=(current_page <= 1), key="detail_prev"):
            st.session_state.detail_email_page -= 1
            st.rerun()
    with pcol2:
        if st.button("下一页", disabled=(current_page >= total_pages), key="detail_next"):
            st.session_state.detail_email_page += 1
            st.rerun()
    with pcol3:
        st.caption(f"第 {current_page}/{total_pages} 页，共 {total_threads} 个对话线程")

    start = (current_page - 1) * page_size
    for thread in threads[start:start + page_size]:
        with st.expander(f"📨 {thread['subject']} ({thread['email_count']}封, {(thread['first_date'] or '')[:10]} ~ {(thread['last_date'] or '')[:10]})"):
            for em in thread['emails']:
                from_addr = em.get('from', '')
                is_outgoing = 'meinuo.com' in from_addr.lower()
                icon = "🟢" if is_outgoing else "🔵"
                direction = "发出" if is_outgoing else "收到"
                color = "#e8f5e9" if is_outgoing else "#e8f4fd"
                border = "#4CAF50" if is_outgoing else "#2196F3"

                st.markdown(f"{icon} **{direction}** [{(em['date'] or '')[:19]}]　`{from_addr}` → `{em.get('to', '')}`")
                st.markdown(f"**主题**: {em['subject']}")

                body = get_email_text(em.get('body', ''), '')
                if body:
                    st.markdown(
                        f'<div style="background:{color};padding:10px 14px;border-left:4px solid {border};'
                        f'border-radius:4px;font-size:13px;line-height:1.6;white-space:pre-wrap;'
                        f'max-height:400px;overflow-y:auto">{body[:3000]}</div>',
                        unsafe_allow_html=True)
                st.markdown("---")


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
