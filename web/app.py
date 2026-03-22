"""Web知识库界面 - 基于Streamlit"""
import streamlit as st
import sqlite3
import json
import os
import sys
import pandas as pd
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH, ANTHROPIC_API_KEY, COMPANY_PRODUCTS, DORMANT_MONTHS
from modules.ai_analyzer import chat_with_knowledge, find_dormant_customers, find_inquired_not_ordered

st.set_page_config(page_title="外贸邮件智能分析系统", page_icon="📧", layout="wide")


def get_db():
    return sqlite3.connect(DB_PATH)


def main():
    st.title("📧 外贸邮件智能分析系统")
    st.caption("个人护理产品 | 客户知识库 & 商机挖掘")

    if not os.path.exists(DB_PATH):
        st.error("数据库不存在，请先运行邮件采集程序！")
        return

    # 侧边栏导航
    page = st.sidebar.selectbox("功能导航", [
        "📊 仪表盘",
        "👥 客户列表",
        "🔍 客户详情",
        "💡 商机看板",
        "🤖 AI 助手",
        "📥 数据导出"
    ])

    if page == "📊 仪表盘":
        show_dashboard()
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


def show_dashboard():
    """仪表盘"""
    conn = get_db()
    cursor = conn.cursor()

    # 基础统计
    col1, col2, col3, col4 = st.columns(4)

    cursor.execute('SELECT COUNT(*) FROM emails')
    total_emails = cursor.fetchone()[0]
    col1.metric("总邮件数", f"{total_emails:,}")

    cursor.execute('SELECT COUNT(*) FROM customers WHERE is_internal = 0')
    total_customers = cursor.fetchone()[0]
    col2.metric("外部联系人", f"{total_customers:,}")

    cursor.execute('SELECT COUNT(*) FROM threads')
    total_threads = cursor.fetchone()[0]
    col3.metric("对话线程", f"{total_threads:,}")

    cursor.execute('SELECT COUNT(*) FROM customer_profiles')
    analyzed = cursor.fetchone()[0]
    col4.metric("已分析客户", f"{analyzed:,}")

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

    # 邮件账号分布
    st.subheader("📬 邮件账号统计")
    cursor.execute('SELECT account, COUNT(*) as cnt FROM emails GROUP BY account ORDER BY cnt DESC')
    account_data = cursor.fetchall()
    if account_data:
        df = pd.DataFrame(account_data, columns=["账号", "邮件数"])
        st.dataframe(df, use_container_width=True)

    conn.close()


def show_customer_list():
    """客户列表"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("👥 客户列表")

    # 筛选器
    col1, col2, col3 = st.columns(3)

    with col1:
        search = st.text_input("搜索（邮箱/姓名/公司）", "")
    with col2:
        country_filter = st.text_input("国家筛选", "")
    with col3:
        min_emails_filter = st.number_input("最少邮件数", min_value=1, value=3)

    query = '''
        SELECT c.email, c.name, c.domain, c.first_contact, c.last_contact, c.email_count,
               cp.company_name, cp.country,
               CASE WHEN cp.id IS NOT NULL THEN '已分析' ELSE '待分析' END as status
        FROM customers c
        LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
        WHERE c.is_internal = 0 AND c.email_count >= ?
    '''
    params = [min_emails_filter]

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
                "首次联系": (r[3] or "-")[:10], "最后联系": (r[4] or "-")[:10],
                "邮件数": r[5], "公司": r[6] or "-", "国家": r[7] or "-",
                "状态": r[8]
            })
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=600)
        st.caption(f"共 {len(results)} 个客户")
    else:
        st.info("没有符合条件的客户")

    conn.close()


def show_customer_detail():
    """客户详情"""
    conn = get_db()
    cursor = conn.cursor()

    st.subheader("🔍 客户详情")

    # 获取已分析的客户列表
    cursor.execute('''
        SELECT customer_email, customer_name, company_name, country
        FROM customer_profiles ORDER BY customer_email
    ''')
    profiles = cursor.fetchall()

    if not profiles:
        st.warning("还没有分析过任何客户，请先运行AI分析。")
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

            # 关键对话
            convos = profile.get('key_conversations', [])
            if convos:
                st.markdown("### 关键对话摘要")
                for convo in convos:
                    with st.expander(f"📌 {convo.get('topic', '对话')} ({convo.get('date', '')})"):
                        st.write(convo.get('summary', ''))
                        st.write(f"**结果**: {convo.get('outcome', '')}")

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

    if not ANTHROPIC_API_KEY:
        st.error("请先设置 ANTHROPIC_API_KEY 环境变量！")
        return

    # 对话历史
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

            # 导出为Excel
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
