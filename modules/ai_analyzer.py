"""AI客户分析引擎 - 使用Claude API分析客户画像和挖掘商机"""
import json
import sqlite3
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH, ANTHROPIC_API_KEY, AI_MODEL, MAX_TOKENS_PER_ANALYSIS, COMPANY_PRODUCTS, DORMANT_MONTHS

import anthropic
from modules.email_parser import get_customer_threads, get_all_external_customers, get_email_text


def init_analysis_tables(conn):
    """创建分析结果表"""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customer_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_email TEXT UNIQUE,
            customer_name TEXT,
            company_name TEXT,
            country TEXT,
            profile_json TEXT,
            summary TEXT,
            strategy TEXT,
            opportunities TEXT,
            analyzed_at TEXT,
            thread_count INTEGER,
            email_count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS business_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_email TEXT,
            opportunity_type TEXT,
            description TEXT,
            priority TEXT,
            created_at TEXT
        )
    ''')
    conn.commit()


def prepare_customer_data(conn, customer_email, max_threads=50, max_emails_per_thread=20):
    """准备客户数据用于AI分析"""
    threads = get_customer_threads(conn, customer_email)

    if not threads:
        return None

    # 限制数据量避免token过多
    threads = threads[:max_threads]
    for thread in threads:
        thread['emails'] = thread['emails'][:max_emails_per_thread]
        for email_item in thread['emails']:
            # 截断过长的邮件正文
            if email_item['body'] and len(email_item['body']) > 2000:
                email_item['body'] = email_item['body'][:2000] + "\n...(内容已截断)"

    return threads


def format_threads_for_prompt(threads):
    """将线程数据格式化为AI分析的文本"""
    text_parts = []
    for i, thread in enumerate(threads):
        text_parts.append(f"\n--- 对话 {i+1}: {thread['subject']} ---")
        text_parts.append(f"时间跨度: {thread['first_date']} ~ {thread['last_date']}")
        for email_item in thread['emails']:
            text_parts.append(f"\n[{email_item['date']}] {email_item['from_name'] or email_item['from']} → {email_item['to']}")
            text_parts.append(f"主题: {email_item['subject']}")
            if email_item['body']:
                text_parts.append(email_item['body'])
    return "\n".join(text_parts)


def analyze_customer(conn, customer_email, client=None):
    """使用AI分析单个客户"""
    if client is None:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    threads = prepare_customer_data(conn, customer_email)
    if not threads:
        print(f"  {customer_email}: 无对话数据，跳过")
        return None

    total_emails = sum(t['email_count'] for t in threads)
    conversation_text = format_threads_for_prompt(threads)

    # 如果内容太少，跳过
    if len(conversation_text) < 100:
        print(f"  {customer_email}: 邮件内容太少，跳过")
        return None

    prompt = f"""你是一位资深的外贸业务分析专家。以下是我们公司（一家个人护理产品生产企业，产品包括：{', '.join(COMPANY_PRODUCTS)}）与客户 {customer_email} 的所有邮件往来记录。

请仔细分析这些邮件，输出以下信息（用JSON格式）：

{{
  "basic_info": {{
    "name": "联系人姓名",
    "company": "公司名称",
    "country": "国家/地区",
    "position": "职位（如果能推断出）",
    "company_type": "公司类型（贸易商/零售商/分销商/品牌商/其他）",
    "company_scale": "公司规模推测（大/中/小）"
  }},
  "products_of_interest": ["该客户感兴趣或采购过的产品列表"],
  "behavior_profile": {{
    "price_sensitivity": "价格敏感度（高/中/低）",
    "price_sensitivity_evidence": "支撑价格敏感度判断的具体证据",
    "decision_pattern": "决策模式（老板直接决策/需要审批流程/团队协商）",
    "decision_evidence": "支撑决策模式判断的具体证据",
    "communication_style": "沟通风格（直接高效/慢热谨慎/关系导向/专业正式）",
    "response_speed": "回复速度（快/一般/慢）",
    "payment_preference": "偏好的付款方式（如T/T, L/C等）",
    "order_frequency": "下单频率推测",
    "average_order_value": "平均订单金额推测"
  }},
  "key_conversations": [
    {{
      "topic": "关键对话主题",
      "date": "大约时间",
      "summary": "对话内容摘要（2-3句话）",
      "outcome": "结果/状态"
    }}
  ],
  "relationship_status": {{
    "current_status": "当前关系状态（活跃客户/沉睡客户/潜在客户/流失客户）",
    "last_contact_date": "最后联系日期",
    "relationship_quality": "关系质量（好/一般/差）",
    "trust_level": "信任度（高/中/低）"
  }},
  "strategy_recommendation": {{
    "approach": "建议的沟通策略（2-3句话）",
    "dos": ["应该做的事情，3-5条"],
    "donts": ["不应该做的事情，2-3条"],
    "next_steps": ["建议的下一步行动，2-3条"]
  }},
  "opportunities": [
    {{
      "type": "商机类型（新产品推荐/重新激活/追加订单/交叉销售）",
      "description": "具体描述",
      "priority": "优先级（高/中/低）"
    }}
  ]
}}

注意：
- 所有分析必须基于邮件内容的实际证据，不要凭空捏造
- 如果某项信息无法从邮件中推断，填写"未知"
- summary 和 strategy 部分请用中文

以下是邮件记录：
{conversation_text}"""

    try:
        response = client.messages.create(
            model=AI_MODEL,
            max_tokens=MAX_TOKENS_PER_ANALYSIS,
            messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.content[0].text

        # 提取JSON
        json_match = result_text
        if '```json' in result_text:
            json_match = result_text.split('```json')[1].split('```')[0]
        elif '```' in result_text:
            json_match = result_text.split('```')[1].split('```')[0]

        try:
            profile = json.loads(json_match.strip())
        except json.JSONDecodeError:
            profile = {"raw_analysis": result_text}

        # 生成摘要
        basic = profile.get('basic_info', {})
        summary = f"客户: {basic.get('name', customer_email)} | 公司: {basic.get('company', '未知')} | 国家: {basic.get('country', '未知')}"

        strategy = ""
        strat = profile.get('strategy_recommendation', {})
        if strat:
            strategy = strat.get('approach', '')

        opportunities = json.dumps(profile.get('opportunities', []), ensure_ascii=False)

        # 存储结果
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO customer_profiles
            (customer_email, customer_name, company_name, country, profile_json,
             summary, strategy, opportunities, analyzed_at, thread_count, email_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            customer_email,
            basic.get('name', ''),
            basic.get('company', ''),
            basic.get('country', ''),
            json.dumps(profile, ensure_ascii=False),
            summary,
            strategy,
            opportunities,
            datetime.now().isoformat(),
            len(threads),
            total_emails
        ))

        # 存储商机
        for opp in profile.get('opportunities', []):
            cursor.execute('''
                INSERT INTO business_opportunities
                (customer_email, opportunity_type, description, priority, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                customer_email,
                opp.get('type', ''),
                opp.get('description', ''),
                opp.get('priority', '中'),
                datetime.now().isoformat()
            ))

        conn.commit()
        print(f"  ✓ {customer_email}: {summary}")
        return profile

    except Exception as e:
        print(f"  ✗ {customer_email} 分析失败: {e}")
        return None


def analyze_all_customers(min_emails=3, max_customers=None):
    """批量分析所有客户"""
    conn = sqlite3.connect(DB_PATH)
    init_analysis_tables(conn)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    customers = get_all_external_customers(conn)
    # 过滤掉邮件太少的客户
    customers = [c for c in customers if c[5] >= min_emails]

    if max_customers:
        customers = customers[:max_customers]

    print(f"\n{'='*50}")
    print(f"开始AI客户分析，共 {len(customers)} 个客户")
    print(f"{'='*50}")

    analyzed = 0
    for i, customer in enumerate(customers):
        email_addr = customer[0]
        print(f"\n[{i+1}/{len(customers)}] 分析 {email_addr}...")
        result = analyze_customer(conn, email_addr, client)
        if result:
            analyzed += 1
        # 控制API调用频率
        time.sleep(1)

    print(f"\n{'='*50}")
    print(f"分析完成！成功分析 {analyzed}/{len(customers)} 个客户")
    print(f"{'='*50}")

    conn.close()
    return analyzed


def find_dormant_customers(conn):
    """查找沉睡客户"""
    cursor = conn.cursor()
    cutoff_date = (datetime.now() - timedelta(days=DORMANT_MONTHS * 30)).isoformat()

    cursor.execute('''
        SELECT c.email, c.name, c.domain, c.last_contact, c.email_count,
               cp.company_name, cp.country
        FROM customers c
        LEFT JOIN customer_profiles cp ON c.email = cp.customer_email
        WHERE c.is_internal = 0
          AND c.email_count >= 3
          AND c.last_contact < ?
        ORDER BY c.email_count DESC
    ''', (cutoff_date,))

    return cursor.fetchall()


def find_inquired_not_ordered(conn):
    """查找询价但未下单的客户（基于关键词判断）"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT e.from_addr, c.name, c.domain
        FROM emails e
        JOIN customers c ON e.from_addr = c.email
        WHERE c.is_internal = 0
          AND (e.body_text LIKE '%sample%' OR e.body_text LIKE '%quotation%'
               OR e.body_text LIKE '%price list%' OR e.body_text LIKE '%MOQ%'
               OR e.body_text LIKE '%inquiry%' OR e.body_text LIKE '%quote%')
          AND e.from_addr NOT IN (
              SELECT DISTINCT e2.from_addr FROM emails e2
              WHERE e2.body_text LIKE '%PO%' OR e2.body_text LIKE '%purchase order%'
                OR e2.body_text LIKE '%payment%' OR e2.body_text LIKE '%shipment%'
                OR e2.body_text LIKE '%BL%' OR e2.body_text LIKE '%invoice%'
          )
    ''')
    return cursor.fetchall()


def chat_with_knowledge(question, conn=None):
    """AI对话助手 - 基于知识库回答问题"""
    if conn is None:
        conn = sqlite3.connect(DB_PATH)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    cursor = conn.cursor()

    # 获取所有客户画像作为知识库
    cursor.execute('''
        SELECT customer_email, profile_json, summary, strategy, opportunities
        FROM customer_profiles
    ''')
    profiles = cursor.fetchall()

    knowledge_base = []
    for p in profiles:
        knowledge_base.append(f"客户: {p[0]}\n摘要: {p[2]}\n策略: {p[3]}\n商机: {p[4]}")

    kb_text = "\n\n---\n\n".join(knowledge_base)

    # 如果问题提到了特定客户，获取详细对话
    mentioned_emails = []
    cursor.execute('SELECT email FROM customers WHERE is_internal = 0')
    for (email_addr,) in cursor.fetchall():
        if email_addr.split('@')[0] in question.lower() or email_addr in question.lower():
            mentioned_emails.append(email_addr)

    extra_context = ""
    for email_addr in mentioned_emails[:3]:
        threads = get_customer_threads(conn, email_addr)
        if threads:
            from modules.email_parser import format_threads_for_prompt
            extra_context += f"\n\n=== {email_addr} 的详细邮件记录 ===\n"
            extra_context += format_threads_for_prompt(threads[:10])

    prompt = f"""你是一位外贸业务知识库助手。以下是公司所有客户的分析数据。
公司产品: {', '.join(COMPANY_PRODUCTS)}

客户知识库:
{kb_text}

{extra_context}

请根据以上知识库回答以下问题（用中文回答）：
{question}"""

    response = client.messages.create(
        model=AI_MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'chat':
        question = ' '.join(sys.argv[2:])
        print(chat_with_knowledge(question))
    else:
        # 默认分析前10个客户作为测试
        analyze_all_customers(min_emails=3, max_customers=10)
