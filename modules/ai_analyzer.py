"""AI客户分析引擎 - 通过OpenRouter调用Claude分析客户画像和挖掘商机"""
import json
import sqlite3
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH, OPENROUTER_API_KEY, OPENROUTER_BASE_URL, AI_MODEL, MAX_TOKENS_PER_ANALYSIS, COMPANY_PRODUCTS, DORMANT_MONTHS
from modules.email_fetcher import get_db_conn

from openai import OpenAI
from modules.email_parser import get_customer_threads, get_all_external_customers, get_email_text


def get_ai_config():
    """获取AI配置，优先从数据库读取，fallback到settings.py"""
    from modules.email_fetcher import get_setting
    return {
        'api_key': get_setting('openrouter_api_key', OPENROUTER_API_KEY) or os.environ.get("OPENROUTER_API_KEY", ""),
        'base_url': get_setting('openrouter_base_url', OPENROUTER_BASE_URL),
        'model': get_setting('ai_model', AI_MODEL),
        'max_tokens': int(get_setting('max_tokens', str(MAX_TOKENS_PER_ANALYSIS))),
    }


def get_ai_client():
    """获取OpenRouter AI客户端"""
    config = get_ai_config()
    if not config['api_key']:
        raise ValueError("请在 Web 界面「邮箱账号管理」中配置 OpenRouter API Key！")
    return OpenAI(
        base_url=config['base_url'],
        api_key=config['api_key'],
    )


def test_api_key(api_key=None, base_url=None):
    """测试API Key是否有效，返回 (是否有效, 信息)"""
    if not api_key:
        config = get_ai_config()
        api_key = config['api_key']
        base_url = config['base_url']
    if not api_key:
        return False, "未配置 API Key"
    try:
        client = OpenAI(base_url=base_url or OPENROUTER_BASE_URL, api_key=api_key)
        response = client.chat.completions.create(
            model="openai/gpt-3.5-turbo",
            max_tokens=5,
            messages=[{"role": "user", "content": "hi"}]
        )
        return True, "API Key 有效"
    except Exception as e:
        err = str(e)
        if '401' in err:
            return False, "API Key 无效（401 认证失败）"
        elif '403' in err:
            return False, "API Key 权限不足（403）"
        elif '429' in err:
            return True, "API Key 有效（当前限流中）"
        else:
            return False, f"连接失败: {err[:100]}"


def generate_report_html(profile, email_addr, analyzed_at='', thread_count=0, email_count=0):
    """从 profile JSON 生成完整 HTML 报告（用于缓存，避免重复查询）"""
    # 字段中文映射
    BASIC_LABELS = {
        'name': '姓名', 'company': '公司', 'country': '国家/地区', 'position': '职位',
        'company_type': '公司类型', 'company_scale': '公司规模', 'all_contacts': '所有联系人',
    }
    BEHAVIOR_LABELS = {
        'price_sensitivity': '价格敏感度', 'price_sensitivity_evidence': '价格敏感度依据',
        'decision_pattern': '决策模式', 'decision_evidence': '决策模式依据',
        'payment_preference': '付款方式', 'communication_style': '沟通风格',
        'response_speed': '回复速度', 'order_frequency': '下单频率',
        'average_order_value': '平均订单金额',
    }
    REL_LABELS = {
        'current_status': '当前状态', 'relationship_quality': '关系质量',
        'last_contact_date': '最后联系', 'trust_level': '信任度',
    }

    def _card(title, content, color='#1976D2'):
        return (f'<div style="margin:16px 0">'
                f'<div style="background:{color};color:white;padding:8px 16px;border-radius:8px 8px 0 0;font-size:16px;font-weight:bold">{title}</div>'
                f'<div style="border:1px solid #e0e0e0;border-top:none;border-radius:0 0 8px 8px;padding:12px 16px">{content}</div></div>')

    def _kv_rows(data, labels):
        rows = ''
        for k, v in data.items():
            label = labels.get(k, k)
            if not v or v == '未知':
                continue
            rows += (f'<div style="display:flex;padding:6px 0;border-bottom:1px solid #f5f5f5">'
                     f'<div style="width:140px;font-weight:bold;color:#555;flex-shrink:0">{label}</div>'
                     f'<div style="flex:1;line-height:1.6">{v}</div></div>')
        return rows

    basic = profile.get('basic_info', {})
    h = []

    # 基本信息卡片
    basic_html = _kv_rows(basic, BASIC_LABELS)
    h.append(_card('👤 基本信息', basic_html))

    # 感兴趣的产品
    products = profile.get('products_of_interest', [])
    if products:
        tags = ''.join(f'<span style="display:inline-block;background:#e3f2fd;color:#1565c0;padding:4px 12px;'
                       f'border-radius:16px;margin:3px;font-size:13px">{p}</span>' for p in products)
        h.append(_card('🏷️ 感兴趣的产品', tags, '#0288D1'))

    # 行为画像
    behavior = profile.get('behavior_profile', {})
    if behavior:
        beh_html = _kv_rows(behavior, BEHAVIOR_LABELS)
        h.append(_card('📊 行为画像', beh_html, '#7B1FA2'))

    # 关系状态
    rel = profile.get('relationship_status', {})
    if rel:
        rel_html = _kv_rows(rel, REL_LABELS)
        h.append(_card('🤝 关系状态', rel_html, '#388E3C'))

    # 应对策略
    strat = profile.get('strategy_recommendation', {})
    if strat:
        s = f'<div style="background:#e8f5e9;padding:10px 14px;border-radius:6px;margin-bottom:12px;font-size:14px;line-height:1.6">{strat.get("approach","")}</div>'
        s += '<div style="display:flex;gap:16px;flex-wrap:wrap">'
        s += '<div style="flex:1;min-width:200px"><div style="font-weight:bold;color:#2e7d32;margin-bottom:4px">✅ 应该做</div><ul style="margin:0;padding-left:20px">'
        for item in strat.get('dos', []):
            s += f'<li style="margin:4px 0;line-height:1.5">{item}</li>'
        s += '</ul></div>'
        s += '<div style="flex:1;min-width:200px"><div style="font-weight:bold;color:#c62828;margin-bottom:4px">❌ 不应该做</div><ul style="margin:0;padding-left:20px">'
        for item in strat.get('donts', []):
            s += f'<li style="margin:4px 0;line-height:1.5">{item}</li>'
        s += '</ul></div></div>'
        s += '<div style="margin-top:12px"><div style="font-weight:bold;color:#1565c0;margin-bottom:4px">📋 建议下一步</div><ul style="margin:0;padding-left:20px">'
        for item in strat.get('next_steps', []):
            s += f'<li style="margin:4px 0;line-height:1.5">{item}</li>'
        s += '</ul></div>'
        h.append(_card('🎯 应对策略', s, '#F57C00'))

    # 商机
    opps = profile.get('opportunities', [])
    if opps:
        o = ''
        for opp in opps:
            color = {"高": "#f44336", "中": "#ff9800", "低": "#4caf50"}.get(opp.get('priority', ''), '#999')
            o += (f'<div style="display:flex;align-items:flex-start;padding:8px 0;border-bottom:1px solid #f5f5f5">'
                  f'<span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;margin-right:8px;flex-shrink:0">{opp.get("priority","")}</span>'
                  f'<div><b>{opp.get("type","")}</b>：{opp.get("description","")}</div></div>')
        h.append(_card('💡 商机', o, '#D32F2F'))

    # 关键对话复盘
    convos = profile.get('key_conversations', [])
    if convos:
        c = ''
        for convo in convos:
            c += (f'<details style="margin-bottom:10px;border:1px solid #e0e0e0;border-radius:6px">'
                  f'<summary style="cursor:pointer;padding:10px 14px;background:#fafafa;font-weight:bold;border-radius:6px">'
                  f'📌 {convo.get("topic","")} ({convo.get("date","")})</summary>'
                  f'<div style="padding:10px 14px">')
            c += f'<p><b>概况</b>：{convo.get("summary","")}</p>'
            c += f'<p><b>结果</b>：{convo.get("outcome","")}</p>'
            for rnd in convo.get('negotiation_rounds', []):
                c += f'<div style="font-weight:bold;margin:10px 0 4px">第 {rnd.get("round","")} 轮</div>'
                if rnd.get('customer_said'):
                    c += (f'<div style="background:#e8f4fd;padding:10px 14px;border-left:4px solid #2196F3;'
                          f'border-radius:4px;margin:6px 0;font-size:13px;line-height:1.7;white-space:pre-wrap">'
                          f'🔵 <b>客户</b>：{rnd["customer_said"]}</div>')
                if rnd.get('customer_said_cn'):
                    c += f'<div style="background:#f5f5f5;padding:6px 14px;border-radius:4px;font-size:12px;color:#666;margin:0 0 6px">💬 {rnd["customer_said_cn"]}</div>'
                if rnd.get('our_response'):
                    c += (f'<div style="background:#e8f5e9;padding:10px 14px;border-left:4px solid #4CAF50;'
                          f'border-radius:4px;margin:6px 0;font-size:13px;line-height:1.7;white-space:pre-wrap">'
                          f'🟢 <b>我方</b>：{rnd["our_response"]}</div>')
                if rnd.get('our_response_cn'):
                    c += f'<div style="background:#f5f5f5;padding:6px 14px;border-radius:4px;font-size:12px;color:#666;margin:0 0 6px">💬 {rnd["our_response_cn"]}</div>'
                if rnd.get('highlight'):
                    c += f'<div style="background:#fff3e0;padding:6px 14px;border-radius:4px;margin:6px 0;font-size:13px">💡 <b>要点</b>：{rnd["highlight"]}</div>'
            lesson = convo.get('lesson_learned', '')
            if lesson:
                c += f'<div style="background:#e3f2fd;padding:8px 14px;border-radius:4px;margin-top:8px;font-size:13px">📚 <b>经验总结</b>：{lesson}</div>'
            c += '</div></details>'
        h.append(_card('⚔️ 关键对话复盘', c, '#455A64'))

    return '\n'.join(h)


def _save_report_html(conn, customer_email, profile, analyzed_at, thread_count, email_count):
    """生成并保存 HTML 报告缓存"""
    try:
        html = generate_report_html(profile, customer_email, analyzed_at, thread_count, email_count)
        conn.execute('UPDATE customer_profiles SET report_html = ? WHERE customer_email = ?',
                     (html, customer_email))
        conn.commit()
    except Exception as e:
        print(f"  保存 HTML 报告缓存失败: {e}")


def ai_chat(client, prompt, max_tokens=None):
    """调用AI生成回复"""
    config = get_ai_config()
    response = client.chat.completions.create(
        model=config['model'],
        max_tokens=max_tokens or config['max_tokens'],
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


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


def find_related_emails_by_keyword(conn, keyword):
    """根据关键词找到所有相关的客户邮箱地址"""
    cursor = conn.cursor()
    kw = f"%{keyword.lower()}%"
    cursor.execute("""
        SELECT DISTINCT email FROM customers
        WHERE is_internal = 0
          AND (email LIKE ? OR name LIKE ? OR domain LIKE ?)
        ORDER BY email_count DESC
    """, (kw, kw, kw))
    return [row[0] for row in cursor.fetchall()]


def get_merged_customer_threads(conn, customer_emails, max_threads=80, max_emails_per_thread=20):
    """获取多个客户邮箱的合并对话线程"""
    all_threads = []
    seen_thread_ids = set()

    for ce in customer_emails:
        threads = get_customer_threads(conn, ce)
        for t in threads:
            if t['thread_id'] not in seen_thread_ids:
                seen_thread_ids.add(t['thread_id'])
                all_threads.append(t)

    # 如果通过线程关联找不到够多的，直接从数据库搜索
    if len(all_threads) < 10:
        cursor = conn.cursor()
        email_patterns = [f"%{ce}%" for ce in customer_emails]
        where_clauses = " OR ".join(["from_addr LIKE ? OR to_addr LIKE ? OR cc_addr LIKE ?"] * len(customer_emails))
        params = []
        for ce in customer_emails:
            params.extend([f"%{ce}%"] * 3)

        cursor.execute(f"""
            SELECT id, from_addr, from_name, to_addr, subject, date, body_text, body_html
            FROM emails
            WHERE {where_clauses}
            ORDER BY date DESC
            LIMIT 500
        """, params)

        extra_emails = cursor.fetchall()
        if extra_emails:
            # 把这些邮件按主题粗略分组
            from collections import defaultdict
            from modules.email_parser import clean_subject, get_email_text
            groups = defaultdict(list)
            for row in extra_emails:
                subj = clean_subject(row[4]) or "（无主题）"
                groups[subj[:60]].append({
                    'from': row[1], 'from_name': row[2], 'to': row[3],
                    'subject': row[4], 'date': row[5],
                    'body': get_email_text(row[6], row[7])
                })

            for subj, emails in groups.items():
                if any(t.get('subject','') == subj for t in all_threads):
                    continue
                all_threads.append({
                    'thread_id': f"merged-{subj}",
                    'subject': subj,
                    'email_count': len(emails),
                    'first_date': emails[-1].get('date', ''),
                    'last_date': emails[0].get('date', ''),
                    'emails': emails
                })

    # 按最后日期排序
    all_threads.sort(key=lambda t: t.get('last_date', ''), reverse=True)

    # 限制数量
    all_threads = all_threads[:max_threads]
    for t in all_threads:
        t['emails'] = t['emails'][:max_emails_per_thread]
        for email_item in t['emails']:
            if email_item.get('body') and len(email_item['body']) > 2000:
                email_item['body'] = email_item['body'][:2000] + "\n...(内容已截断)"

    return all_threads


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


def estimate_cost(text_length, model="anthropic/claude-opus-4-6"):
    """预估API调用费用
    OpenRouter Claude Opus 4.6 价格:
    - Input: $5 / 1M tokens
    - Output: $25 / 1M tokens
    英文约 1 token ≈ 4 字符, 中文约 1 token ≈ 2 字符
    """
    # 保守估计：混合语言按 3 字符/token
    input_tokens = text_length / 3
    # prompt 模板约 2000 字符
    input_tokens += 2000 / 3
    # 输出预估：约 8000-12000 tokens
    output_tokens = 10000

    input_cost = (input_tokens / 1_000_000) * 5
    output_cost = (output_tokens / 1_000_000) * 25

    return {
        'input_tokens': int(input_tokens),
        'output_tokens': int(output_tokens),
        'input_cost': input_cost,
        'output_cost': output_cost,
        'total_cost': input_cost + output_cost,
        'total_cost_rmb': (input_cost + output_cost) * 7.2,  # 美元转人民币
    }


def estimate_customer_cost(conn, customer_emails):
    """预估分析某客户（一个或多个邮箱）的费用"""
    if isinstance(customer_emails, str):
        customer_emails = [customer_emails]

    threads = get_merged_customer_threads(conn, customer_emails)
    if not threads:
        return None

    text = format_threads_for_prompt(threads)
    total_emails = sum(t['email_count'] for t in threads)

    cost = estimate_cost(len(text))
    cost['thread_count'] = len(threads)
    cost['email_count'] = total_emails
    cost['text_length'] = len(text)

    return cost


def analyze_customer(conn, customer_email, client=None):
    """使用AI分析单个客户"""
    # 首次分析时自动备份（通过标记避免重复备份）
    if not getattr(analyze_customer, '_backed_up', False):
        try:
            from modules.db_backup import create_backup
            create_backup(reason="before_ai_analysis")
            analyze_customer._backed_up = True
        except Exception as e:
            print(f"自动备份失败（继续执行）: {e}")

    if client is None:
        client = get_ai_client()

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
      "topic": "关键对话主题（如：价格谈判、投诉处理、新品开发等）",
      "date": "大约时间",
      "summary": "整体摘要：这个项目/话题的来龙去脉（3-5句话）",
      "outcome": "最终结果/当前状态",
      "negotiation_rounds": [
        {{
          "round": 1,
          "customer_from": "客户发件邮箱地址",
          "customer_to": "客户发给谁（我方收件邮箱）",
          "customer_date": "客户邮件日期",
          "customer_said": "客户邮件的完整关键段落（英文原文，不要截断，保留完整的2-5句关键原文）",
          "customer_said_cn": "客户原文的中文翻译（完整翻译，不要省略）",
          "our_from": "我方回复的发件邮箱地址",
          "our_to": "我方回复给谁（客户收件邮箱）",
          "our_date": "我方回复日期",
          "our_response": "我方业务员回复的完整关键段落（英文原文，不要截断，保留完整的2-5句关键原文）",
          "our_response_cn": "我方回复的中文翻译（完整翻译，不要省略）",
          "highlight": "这一轮交锋的关键点/学习要点（中文，1句话总结这轮博弈的精髓）"
        }}
      ],
      "lesson_learned": "从这个对话中可以学到的业务技巧或教训（中文，2-3句话）"
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
- 客户可能使用多个邮箱地址（同一公司不同人），邮件中出现的同一公司不同联系人都算该客户的沟通
- 我们公司的业务员也可能有多个邮箱（@meinuo.com），他们的回复都算"我方"
- key_conversations 是最重要的部分！请着重展示双方博弈的过程：
  - 每个关键话题至少提取2-5轮交锋（negotiation_rounds）
  - 重点关注：价格谈判、付款条件讨论、质量投诉处理、新品开发讨论等
  - customer_said 和 our_response 务必摘取原始英文邮件中最关键的原句
  - highlight 要写出每一轮博弈中值得学习的要点
  - lesson_learned 要总结整个对话中业务员可以学到的谈判技巧

以下是邮件记录：
{conversation_text}"""

    try:
        result_text = ai_chat(client, prompt)

        # 提取JSON - 多种格式兼容
        json_match = result_text.strip()
        if '```json' in json_match:
            json_match = json_match.split('```json')[1].split('```')[0]
        elif '```' in json_match:
            json_match = json_match.split('```')[1].split('```')[0]

        # 尝试找到 JSON 对象
        json_match = json_match.strip()
        if not json_match.startswith('{'):
            # 找到第一个 { 开始
            idx = json_match.find('{')
            if idx >= 0:
                json_match = json_match[idx:]

        # 找到最后一个 } 结束
        if json_match:
            last_brace = json_match.rfind('}')
            if last_brace >= 0:
                json_match = json_match[:last_brace + 1]

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
        # 生成并缓存 HTML 报告
        _save_report_html(conn, customer_email, profile, datetime.now().isoformat(), len(threads), total_emails)
        print(f"  ✓ {customer_email}: {summary}")
        return profile

    except Exception as e:
        print(f"  ✗ {customer_email} 分析失败: {e}")
        return None


def analyze_customer_group(conn, keyword, customer_emails=None, client=None):
    """合并分析同一客户/公司的多个邮箱
    keyword: 客户关键词（如 topodom），用于标识和存储
    customer_emails: 相关的邮箱地址列表，如果不传则自动搜索
    """
    if client is None:
        client = get_ai_client()

    init_analysis_tables(conn)

    # 自动查找相关邮箱
    if not customer_emails:
        customer_emails = find_related_emails_by_keyword(conn, keyword)
    if not customer_emails:
        print(f"  未找到与 {keyword} 相关的客户邮箱")
        return None

    print(f"  合并分析 {keyword}，关联邮箱: {customer_emails}")

    # 获取合并的对话线程
    threads = get_merged_customer_threads(conn, customer_emails)
    if not threads:
        print(f"  {keyword}: 无对话数据")
        return None

    total_emails = sum(t['email_count'] for t in threads)
    conversation_text = format_threads_for_prompt(threads)

    if len(conversation_text) < 100:
        print(f"  {keyword}: 邮件内容太少")
        return None

    print(f"  找到 {len(threads)} 个对话线程，共 {total_emails} 封邮件")

    # 构建 prompt（和 analyze_customer 一样，但说明这是多邮箱合并）
    emails_list = ", ".join(customer_emails)
    prompt = f"""你是一位资深的外贸业务分析专家。以下是我们公司（一家个人护理产品生产企业，产品包括：{', '.join(COMPANY_PRODUCTS)}）与客户的所有邮件往来记录。

重要说明：这个客户使用了多个邮箱地址，以下邮箱都属于同一家公司/客户：
{emails_list}
请将所有这些邮箱的沟通合并为一个完整的客户画像。

请仔细分析这些邮件，输出以下信息（用JSON格式）：

{{
  "basic_info": {{
    "name": "主要联系人姓名（可列出多个联系人）",
    "company": "公司名称",
    "country": "国家/地区",
    "position": "职位（如果能推断出）",
    "company_type": "公司类型（贸易商/零售商/分销商/品牌商/其他）",
    "company_scale": "公司规模推测（大/中/小）",
    "all_contacts": "所有联系人及其邮箱的列表说明"
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
      "topic": "关键对话主题（如：价格谈判、投诉处理、新品开发等）",
      "date": "大约时间",
      "summary": "整体摘要：这个项目/话题的来龙去脉（3-5句话）",
      "outcome": "最终结果/当前状态",
      "negotiation_rounds": [
        {{
          "round": 1,
          "customer_from": "客户发件邮箱地址",
          "customer_to": "客户发给谁（我方收件邮箱）",
          "customer_date": "客户邮件日期",
          "customer_said": "客户邮件的完整关键段落（英文原文，不要截断，保留完整的2-5句关键原文）",
          "customer_said_cn": "客户原文的中文翻译（完整翻译，不要省略）",
          "our_from": "我方回复的发件邮箱地址",
          "our_to": "我方回复给谁（客户收件邮箱）",
          "our_date": "我方回复日期",
          "our_response": "我方业务员回复的完整关键段落（英文原文，不要截断，保留完整的2-5句关键原文）",
          "our_response_cn": "我方回复的中文翻译（完整翻译，不要省略）",
          "highlight": "这一轮交锋的关键点/学习要点（中文，1句话总结这轮博弈的精髓）"
        }}
      ],
      "lesson_learned": "从这个对话中可以学到的业务技巧或教训（中文，2-3句话）"
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
- 客户使用多个邮箱地址（{emails_list}），邮件中出现的这些地址都是同一客户
- 我们公司的业务员也可能有多个邮箱（@meinuo.com），他们的回复都算"我方"
- key_conversations 是最重要的部分！请着重展示双方博弈的过程
- 每个关键话题至少提取2-5轮交锋（negotiation_rounds）
- customer_said 和 our_response 务必摘取原始英文邮件中最关键的原句
- highlight 要写出每一轮博弈中值得学习的要点
- lesson_learned 要总结整个对话中业务员可以学到的谈判技巧

以下是邮件记录：
{conversation_text}"""

    try:
        result_text = ai_chat(client, prompt)

        # 提取JSON
        json_match = result_text.strip()
        if '```json' in json_match:
            json_match = json_match.split('```json')[1].split('```')[0]
        elif '```' in json_match:
            json_match = json_match.split('```')[1].split('```')[0]
        json_match = json_match.strip()
        if not json_match.startswith('{'):
            idx = json_match.find('{')
            if idx >= 0:
                json_match = json_match[idx:]
        if json_match:
            last_brace = json_match.rfind('}')
            if last_brace >= 0:
                json_match = json_match[:last_brace + 1]

        try:
            profile = json.loads(json_match.strip())
        except json.JSONDecodeError:
            profile = {"raw_analysis": result_text}

        # 添加关联邮箱信息
        profile['_related_emails'] = customer_emails
        profile['_keyword'] = keyword

        basic = profile.get('basic_info', {})
        summary = f"客户: {basic.get('name', keyword)} | 公司: {basic.get('company', '未知')} | 国家: {basic.get('country', '未知')}"
        strategy = profile.get('strategy_recommendation', {}).get('approach', '')
        opportunities = json.dumps(profile.get('opportunities', []), ensure_ascii=False)

        # 用关键词作为主键存储，同时为每个关联邮箱创建记录
        cursor = conn.cursor()
        primary_email = customer_emails[0]
        profile_json = json.dumps(profile, ensure_ascii=False)

        for ce in customer_emails:
            cursor.execute('''
                INSERT OR REPLACE INTO customer_profiles
                (customer_email, customer_name, company_name, country, profile_json,
                 summary, strategy, opportunities, analyzed_at, thread_count, email_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ce, basic.get('name', ''), basic.get('company', ''),
                basic.get('country', ''), profile_json,
                summary, strategy, opportunities,
                datetime.now().isoformat(), len(threads), total_emails
            ))

        # 存储商机
        cursor.execute('DELETE FROM business_opportunities WHERE customer_email IN ({})'.format(
            ','.join('?' * len(customer_emails))), customer_emails)
        for opp in profile.get('opportunities', []):
            cursor.execute('''
                INSERT INTO business_opportunities
                (customer_email, opportunity_type, description, priority, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (primary_email, opp.get('type', ''), opp.get('description', ''),
                  opp.get('priority', '中'), datetime.now().isoformat()))

        conn.commit()
        # 为每个关联邮箱生成并缓存 HTML 报告
        for ce in customer_emails:
            _save_report_html(conn, ce, profile, datetime.now().isoformat(), len(threads), total_emails)
        print(f"  ✓ {keyword}: {summary}")
        return profile

    except Exception as e:
        print(f"  ✗ {keyword} 合并分析失败: {e}")
        return None


def analyze_all_customers(min_emails=3, max_customers=None):
    """批量分析所有客户"""
    conn = get_db_conn()
    init_analysis_tables(conn)

    client = get_ai_client()

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
        conn = get_db_conn()

    client = get_ai_client()
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
            extra_context += f"\n\n=== {email_addr} 的详细邮件记录 ===\n"
            extra_context += format_threads_for_prompt(threads[:10])

    prompt = f"""你是一位外贸业务知识库助手。以下是公司所有客户的分析数据。
公司产品: {', '.join(COMPANY_PRODUCTS)}

客户知识库:
{kb_text}

{extra_context}

请根据以上知识库回答以下问题（用中文回答）：
{question}"""

    result = ai_chat(client, prompt, max_tokens=2048)
    return result


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'chat':
        question = ' '.join(sys.argv[2:])
        print(chat_with_knowledge(question))
    else:
        # 默认分析前10个客户作为测试
        analyze_all_customers(min_emails=3, max_customers=10)
