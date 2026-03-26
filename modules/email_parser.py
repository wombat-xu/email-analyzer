"""邮件解析与对话线程重组模块"""
import sqlite3
import re
import os
import sys
from collections import defaultdict
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH
from modules.email_fetcher import get_db_conn


def init_thread_tables(conn):
    """创建线程和客户表"""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            name TEXT,
            company TEXT,
            domain TEXT,
            country TEXT,
            first_contact TEXT,
            last_contact TEXT,
            email_count INTEGER DEFAULT 0,
            is_internal INTEGER DEFAULT 0,
            contact_type TEXT DEFAULT 'customer'
        )
    ''')
    # 兼容旧表：如果 contact_type 列不存在则添加
    try:
        cursor.execute('ALTER TABLE customers ADD COLUMN contact_type TEXT DEFAULT "customer"')
    except Exception:
        pass
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_key TEXT UNIQUE,
            subject_clean TEXT,
            customer_email TEXT,
            email_count INTEGER DEFAULT 0,
            first_date TEXT,
            last_date TEXT,
            participants TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS email_threads (
            email_id INTEGER,
            thread_id INTEGER,
            FOREIGN KEY (email_id) REFERENCES emails(id),
            FOREIGN KEY (thread_id) REFERENCES threads(id),
            PRIMARY KEY (email_id, thread_id)
        )
    ''')
    conn.commit()


def clean_subject(subject):
    """清理邮件主题（去掉 Re: Fwd: 等前缀）"""
    if not subject:
        return ""
    cleaned = re.sub(r'^(Re|RE|Fw|FW|Fwd|FWD|回复|转发)\s*[:：]\s*', '', subject)
    cleaned = re.sub(r'^(Re|RE|Fw|FW|Fwd|FWD|回复|转发)\s*[:：]\s*', '', cleaned)
    cleaned = re.sub(r'^(Re|RE|Fw|FW|Fwd|FWD|回复|转发)\s*[:：]\s*', '', cleaned)
    return cleaned.strip()


def html_to_text(html):
    """将HTML转换为纯文本"""
    if not html:
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'head']):
        tag.decompose()
    text = soup.get_text(separator='\n')
    lines = [line.strip() for line in text.splitlines()]
    return '\n'.join(line for line in lines if line)


def get_email_text(body_text, body_html):
    """获取邮件文本内容（优先纯文本，否则从HTML提取）"""
    if body_text and body_text.strip():
        return body_text.strip()
    if body_html:
        return html_to_text(body_html)
    return ""


def extract_domain(email_addr):
    """从邮箱地址提取域名"""
    if '@' in email_addr:
        return email_addr.split('@')[1].lower()
    return ""


def identify_internal_emails(conn, company_domains=None):
    """识别内部邮箱（公司域名的邮箱）"""
    cursor = conn.cursor()

    if company_domains is None:
        # 自动检测：根据 account 字段推断公司域名
        cursor.execute('SELECT DISTINCT account FROM emails')
        accounts = [row[0] for row in cursor.fetchall()]
        company_domains = set()
        for acc in accounts:
            domain = extract_domain(acc)
            if domain:
                company_domains.add(domain)

    return company_domains


def classify_contact(email_addr, from_name, domain):
    """基于规则的联系人分类引擎 —— 不用AI，纯规则判断"""
    email_lower = email_addr.lower()
    name_lower = (from_name or '').lower()
    domain_lower = domain.lower()
    local_part = email_lower.split('@')[0] if '@' in email_lower else ''

    # ========== 1. 系统/自动邮件（最先判断）==========
    system_local_parts = [
        'noreply', 'no-reply', 'donotreply', 'do-not-reply', 'do_not_reply',
        'mailer-daemon', 'postmaster', 'bounce', 'auto', 'automail',
        'system', 'admin', 'webmaster', 'root', 'daemon',
        'notification', 'notifications', 'notify', 'alert', 'alerts',
        'newsletter', 'news', 'digest', 'update', 'updates',
        'support', 'help', 'feedback', 'survey',
    ]
    if local_part in system_local_parts:
        return 'platform'

    # ========== 2. B2B平台/电商平台 ==========
    platform_domains = [
        # 阿里系
        'alibaba.com', 'aliexpress.com', 'aliyun.com', '1688.com',
        'service.alibaba.com', 'email.alibaba.com', 'notice.alibaba.com',
        'alerts.globalsources.com',
        # 国际B2B
        'globalsources.com', 'made-in-china.com', 'dhgate.com', 'tradesparq.com',
        # 电商
        'amazon.com', 'ebay.com', 'shopee.com', 'lazada.com', 'jumia.com',
        'wish.com', 'temu.com',
        # 邮件营销/CRM工具
        'xiaomanmail.com', 'xiaoman.cn', 'mailchimp.com', 'sendgrid.net',
        'constantcontact.com', 'hubspot.com', 'zoho.com',
        'marketgate.com', 'ecrm.marketgate.com',
        # 文件传输
        'wetransfer.com', 'dropbox.com',
        # 社交
        'linkedin.com', 'facebook.com', 'twitter.com',
    ]
    for pd in platform_domains:
        if domain_lower == pd or domain_lower.endswith('.' + pd):
            return 'platform'

    # 平台关键词（在域名中）
    platform_kw = ['alibaba', 'aliexpress', 'globalsource', 'xiaoman']
    for kw in platform_kw:
        if kw in domain_lower:
            return 'platform'

    # ========== 3. 货代/物流/船公司 ==========
    logistics_domains = [
        # 国际快递
        'ups.com', 'fedex.com', 'dhl.com', 'dhl.de', 'tnt.com', 'aramex.com',
        'dpd.com', 'gls-group.com',
        # 船公司
        'maersk.com', 'msc.com', 'cma-cgm.com', 'cosco.com', 'oocl.com',
        'evergreen-line.com', 'hapag-lloyd.com', 'yangming.com', 'zim.com',
        'oneline.com', 'hmm21.com', 'pilship.com',
        # 货代
        'kuehne-nagel.com', 'dbschenker.com', 'expeditors.com',
        'tollgroup.com', 'bollore.com', 'geodis.com', 'ceva-logistics.com',
        'sparxlogistics.com',
    ]
    for ld in logistics_domains:
        if domain_lower == ld or domain_lower.endswith('.' + ld):
            return 'logistics'

    logistics_kw = [
        'shipping', 'freight', 'logistics', 'cargo', 'forwarder', 'forwarding',
        'transport', 'express', 'courier', 'customs', 'clearance',
        'warehouse', 'trucking', 'haulage',
    ]
    for kw in logistics_kw:
        if kw in domain_lower:
            return 'logistics'

    # ========== 4. 验厂/检测/认证机构 ==========
    inspection_domains = [
        'bureauveritas.com', 'sgs.com', 'intertek.com', 'tuv.com',
        'ul.com', 'eurofins.com', 'cotecna.com', 'gl-insp.com',
        'bsigroup.com', 'dekra.com', 'dnv.com',
    ]
    for iid in inspection_domains:
        if domain_lower == iid or domain_lower.endswith('.' + iid):
            return 'inspection'

    inspection_kw = ['inspection', 'audit', 'certification', 'testing', 'laboratory']
    for kw in inspection_kw:
        if kw in domain_lower:
            return 'inspection'

    # ========== 5. 广告/展会/媒体 ==========
    ad_domains = [
        'ubm.com', 'informa.com', 'reedexpo.com', 'messefrankfurt.com',
        'koelnmesse.com', 'nurnbergmesse.de',
    ]
    for ad in ad_domains:
        if domain_lower == ad or domain_lower.endswith('.' + ad):
            return 'advertisement'

    ad_kw = [
        'exhibition', 'expo', 'fair', 'trade-show', 'tradeshow',
        'advert', 'promo', 'campaign', 'subscribe', 'unsubscribe',
        'media', 'press', 'editorial', 'magazine',
    ]
    for kw in ad_kw:
        if kw in domain_lower or kw in local_part:
            return 'advertisement'

    # ========== 6. 银行/金融/保险 ==========
    finance_kw = [
        'bank', 'hsbc', 'citibank', 'chase', 'barclays',
        'insurance', 'paypal', 'visa', 'mastercard',
        'pingan', 'icbc', 'boc.cn',
    ]
    for kw in finance_kw:
        if kw in domain_lower:
            return 'finance'

    # ========== 7. 政府/海关/商会 ==========
    gov_kw = [
        '.gov', 'customs', 'chamber', 'commerce', 'council',
        'ccpit', 'mofcom', 'ciqa', 'aqsiq',
    ]
    for kw in gov_kw:
        if kw in domain_lower:
            return 'government'

    # ========== 8. 免费邮箱（可能是客户，也可能是个人） ==========
    free_email_domains = [
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'live.com',
        'aol.com', 'icloud.com', 'msn.com', 'mail.com', 'yandex.com',
        'hotmail.co.uk', 'yahoo.co.uk', 'yahoo.fr', 'hotmail.fr',
        'qq.com', '163.com', '126.com', 'sina.com', 'foxmail.com',
        'rogers.com', 'comcast.net', 'att.net', 'verizon.net',
    ]
    if domain_lower in free_email_domains:
        # 免费邮箱不一定是客户，标记为 potential_customer
        # 但如果有名字且不像自动邮件，还是算客户
        return 'customer'

    # ========== 9. 中国供应商/工厂（.cn / .com.cn 域名） ==========
    if domain_lower.endswith('.cn') or domain_lower.endswith('.com.cn'):
        # 排除 qiye.163.com 等邮箱服务商
        if 'qiye.163' in domain_lower or '163.com' in domain_lower:
            return 'platform'
        return 'supplier'

    # ========== 10. 默认：客户 ==========
    return 'customer'


def build_customer_list(conn):
    """构建客户列表"""
    cursor = conn.cursor()
    init_thread_tables(conn)

    # 获取公司域名
    company_domains = identify_internal_emails(conn)
    print(f"公司域名: {company_domains}")

    # 提取所有发件人
    cursor.execute('''
        SELECT from_addr, from_name, MIN(date), MAX(date), COUNT(*)
        FROM emails
        WHERE from_addr != ''
        GROUP BY from_addr
    ''')

    customers_added = 0
    for from_addr, from_name, first_date, last_date, count in cursor.fetchall():
        domain = extract_domain(from_addr)
        is_internal = 1 if domain in company_domains else 0
        contact_type = 'internal' if is_internal else classify_contact(from_addr, from_name, domain)

        cursor.execute('''
            INSERT OR REPLACE INTO customers (email, name, domain, first_contact, last_contact, email_count, is_internal, contact_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (from_addr, from_name, domain, first_date, last_date, count, is_internal, contact_type))
        customers_added += 1

    # 同样处理收件人中的外部地址
    cursor.execute('SELECT DISTINCT to_addr FROM emails WHERE to_addr != ""')
    for (to_addr_field,) in cursor.fetchall():
        # to_addr 可能包含多个地址
        addrs = re.findall(r'[\w\.\-]+@[\w\.\-]+', to_addr_field)
        for addr in addrs:
            addr = addr.lower()
            domain = extract_domain(addr)
            if domain not in company_domains:
                cursor.execute('''
                    INSERT OR IGNORE INTO customers (email, name, domain, is_internal)
                    VALUES (?, ?, ?, 0)
                ''', (addr, '', domain))

    conn.commit()

    cursor.execute('SELECT COUNT(*) FROM customers WHERE is_internal = 0')
    external_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM customers WHERE is_internal = 1')
    internal_count = cursor.fetchone()[0]

    print(f"客户列表构建完成: {external_count} 个外部联系人, {internal_count} 个内部邮箱")
    return external_count


def build_threads(conn):
    """构建邮件对话线程"""
    cursor = conn.cursor()
    init_thread_tables(conn)

    company_domains = identify_internal_emails(conn)

    # 获取所有邮件
    cursor.execute('''
        SELECT id, message_id, from_addr, to_addr, subject, date,
               in_reply_to, references_header
        FROM emails ORDER BY date
    ''')
    all_emails = cursor.fetchall()

    # 构建 message_id -> email_id 映射
    msgid_to_emailid = {}
    email_data = {}
    for row in all_emails:
        eid, mid, from_addr, to_addr, subject, date, in_reply_to, refs = row
        msgid_to_emailid[mid] = eid
        email_data[eid] = {
            'message_id': mid, 'from': from_addr, 'to': to_addr,
            'subject': subject, 'date': date,
            'in_reply_to': in_reply_to, 'references': refs
        }

    # 使用 Union-Find 算法将相关邮件分组
    parent = {eid: eid for eid in email_data}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # 基于 In-Reply-To 和 References 关联
    for eid, data in email_data.items():
        if data['in_reply_to']:
            ref_id = data['in_reply_to'].strip()
            if ref_id in msgid_to_emailid:
                union(eid, msgid_to_emailid[ref_id])

        if data['references']:
            ref_ids = data['references'].split()
            for ref_id in ref_ids:
                ref_id = ref_id.strip()
                if ref_id in msgid_to_emailid:
                    union(eid, msgid_to_emailid[ref_id])

    # 基于清理后的主题和参与者 进一步关联
    subject_groups = defaultdict(list)
    for eid, data in email_data.items():
        clean_subj = clean_subject(data['subject'])
        if clean_subj:
            # 提取外部参与者
            all_addrs = set()
            all_addrs.add(data['from'])
            external_addrs = re.findall(r'[\w\.\-]+@[\w\.\-]+', data.get('to', ''))
            all_addrs.update(a.lower() for a in external_addrs)
            external = {a for a in all_addrs if extract_domain(a) not in company_domains}

            for ext in external:
                key = (clean_subj.lower()[:80], ext)
                subject_groups[key].append(eid)

    for key, eids in subject_groups.items():
        for i in range(1, len(eids)):
            union(eids[0], eids[i])

    # 收集线程
    thread_groups = defaultdict(list)
    for eid in email_data:
        root = find(eid)
        thread_groups[root].append(eid)

    # 清除旧数据
    cursor.execute('DELETE FROM threads')
    cursor.execute('DELETE FROM email_threads')

    # 存储线程
    thread_count = 0
    for root, eids in thread_groups.items():
        eids.sort(key=lambda e: email_data[e].get('date', ''))

        subjects = [email_data[e]['subject'] for e in eids if email_data[e]['subject']]
        subject_clean = clean_subject(subjects[0]) if subjects else "（无主题）"

        dates = [email_data[e]['date'] for e in eids if email_data[e]['date']]
        first_date = dates[0] if dates else ''
        last_date = dates[-1] if dates else ''

        # 找出客户邮箱（非公司域名的主要参与者）
        participants = set()
        for e in eids:
            participants.add(email_data[e]['from'])
            addrs = re.findall(r'[\w\.\-]+@[\w\.\-]+', email_data[e].get('to', ''))
            participants.update(a.lower() for a in addrs)

        external_participants = [p for p in participants if extract_domain(p) not in company_domains]
        customer_email = external_participants[0] if external_participants else ''

        thread_key = f"{subject_clean[:80]}|{customer_email}"

        cursor.execute('''
            INSERT OR REPLACE INTO threads
            (thread_key, subject_clean, customer_email, email_count, first_date, last_date, participants)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (thread_key, subject_clean, customer_email, len(eids), first_date, last_date,
              ','.join(participants)))

        thread_id = cursor.lastrowid
        for eid in eids:
            cursor.execute('INSERT OR IGNORE INTO email_threads (email_id, thread_id) VALUES (?, ?)',
                           (eid, thread_id))

        thread_count += 1

    conn.commit()
    print(f"对话线程构建完成: {thread_count} 个对话线程")
    return thread_count


def get_customer_threads(conn, customer_email):
    """获取某个客户的所有对话线程及其邮件内容"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT t.id, t.subject_clean, t.email_count, t.first_date, t.last_date
        FROM threads t
        WHERE t.customer_email = ?
        ORDER BY t.last_date DESC
    ''', (customer_email,))
    threads = cursor.fetchall()

    result = []
    for thread_id, subject, count, first_date, last_date in threads:
        cursor.execute('''
            SELECT e.from_addr, e.from_name, e.to_addr, e.subject, e.date, e.body_text, e.body_html
            FROM emails e
            JOIN email_threads et ON e.id = et.email_id
            WHERE et.thread_id = ?
            ORDER BY e.date
        ''', (thread_id,))
        emails = []
        for row in cursor.fetchall():
            text = get_email_text(row[5], row[6])
            emails.append({
                'from': row[0], 'from_name': row[1], 'to': row[2],
                'subject': row[3], 'date': row[4], 'body': text
            })
        result.append({
            'thread_id': thread_id, 'subject': subject,
            'email_count': count, 'first_date': first_date,
            'last_date': last_date, 'emails': emails
        })

    return result


def get_all_external_customers(conn, contact_type='customer'):
    """获取所有外部客户列表，可按类型筛选"""
    cursor = conn.cursor()
    if contact_type == 'all':
        cursor.execute('''
            SELECT c.email, c.name, c.domain, c.first_contact, c.last_contact, c.email_count,
                   (SELECT COUNT(*) FROM threads t WHERE t.customer_email = c.email) as thread_count,
                   c.contact_type
            FROM customers c
            WHERE c.is_internal = 0 AND c.email_count > 0
            ORDER BY c.email_count DESC
        ''')
    else:
        cursor.execute('''
            SELECT c.email, c.name, c.domain, c.first_contact, c.last_contact, c.email_count,
                   (SELECT COUNT(*) FROM threads t WHERE t.customer_email = c.email) as thread_count,
                   c.contact_type
            FROM customers c
            WHERE c.is_internal = 0 AND c.email_count > 0 AND c.contact_type = ?
            ORDER BY c.email_count DESC
        ''', (contact_type,))
    return cursor.fetchall()


def process_all(conn=None):
    """执行完整的解析和线程构建流程"""
    # 操作前自动备份
    try:
        from modules.db_backup import create_backup
        create_backup(reason="before_parse")
    except Exception as e:
        print(f"自动备份失败（继续执行）: {e}")

    if conn is None:
        conn = get_db_conn()
    print("=" * 50)
    print("开始邮件解析与线程重组...")
    print("=" * 50)

    print("\n[1/2] 构建客户列表...")
    build_customer_list(conn)

    print("\n[2/2] 构建对话线程...")
    build_threads(conn)

    print("\n解析完成！")
    conn.close()


if __name__ == '__main__':
    process_all()
