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
            is_internal INTEGER DEFAULT 0
        )
    ''')
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

        cursor.execute('''
            INSERT OR REPLACE INTO customers (email, name, domain, first_contact, last_contact, email_count, is_internal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (from_addr, from_name, domain, first_date, last_date, count, is_internal))
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


def get_all_external_customers(conn):
    """获取所有外部客户列表"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT c.email, c.name, c.domain, c.first_contact, c.last_contact, c.email_count,
               (SELECT COUNT(*) FROM threads t WHERE t.customer_email = c.email) as thread_count
        FROM customers c
        WHERE c.is_internal = 0 AND c.email_count > 0
        ORDER BY c.email_count DESC
    ''')
    return cursor.fetchall()


def process_all(conn=None):
    """执行完整的解析和线程构建流程"""
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
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
