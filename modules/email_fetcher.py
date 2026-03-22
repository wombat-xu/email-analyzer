"""邮件采集模块 - 通过IMAP协议从网易企业邮箱拉取邮件"""
import imaplib
import email
import sqlite3
import os
import sys
from email.header import decode_header
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import IMAP_SERVER, IMAP_PORT, IMAP_USE_SSL, DB_PATH


def init_database():
    """初始化SQLite数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            account TEXT,
            folder TEXT,
            from_addr TEXT,
            from_name TEXT,
            to_addr TEXT,
            cc_addr TEXT,
            subject TEXT,
            date TEXT,
            body_text TEXT,
            body_html TEXT,
            in_reply_to TEXT,
            references_header TEXT,
            raw_headers TEXT,
            fetched_at TEXT
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_from_addr ON emails(from_addr)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_subject ON emails(subject)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_date ON emails(date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_account ON emails(account)
    ''')
    conn.commit()
    return conn


def decode_mime_header(header_value):
    """解码MIME编码的邮件头"""
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    result = []
    for part, charset in decoded_parts:
        if isinstance(part, bytes):
            try:
                result.append(part.decode(charset or 'utf-8', errors='replace'))
            except (LookupError, UnicodeDecodeError):
                result.append(part.decode('utf-8', errors='replace'))
        else:
            result.append(part)
    return " ".join(result)


def extract_email_address(from_field):
    """从发件人字段提取邮箱地址和姓名"""
    if not from_field:
        return "", ""
    decoded = decode_mime_header(from_field)
    if '<' in decoded and '>' in decoded:
        name = decoded[:decoded.index('<')].strip().strip('"')
        addr = decoded[decoded.index('<')+1:decoded.index('>')]
        return addr.lower(), name
    return decoded.strip().lower(), ""


def get_email_body(msg):
    """提取邮件正文（纯文本和HTML）"""
    body_text = ""
    body_html = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in content_disposition:
                continue
            try:
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                charset = part.get_content_charset() or 'utf-8'
                try:
                    text = payload.decode(charset, errors='replace')
                except (LookupError, UnicodeDecodeError):
                    text = payload.decode('utf-8', errors='replace')

                if content_type == "text/plain":
                    body_text += text
                elif content_type == "text/html":
                    body_html += text
            except Exception:
                continue
    else:
        content_type = msg.get_content_type()
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or 'utf-8'
                try:
                    text = payload.decode(charset, errors='replace')
                except (LookupError, UnicodeDecodeError):
                    text = payload.decode('utf-8', errors='replace')
                if content_type == "text/plain":
                    body_text = text
                elif content_type == "text/html":
                    body_html = text
        except Exception:
            pass

    return body_text, body_html


def connect_imap(email_addr, password):
    """连接到IMAP服务器"""
    print(f"正在连接 {IMAP_SERVER}:{IMAP_PORT} ...")
    if IMAP_USE_SSL:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    else:
        mail = imaplib.IMAP4(IMAP_SERVER, IMAP_PORT)

    print(f"正在登录 {email_addr} ...")
    mail.login(email_addr, password)
    print("登录成功！")
    return mail


def list_folders(mail):
    """列出邮箱中的所有文件夹"""
    status, folders = mail.list()
    folder_names = []
    if status == 'OK':
        for folder in folders:
            decoded = folder.decode() if isinstance(folder, bytes) else folder
            # 提取文件夹名
            parts = decoded.split(' "/" ')
            if len(parts) >= 2:
                folder_names.append(parts[-1].strip('"'))
            else:
                parts = decoded.split(' "." ')
                if len(parts) >= 2:
                    folder_names.append(parts[-1].strip('"'))
    return folder_names


def fetch_emails_from_folder(mail, folder_name, account_email, conn, limit=None):
    """从指定文件夹拉取邮件"""
    cursor = conn.cursor()

    try:
        status, _ = mail.select(f'"{folder_name}"', readonly=True)
        if status != 'OK':
            print(f"  无法打开文件夹: {folder_name}")
            return 0
    except Exception as e:
        print(f"  打开文件夹失败 {folder_name}: {e}")
        return 0

    # 搜索所有邮件
    status, messages = mail.search(None, 'ALL')
    if status != 'OK':
        return 0

    msg_ids = messages[0].split()
    total = len(msg_ids)
    if total == 0:
        return 0

    if limit:
        msg_ids = msg_ids[-limit:]  # 取最近的N封

    print(f"  文件夹 [{folder_name}] 共 {total} 封邮件，本次拉取 {len(msg_ids)} 封")

    fetched = 0
    for i, msg_id in enumerate(msg_ids):
        try:
            status, msg_data = mail.fetch(msg_id, '(RFC822)')
            if status != 'OK':
                continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            # 提取关键信息
            message_id = msg.get('Message-ID', '')
            if not message_id:
                message_id = f"generated-{account_email}-{folder_name}-{msg_id.decode()}"

            # 检查是否已存在
            cursor.execute('SELECT id FROM emails WHERE message_id = ?', (message_id,))
            if cursor.fetchone():
                continue

            from_addr, from_name = extract_email_address(msg.get('From', ''))
            to_addr = decode_mime_header(msg.get('To', ''))
            cc_addr = decode_mime_header(msg.get('Cc', ''))
            subject = decode_mime_header(msg.get('Subject', ''))
            date_str = msg.get('Date', '')
            in_reply_to = msg.get('In-Reply-To', '')
            references = msg.get('References', '')

            body_text, body_html = get_email_body(msg)

            cursor.execute('''
                INSERT OR IGNORE INTO emails
                (message_id, account, folder, from_addr, from_name, to_addr, cc_addr,
                 subject, date, body_text, body_html, in_reply_to, references_header,
                 raw_headers, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                message_id, account_email, folder_name,
                from_addr, from_name, to_addr, cc_addr,
                subject, date_str, body_text, body_html,
                in_reply_to, references, str(dict(msg.items())),
                datetime.now().isoformat()
            ))

            fetched += 1
            if (i + 1) % 100 == 0:
                conn.commit()
                print(f"    已处理 {i+1}/{len(msg_ids)} 封...")

        except Exception as e:
            print(f"    邮件 {msg_id} 处理失败: {e}")
            continue

    conn.commit()
    print(f"  完成！新增 {fetched} 封邮件")
    return fetched


def fetch_all_emails(account_email, password, limit_per_folder=None):
    """拉取一个账号的所有邮件"""
    conn = init_database()
    mail = connect_imap(account_email, password)

    folders = list_folders(mail)
    print(f"\n发现 {len(folders)} 个文件夹: {folders}")

    total_fetched = 0
    # 优先处理的文件夹
    priority_folders = ['INBOX', '已发送', 'Sent Messages', 'Sent']
    other_folders = [f for f in folders if f not in priority_folders]
    ordered_folders = [f for f in priority_folders if f in folders] + other_folders

    for folder in ordered_folders:
        fetched = fetch_emails_from_folder(mail, folder, account_email, conn, limit_per_folder)
        total_fetched += fetched

    mail.logout()
    conn.close()
    print(f"\n===== 总计新增 {total_fetched} 封邮件 =====")
    return total_fetched


def get_email_stats():
    """获取数据库中的邮件统计"""
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stats = {}
    cursor.execute('SELECT COUNT(*) FROM emails')
    stats['total'] = cursor.fetchone()[0]

    cursor.execute('SELECT account, COUNT(*) FROM emails GROUP BY account')
    stats['by_account'] = dict(cursor.fetchall())

    cursor.execute('SELECT folder, COUNT(*) FROM emails GROUP BY folder')
    stats['by_folder'] = dict(cursor.fetchall())

    cursor.execute('SELECT COUNT(DISTINCT from_addr) FROM emails')
    stats['unique_senders'] = cursor.fetchone()[0]

    conn.close()
    return stats


if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 3:
        account = sys.argv[1]
        password = sys.argv[2]
        limit = int(sys.argv[3]) if len(sys.argv) >= 4 else None
        fetch_all_emails(account, password, limit)
    else:
        print("用法: python email_fetcher.py <邮箱地址> <密码> [每个文件夹限制数量]")
        print("示例: python email_fetcher.py sales@company.com password123 1000")
