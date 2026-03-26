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
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")  # 等待60秒
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
    # 邮箱账号管理表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS email_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            password TEXT,
            imap_server TEXT,
            salesperson_name TEXT,
            added_at TEXT,
            last_sync TEXT,
            is_active INTEGER DEFAULT 1
        )
    ''')
    # 同步状态表 - 记录每个账号每个文件夹的拉取情况
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT,
            folder TEXT,
            total_on_server INTEGER DEFAULT 0,
            fetched_count INTEGER DEFAULT 0,
            last_sync TEXT,
            UNIQUE(account, folder)
        )
    ''')
    # 后台任务表 - 记录进行中的任务
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT,
            description TEXT,
            status TEXT DEFAULT 'running',
            progress_current INTEGER DEFAULT 0,
            progress_total INTEGER DEFAULT 0,
            progress_text TEXT,
            result TEXT,
            created_at TEXT,
            finished_at TEXT
        )
    ''')
    conn.commit()
    return conn


def add_email_account(email_addr, password, salesperson_name='', imap_server=None):
    """添加邮箱账号"""
    conn = init_database()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO email_accounts (email, password, imap_server, salesperson_name, added_at, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
    ''', (email_addr, password, imap_server or IMAP_SERVER, salesperson_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_all_accounts():
    """获取所有已配置的邮箱账号"""
    conn = init_database()
    cursor = conn.cursor()
    cursor.execute('SELECT email, password, imap_server, salesperson_name, last_sync, is_active FROM email_accounts WHERE is_active = 1')
    accounts = cursor.fetchall()
    conn.close()
    return accounts


def remove_email_account(email_addr):
    """删除邮箱账号"""
    conn = init_database()
    cursor = conn.cursor()
    cursor.execute('UPDATE email_accounts SET is_active = 0 WHERE email = ?', (email_addr,))
    conn.commit()
    conn.close()


def fetch_customer_from_all_accounts(customer_email):
    """从所有已配置的邮箱账号中拉取某客户的全部邮件"""
    accounts = get_all_accounts()
    if not accounts:
        print("没有配置任何邮箱账号")
        return 0
    total = 0
    for acc_email, acc_pwd, acc_imap, acc_name, _, _ in accounts:
        print(f"\n--- 从 {acc_email} ({acc_name}) 拉取 {customer_email} 的邮件 ---")
        try:
            count = fetch_customer_emails(acc_email, acc_pwd, customer_email)
            total += count
        except Exception as e:
            print(f"  账号 {acc_email} 拉取失败: {e}")
    print(f"\n===== 所有账号共新增 {total} 封关于 {customer_email} 的邮件 =====")
    return total


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
    """连接到IMAP服务器（带 socket 超时）"""
    import socket
    print(f"正在连接 {IMAP_SERVER}:{IMAP_PORT} ...")
    if IMAP_USE_SSL:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT, timeout=120)
    else:
        mail = imaplib.IMAP4(IMAP_SERVER, IMAP_PORT, timeout=120)
    # 设置 socket 级别超时，防止 recv() 永远阻塞
    mail.socket().settimeout(120)

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


def fetch_emails_from_folder(mail, folder_name, account_email, conn, limit=None, progress_callback=None, task_id=None, password=None):
    """从指定文件夹拉取邮件
    progress_callback: 可选回调函数 (current, total, text) 用于报告进度
    task_id: 可选任务ID，用于更新数据库中的任务进度
    password: 可选密码，用于IMAP断线自动重连
    返回: (fetched_count, mail_object) 元组
    """
    cursor = conn.cursor()

    def _reconnect(mail_obj):
        """IMAP断线重连"""
        if not password:
            return None
        try:
            print(f"    IMAP连接断开，正在重连...")
            mail_obj = connect_imap(account_email, password)
            mail_obj.select(f'"{folder_name}"', readonly=True)
            print(f"    重连成功！")
            return mail_obj
        except Exception as re:
            print(f"    重连失败: {re}")
            return None

    try:
        status, _ = mail.select(f'"{folder_name}"', readonly=True)
        if status != 'OK':
            print(f"  无法打开文件夹: {folder_name}")
            return 0, mail
    except Exception as e:
        print(f"  打开文件夹失败 {folder_name}: {e}")
        new_mail = _reconnect(mail)
        if new_mail is None:
            return 0, mail
        mail = new_mail
        try:
            status, _ = mail.select(f'"{folder_name}"', readonly=True)
            if status != 'OK':
                return 0, mail
        except Exception:
            return 0, mail

    # 搜索所有邮件
    try:
        status, messages = mail.search(None, 'ALL')
    except Exception as e:
        print(f"  搜索邮件失败 {folder_name}: {e}")
        new_mail = _reconnect(mail)
        if new_mail is None:
            return 0, mail
        mail = new_mail
        try:
            status, messages = mail.search(None, 'ALL')
        except Exception:
            return 0, mail
    if status != 'OK':
        return 0, mail

    msg_ids = messages[0].split()
    total = len(msg_ids)
    if total == 0:
        return 0, mail

    # 倒序：从最新的邮件开始拉取
    msg_ids = list(reversed(msg_ids))
    if limit:
        msg_ids = msg_ids[:limit]  # 取最近的N封

    to_fetch = len(msg_ids)
    print(f"  文件夹 [{folder_name}] 服务器共 {total} 封，本次拉取 {to_fetch} 封（最新优先）")

    # 记录同步状态
    cursor.execute('''
        INSERT OR REPLACE INTO sync_status (account, folder, total_on_server, fetched_count, last_sync)
        VALUES (?, ?, ?, COALESCE((SELECT fetched_count FROM sync_status WHERE account=? AND folder=?), 0), ?)
    ''', (account_email, folder_name, total, account_email, folder_name, datetime.now().isoformat()))

    # 预加载已有的 message_id 到内存（避免逐条查数据库）
    # 加载本文件夹的已有ID
    cursor.execute('SELECT message_id FROM emails WHERE account = ? AND folder = ?', (account_email, folder_name))
    existing_ids = set(r[0] for r in cursor.fetchall())
    # 也加载全局所有message_id用于跳过跨文件夹重复
    cursor.execute('SELECT message_id FROM emails')
    all_existing_ids = set(r[0] for r in cursor.fetchall())
    estimated_new = to_fetch - len(existing_ids)
    print(f"    该文件夹已有 {len(existing_ids)} 封，全局已有 {len(all_existing_ids)} 封，需下载约 {estimated_new} 封")

    # 如果已有数量 >= 服务器总数的95%，跳过此文件夹（已基本完成）
    if len(existing_ids) >= total * 0.95 and estimated_new < 50:
        print(f"    ✓ 已基本完成，跳过")
        return 0, mail

    # 批量获取 Message-ID 头（每次200封，大幅减少IMAP请求次数）
    BATCH_SIZE = 200
    fetched = 0
    skipped = 0
    consecutive_errors = 0
    need_download = []  # 需要下载完整内容的 (msg_id, message_id)

    print(f"    批量扫描 Message-ID（每批 {BATCH_SIZE} 封）...")
    for batch_start in range(0, to_fetch, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, to_fetch)
        batch_ids = msg_ids[batch_start:batch_end]
        batch_str = b','.join(batch_ids)

        try:
            status, batch_data = mail.fetch(batch_str, '(BODY[HEADER.FIELDS (MESSAGE-ID)])')
        except Exception as fetch_err:
            new_mail = _reconnect(mail)
            if new_mail is None:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    print(f"    连续 {consecutive_errors} 次批量获取失败，放弃此文件夹")
                    break
                continue
            mail = new_mail
            consecutive_errors = 0
            try:
                status, batch_data = mail.fetch(batch_str, '(BODY[HEADER.FIELDS (MESSAGE-ID)])')
            except Exception:
                continue
        if status != 'OK':
            continue
        consecutive_errors = 0

        # 解析批量返回的 Message-ID
        batch_idx = 0
        for item in batch_data:
            if isinstance(item, tuple) and len(item) >= 2:
                raw = item[1]
                mid = ''
                if isinstance(raw, bytes):
                    text = raw.decode('utf-8', errors='replace')
                    for line in text.split('\n'):
                        if 'message-id' in line.lower():
                            mid = line.split(':', 1)[-1].strip()
                            break
                # 从响应中提取IMAP序号
                seq_num = None
                if isinstance(item[0], bytes):
                    seq_str = item[0].decode('utf-8', errors='replace')
                    parts = seq_str.split()
                    if parts:
                        try:
                            seq_num = parts[0].encode()
                        except Exception:
                            pass
                if not mid and seq_num:
                    mid = f"generated-{account_email}-{folder_name}-{seq_num.decode()}"
                elif not mid:
                    batch_idx += 1
                    continue

                if mid in existing_ids or mid in all_existing_ids:
                    skipped += 1
                else:
                    if seq_num:
                        need_download.append((seq_num, mid))
                batch_idx += 1

        if (batch_end) % 2000 == 0 or batch_end == to_fetch:
            progress_text = f"[{folder_name}] 扫描 {batch_end}/{to_fetch}，跳过 {skipped}，待下载 {len(need_download)}"
            print(f"    {progress_text}")
            if task_id:
                cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                               (batch_end, to_fetch, progress_text, task_id))
                conn.commit()

    print(f"    扫描完成！跳过 {skipped}，需下载 {len(need_download)} 封")

    # 逐封下载需要的邮件
    for dl_idx, (msg_id, mid) in enumerate(need_download):
        try:
            try:
                status, msg_data = mail.fetch(msg_id, '(RFC822)')
            except Exception as fetch_err:
                new_mail = _reconnect(mail)
                if new_mail is None:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        print(f"    连续 {consecutive_errors} 次失败，放弃剩余下载")
                        break
                    continue
                mail = new_mail
                consecutive_errors = 0
                try:
                    status, msg_data = mail.fetch(msg_id, '(RFC822)')
                except Exception:
                    continue
            if status != 'OK':
                continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            message_id = msg.get('Message-ID', '') or mid
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
            existing_ids.add(message_id)
            consecutive_errors = 0

            if fetched % 50 == 0:
                try:
                    conn.commit()
                except Exception as ce:
                    print(f"    COMMIT ERROR: {ce}")

        except Exception as e:
            if 'locked' in str(e).lower():
                print(f"    DB LOCKED: {e}")
            continue

        if (dl_idx + 1) % 50 == 0:
            conn.commit()
            progress_text = f"[{folder_name}] 下载 {dl_idx+1}/{len(need_download)}，已入库 {fetched}"
            print(f"    {progress_text}")
            if progress_callback:
                progress_callback(dl_idx + 1, len(need_download), progress_text)
            if task_id:
                cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                               (dl_idx + 1, len(need_download), progress_text, task_id))
                conn.commit()

    conn.commit()
    # 更新同步状态
    cursor.execute('''
        UPDATE sync_status SET fetched_count = fetched_count + ?, last_sync = ?
        WHERE account = ? AND folder = ?
    ''', (fetched, datetime.now().isoformat(), account_email, folder_name))
    conn.commit()
    print(f"  完成！新增 {fetched} 封邮件")
    return fetched, mail


def fetch_all_emails(account_email, password, limit_per_folder=None, task_id=None):
    """拉取一个账号的所有邮件（支持IMAP断线自动重连）"""
    conn = init_database()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 等待30秒而不是立即失败
    mail = connect_imap(account_email, password)

    folders = list_folders(mail)
    print(f"\n发现 {len(folders)} 个文件夹: {folders}")

    total_fetched = 0
    # 优先处理的文件夹
    priority_folders = ['INBOX', '已发送', 'Sent Messages', 'Sent']
    other_folders = [f for f in folders if f not in priority_folders]
    ordered_folders = [f for f in priority_folders if f in folders] + other_folders

    for folder in ordered_folders:
        try:
            fetched, mail = fetch_emails_from_folder(
                mail, folder, account_email, conn, limit_per_folder,
                password=password, task_id=task_id
            )
            total_fetched += fetched
        except Exception as e:
            print(f"  文件夹 {folder} 处理失败: {e}")
            try:
                mail = connect_imap(account_email, password)
            except Exception:
                print(f"  重连失败，跳过剩余文件夹")
                break

    try:
        mail.logout()
    except Exception:
        pass
    conn.close()
    print(f"\n===== 总计新增 {total_fetched} 封邮件 =====")
    return total_fetched


def fetch_customer_emails(account_email, password, customer_email, task_id=None, search_keywords=None):
    """针对特定客户，全量拉取邮件并本地筛选。
    因为网易企业邮箱 IMAP SEARCH FROM/TO 不可用，只能全量拉取后在本地按关键词匹配。
    为避免重复拉取已有邮件，会跳过已存在的邮件。
    """
    conn = init_database()
    mail = connect_imap(account_email, password)
    folders = list_folders(mail)
    cursor = conn.cursor()

    # 构建本地匹配关键词
    match_keywords = set()
    match_keywords.add(customer_email.lower())
    if '@' in customer_email:
        domain = customer_email.split('@')[1].lower()
        prefix = customer_email.split('@')[0].lower()
        match_keywords.add(domain)
        if len(prefix) >= 4:
            match_keywords.add(prefix)
    if search_keywords:
        match_keywords.update(k.lower() for k in search_keywords)

    print(f"\n全量拉取 {account_email}，本地匹配关键词: {match_keywords}")
    print(f"（网易企业邮箱不支持IMAP SEARCH，改用全量拉取+本地筛选）")

    total_fetched = 0
    total_matched = 0
    total_scanned = 0

    # 优先处理收件箱和已发送
    priority = ['INBOX', '&XfJT0ZAB-']
    ordered = [f for f in priority if f in folders] + [f for f in folders if f not in priority]

    for fi, folder_name in enumerate(ordered):
        try:
            status, _ = mail.select(f'"{folder_name}"', readonly=True)
            if status != 'OK':
                continue
        except Exception:
            # 断线重连
            try:
                mail = connect_imap(account_email, password)
                status, _ = mail.select(f'"{folder_name}"', readonly=True)
                if status != 'OK':
                    continue
            except Exception:
                continue

        try:
            status, messages = mail.search(None, 'ALL')
            if status != 'OK' or not messages[0]:
                continue
        except Exception:
            try:
                mail = connect_imap(account_email, password)
                mail.select(f'"{folder_name}"', readonly=True)
                status, messages = mail.search(None, 'ALL')
                if status != 'OK' or not messages[0]:
                    continue
            except Exception:
                continue

        msg_ids = messages[0].split()
        folder_total = len(msg_ids)
        # 倒序（最新优先）
        msg_ids = list(reversed(msg_ids))

        folder_matched = 0
        folder_fetched = 0

        for mi, msg_id in enumerate(msg_ids):
            try:
                # 先用 HEADER 快速获取头信息，判断是否匹配
                try:
                    status, header_data = mail.fetch(msg_id, '(BODY[HEADER.FIELDS (FROM TO CC)])')
                except Exception:
                    # 断线重连
                    try:
                        mail = connect_imap(account_email, password)
                        mail.select(f'"{folder_name}"', readonly=True)
                        status, header_data = mail.fetch(msg_id, '(BODY[HEADER.FIELDS (FROM TO CC)])')
                    except Exception:
                        continue
                if status != 'OK':
                    continue

                header_text = ''
                if header_data and header_data[0] and len(header_data[0]) > 1:
                    raw = header_data[0][1]
                    header_text = raw.decode('utf-8', errors='replace').lower()

                # 检查是否匹配任何关键词
                matched = any(kw in header_text for kw in match_keywords)
                if not matched:
                    continue

                folder_matched += 1

                # 匹配到了，拉取完整邮件
                status, msg_data = mail.fetch(msg_id, '(RFC822)')
                if status != 'OK':
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                message_id = msg.get('Message-ID', '')
                if not message_id:
                    message_id = f"generated-{account_email}-{folder_name}-{msg_id.decode()}"

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
                folder_fetched += 1

                if folder_fetched % 20 == 0:
                    conn.commit()

            except Exception as e:
                continue

            # 每500封更新一次进度
            if (mi + 1) % 500 == 0:
                progress_text = f"{account_email} [{folder_name}] 扫描 {mi+1}/{folder_total}，匹配 {folder_matched}，新增 {folder_fetched}"
                print(f"    {progress_text}")
                if task_id:
                    cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                                   (mi + 1, folder_total, progress_text, task_id))
                    conn.commit()

        conn.commit()
        total_scanned += folder_total
        total_matched += folder_matched
        total_fetched += folder_fetched

        if folder_matched > 0:
            print(f"  [{folder_name}] 扫描 {folder_total} 封，匹配 {folder_matched}，新增 {folder_fetched}")

        if task_id:
            progress_text = f"{account_email}: 已扫描 {fi+1}/{len(ordered)} 个文件夹（{total_scanned}封），匹配 {total_matched}，新增 {total_fetched}"
            cursor.execute('UPDATE tasks SET progress_current=?, progress_total=?, progress_text=? WHERE id=?',
                           (fi + 1, len(ordered), progress_text, task_id))
            conn.commit()

    try:
        mail.logout()
    except Exception:
        pass
    conn.close()
    print(f"\n===== {account_email}: 扫描 {total_scanned} 封，匹配 {total_matched}，新增 {total_fetched} =====")
    return total_fetched


def create_task(description, task_type='fetch'):
    """创建后台任务记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tasks (task_type, description, status, created_at)
        VALUES (?, ?, 'running', ?)
    ''', (task_type, description, datetime.now().isoformat()))
    task_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return task_id


def finish_task(task_id, result=''):
    """完成后台任务"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE tasks SET status='done', result=?, finished_at=? WHERE id=?
    ''', (result, datetime.now().isoformat(), task_id))
    conn.commit()
    conn.close()


def fail_task(task_id, error=''):
    """标记任务失败"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE tasks SET status='failed', result=?, finished_at=? WHERE id=?
    ''', (error, datetime.now().isoformat(), task_id))
    conn.commit()
    conn.close()


def cleanup_zombie_tasks():
    """清理所有stuck为running状态的僵尸任务"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE tasks SET status='failed', result='进程异常退出（自动清理）', finished_at=?
        WHERE status='running'
    """, (datetime.now().isoformat(),))
    cleaned = cursor.rowcount
    conn.commit()
    conn.close()
    return cleaned


def get_running_tasks():
    """获取正在运行的任务"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, task_type, description, progress_current, progress_total, progress_text, created_at FROM tasks WHERE status='running'")
    tasks = cursor.fetchall()
    conn.close()
    return tasks


def get_recent_tasks(limit=10):
    """获取最近的任务（含已完成）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, task_type, description, status, progress_current, progress_total, progress_text, result, created_at, finished_at FROM tasks ORDER BY id DESC LIMIT ?", (limit,))
    tasks = cursor.fetchall()
    conn.close()
    return tasks


def get_sync_status():
    """获取所有账号的同步状态"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT account, folder, total_on_server, fetched_count, last_sync
        FROM sync_status ORDER BY account, folder
    ''')
    rows = cursor.fetchall()
    conn.close()
    return rows


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
