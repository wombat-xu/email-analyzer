"""数据库备份与恢复模块"""
import os
import shutil
import sqlite3
from datetime import datetime

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import DB_PATH, BACKUP_DIR, MAX_BACKUPS

try:
    from config.settings import EXTERNAL_BACKUP_DIR
except ImportError:
    EXTERNAL_BACKUP_DIR = None


def create_backup(reason="manual"):
    """创建数据库一致性快照

    使用 SQLite VACUUM INTO 创建快照，不阻塞在线读写。
    本地最多保留 MAX_BACKUPS 份，自动清理最旧的。
    如果配置了外部路径，同时复制一份。

    返回: 备份文件路径
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"emails_{timestamp}_{reason}.db"
    backup_path = os.path.join(BACKUP_DIR, filename)

    print(f"正在创建备份: {filename} ...")
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute(f"VACUUM INTO '{backup_path}'")
    conn.close()

    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
    print(f"备份完成: {filename} ({size_mb:.0f} MB)")

    # 复制到外部路径
    if EXTERNAL_BACKUP_DIR:
        try:
            os.makedirs(EXTERNAL_BACKUP_DIR, exist_ok=True)
            ext_path = os.path.join(EXTERNAL_BACKUP_DIR, filename)
            shutil.copy2(backup_path, ext_path)
            print(f"已复制到外部: {ext_path}")
        except Exception as e:
            print(f"外部备份失败: {e}")

    # 清理旧备份（本地）
    _cleanup_old_backups(BACKUP_DIR)

    return backup_path


def restore_backup(backup_path):
    """从备份恢复数据库

    恢复前会先备份当前数据库（防止恢复错了还能回来）。
    """
    if not os.path.exists(backup_path):
        raise FileNotFoundError(f"备份文件不存在: {backup_path}")

    # 先备份当前数据库
    print("恢复前先备份当前数据库...")
    create_backup(reason="pre_restore")

    # 替换数据库
    print(f"正在恢复: {backup_path} ...")
    # 删除 WAL 文件（恢复后需要干净状态）
    for ext in ['-wal', '-shm']:
        wal_path = DB_PATH + ext
        if os.path.exists(wal_path):
            os.remove(wal_path)

    shutil.copy2(backup_path, DB_PATH)
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"恢复完成！数据库大小: {size_mb:.0f} MB")

    # 验证
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    conn.close()
    print(f"验证通过，邮件数: {count}")
    return count


def list_backups():
    """列出所有可用备份

    返回: [(文件名, 完整路径, 大小MB, 修改时间字符串), ...]
    """
    backups = []

    for directory in [BACKUP_DIR, EXTERNAL_BACKUP_DIR]:
        if not directory or not os.path.exists(directory):
            continue
        for f in sorted(os.listdir(directory), reverse=True):
            if f.startswith("emails_") and f.endswith(".db"):
                path = os.path.join(directory, f)
                size_mb = os.path.getsize(path) / (1024 * 1024)
                mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
                location = "外部" if directory == EXTERNAL_BACKUP_DIR else "本地"
                backups.append((f, path, size_mb, mtime, location))

    return backups


def _cleanup_old_backups(directory):
    """清理旧备份，只保留最新的 MAX_BACKUPS 份"""
    if not os.path.exists(directory):
        return
    files = sorted(
        [f for f in os.listdir(directory) if f.startswith("emails_") and f.endswith(".db")],
        reverse=True
    )
    for old_file in files[MAX_BACKUPS:]:
        old_path = os.path.join(directory, old_file)
        os.remove(old_path)
        print(f"已清理旧备份: {old_file}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        cmd = sys.argv[1]
        if cmd == "backup":
            reason = sys.argv[2] if len(sys.argv) >= 3 else "manual"
            create_backup(reason)
        elif cmd == "list":
            for name, path, size, mtime, loc in list_backups():
                print(f"  [{loc}] {name} ({size:.0f} MB) - {mtime}")
        elif cmd == "restore":
            if len(sys.argv) >= 3:
                restore_backup(sys.argv[2])
            else:
                print("用法: python db_backup.py restore <备份文件路径>")
    else:
        print("用法:")
        print("  python db_backup.py backup [原因]  - 创建备份")
        print("  python db_backup.py list           - 列出备份")
        print("  python db_backup.py restore <路径> - 恢复备份")
