"""配置文件 - 邮箱账号和API密钥"""
import os

# IMAP 邮件服务器配置
IMAP_SERVER = "imaphz.qiye.163.com"
IMAP_PORT = 993
IMAP_USE_SSL = True

# 邮箱账号列表（先配置一个测试）
# 格式: {"email": "邮箱地址", "password": "密码或授权码"}
EMAIL_ACCOUNTS = [
    # {"email": "sales@yourcompany.com", "password": "your_password"},
]

# OpenRouter API 配置
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "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")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# 数据库路径
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "emails.db")

# 备份配置
BACKUP_DIR = os.path.join(DATA_DIR, "backups")  # 本地备份目录
EXTERNAL_BACKUP_DIR = None  # 外部备份路径，如 "/Volumes/MyDisk/email-backups"
MAX_BACKUPS = 5  # 最多保留几份本地备份

# AI 分析配置
AI_MODEL = "anthropic/claude-opus-4-6"  # OpenRouter 模型名
MAX_TOKENS_PER_ANALYSIS = 16000

# 商机挖掘配置
DORMANT_MONTHS = 6  # 超过几个月未联系算沉睡客户

# 公司产品列表（用于AI分析时参考）
COMPANY_PRODUCTS = [
    "hair dye", "hair oil", "hair relaxer", "hair removal cream",
    "hair gel", "hair wax", "shampoo", "lotion",
    "lubricant gel", "mosquito repellent", "baby care products",
    "diaper cream", "hand cream", "foot cream"
]
