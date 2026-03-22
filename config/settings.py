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

# Anthropic API 配置
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "emails.db")

# AI 分析配置
AI_MODEL = "claude-sonnet-4-6"
MAX_TOKENS_PER_ANALYSIS = 4096

# 商机挖掘配置
DORMANT_MONTHS = 6  # 超过几个月未联系算沉睡客户

# 公司产品列表（用于AI分析时参考）
COMPANY_PRODUCTS = [
    "hair dye", "hair oil", "hair relaxer", "hair removal cream",
    "hair gel", "hair wax", "shampoo", "lotion",
    "lubricant gel", "mosquito repellent", "baby care products",
    "diaper cream", "hand cream", "foot cream"
]
