# 外贸邮件智能分析系统

[English](README_EN.md)

从企业邮箱自动采集邮件，构建客户知识库，利用 AI 进行客户画像分析和商机挖掘。

## 功能特性

- **邮件采集** — 通过 IMAP 协议批量下载企业邮箱邮件，支持多账号、多文件夹，断线自动重连
- **客户知识库** — 自动提取联系人，按类型分类（客户/供应商/物流/平台等），构建对话线程
- **AI 客户分析** — 基于邮件往来记录，生成客户画像、行为偏好、合作策略建议
- **商机看板** — 识别沉睡客户、询盘未下单客户等商机
- **AI 助手** — 基于邮件知识库的智能问答
- **数据导出** — 支持导出客户数据到 Excel
- **邮件浏览** — 全部邮件搜索、筛选、分页浏览

## 系统截图

Web 界面基于 Streamlit 构建，包含仪表盘、邮箱管理、邮件浏览、客户列表、客户详情、商机看板、AI 助手等功能模块。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置邮箱

编辑 `config/settings.py`，设置 IMAP 服务器信息：

```python
IMAP_SERVER = "imaphz.qiye.163.com"
IMAP_PORT = 993
IMAP_USE_SSL = True
```

或者通过 Web 界面的「邮箱账号管理」页面添加邮箱账号。

### 3. 配置 AI（可选）

设置 OpenRouter API Key 用于 AI 分析功能：

```bash
export OPENROUTER_API_KEY="your-api-key"
```

### 4. 启动 Web 界面

```bash
streamlit run web/app.py --server.port 8501
```

### 5. 采集邮件

**方式一：Web 界面操作**

进入「邮箱账号管理」页面，点击「开始拉取邮件」。

**方式二：命令行全量下载**

```bash
python3 run_full_download.py
```

支持断线自动重连，脚本中断后重新运行会自动跳过已下载的邮件。

**方式三：交互式 CLI**

```bash
python3 run.py
```

## 项目结构

```
email-analyzer/
├── config/
│   └── settings.py          # 配置：IMAP 服务器、API Key、数据库路径
├── modules/
│   ├── email_fetcher.py     # 邮件采集：IMAP 连接、下载、断线重连
│   ├── email_parser.py      # 邮件解析：线程重组、客户提取、分类
│   ├── ai_analyzer.py       # AI 分析：客户画像、商机识别
│   └── background_worker.py # 后台任务：指定客户邮件拉取
├── web/
│   └── app.py               # Streamlit Web 界面
├── data/                    # 数据目录（自动创建，已 gitignore）
│   ├── emails.db            # SQLite 数据库
│   └── worker.log           # 下载日志
├── run.py                   # 交互式 CLI
├── run_full_download.py     # 全量下载脚本
└── requirements.txt         # Python 依赖
```

## 技术栈

- **Python 3.9+**
- **SQLite** — 本地数据存储（WAL 模式）
- **Streamlit** — Web 界面
- **IMAP** — 邮件协议
- **Claude API**（via OpenRouter）— AI 分析

## 注意事项

- 邮箱密码存储在 SQLite 数据库中，请确保 `data/` 目录的安全性
- 网易企业邮箱的 IMAP 不支持 SEARCH FROM/TO，系统采用全量拉取 + 本地匹配的方式
- 首次全量下载可能需要较长时间，支持中断后续传
- AI 分析功能需要配置 OpenRouter API Key

## License

MIT
