# Foreign Trade Email Intelligence System

[中文](README.md)

Automatically collect emails from enterprise mailboxes, build a customer knowledge base, and leverage AI for customer profiling and business opportunity mining.

## Features

- **Email Collection** — Batch download via IMAP with multi-account support, auto-reconnect on disconnection
- **Customer Knowledge Base** — Auto-extract contacts, classify by type (customer/supplier/logistics/platform), build conversation threads
- **AI Customer Analysis** — Generate customer profiles, behavior preferences, and cooperation strategies from email history
- **Opportunity Board** — Identify dormant customers, inquired-but-not-ordered leads, and other opportunities
- **AI Assistant** — Knowledge-based Q&A powered by email data
- **Data Export** — Export customer data to Excel
- **Email Browser** — Search, filter, and paginate through all imported emails

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Email Accounts

Edit `config/settings.py` to set IMAP server info:

```python
IMAP_SERVER = "imaphz.qiye.163.com"
IMAP_PORT = 993
IMAP_USE_SSL = True
```

Or add accounts through the Web UI's "Email Account Management" page.

### 3. Configure AI (Optional)

Set the OpenRouter API Key for AI analysis features:

```bash
export OPENROUTER_API_KEY="your-api-key"
```

### 4. Launch Web UI

```bash
streamlit run web/app.py --server.port 8501
```

### 5. Collect Emails

**Option A: Web UI**

Go to "Email Account Management" and click "Start Fetching Emails".

**Option B: Command-line full download**

```bash
python3 run_full_download.py
```

Supports auto-reconnect on disconnection. Re-running after interruption will automatically skip already-downloaded emails.

**Option C: Interactive CLI**

```bash
python3 run.py
```

## Project Structure

```
email-analyzer/
├── config/
│   └── settings.py          # Config: IMAP server, API keys, DB path
├── modules/
│   ├── email_fetcher.py     # Email collection: IMAP, download, auto-reconnect
│   ├── email_parser.py      # Email parsing: thread grouping, contact extraction
│   ├── ai_analyzer.py       # AI analysis: customer profiling, opportunity mining
│   └── background_worker.py # Background tasks: per-customer email fetching
├── web/
│   └── app.py               # Streamlit web interface
├── data/                    # Data directory (auto-created, gitignored)
│   ├── emails.db            # SQLite database
│   └── worker.log           # Download logs
├── run.py                   # Interactive CLI
├── run_full_download.py     # Full download script
└── requirements.txt         # Python dependencies
```

## Tech Stack

- **Python 3.9+**
- **SQLite** — Local data storage (WAL mode)
- **Streamlit** — Web interface
- **IMAP** — Email protocol
- **Claude API** (via OpenRouter) — AI analysis

## Notes

- Email passwords are stored in the SQLite database — ensure `data/` directory security
- Netease Enterprise Mail IMAP does not support SEARCH FROM/TO, so the system uses full download + local matching
- Initial full download may take a long time; supports resume after interruption
- AI analysis features require an OpenRouter API Key

## License

MIT
