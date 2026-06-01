"""
config/settings.py
Pipeline configuration — all values can be overridden via a .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(".env"))

# ─── PostgreSQL connection ─────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "trading_db")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Naya110212")

# ─── Pipeline parameters ───────────────────────────────────────────────────────
WINDOW_SIZE     = int(os.getenv("WINDOW_SIZE",     "20"))   # trades per window
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", "500000000"))  # PEPE units

# ─── Birdeye API ───────────────────────────────────────────────────────────────
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_CHAIN   = os.getenv("BIRDEYE_CHAIN",   "ethereum")

# PEPE ERC-20 contract address (Ethereum mainnet — official PEPE token)
PEPE_ADDRESS = os.getenv(
    "PEPE_ADDRESS",
    "0x6982508145454ce325ddbe47a25d4ec3d2311933",
)

# Polling interval (seconds between Birdeye API calls)
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))

# Maximum backoff seconds on repeated API errors
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "60"))

# ─── Connection pool ───────────────────────────────────────────────────────────
DB_MIN_CONN = 1
DB_MAX_CONN = 5

# ─── Market Context Layer ──────────────────────────────────────────────────────
# How often (seconds) to refresh market context from CoinPaprika
MARKET_CONTEXT_INTERVAL = int(os.getenv("MARKET_CONTEXT_INTERVAL", "60"))

# ─── Signal Confirmation Layer ─────────────────────────────────────────────────
# Number of most-recent raw signals used for majority voting
SIGNAL_CONFIRMATION_WINDOW = int(os.getenv("SIGNAL_CONFIRMATION_WINDOW", "3"))
