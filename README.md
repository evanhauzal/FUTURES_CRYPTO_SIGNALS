# 🐸 PEPE Real-Time Trading Signal Pipeline

A production-ready, modular pipeline that polls live **PEPE token** trades from
**Birdeye API**, computes windowed features, generates directional trading signals,
persists everything to **PostgreSQL**, and prints colour-coded output to the terminal.

---

## 📐 Architecture

```
Birdeye REST API  (PEPE/ERC-20)
       │
       ▼
 birdeye_client.py     ← async HTTP polling every 1–2 s (auto-fallback to synthetic)
       │
       ▼
 window_processor.py   ← sliding window (deque, configurable size)
       │
       ▼
 feature_builder.py    ← buy/sell PEPE volume, net volume, whale flag …
       │
       ▼
 signal_generator.py   ← BULLISH / BEARISH / NEUTRAL + PEPE trend
       │
       ▼
 postgres_client.py    ← ThreadedConnectionPool → 3 tables
       │
       ▼
 Terminal (colour output)
```

---

## 📁 Project Structure

```
trading-bot/
├── config/
│   └── settings.py              # all config (reads .env)
├── src/
│   ├── ingestion/
│   │   └── websocket_client.py  # Birdeye HTTP polling for PEPE trades
│   ├── processing/
│   │   └── window_processor.py
│   ├── features/
│   │   └── feature_builder.py
│   ├── signals/
│   │   └── signal_generator.py
│   ├── storage/
│   │   └── postgres_client.py
│   └── utils/
│       └── logger.py
├── scripts/
│   └── run_pipeline.py          # entry-point
├── .env.example
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup

### 1 — Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.10 |
| PostgreSQL | ≥ 14 |

### 2 — Create the database

```sql
CREATE DATABASE trading_db;
```

> The pipeline creates tables **automatically** on first run.

### 3 — Configure environment

```bash
cp .env.example .env
# Edit .env with your DB credentials and Birdeye API key
```

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `trading_db` | Database name |
| `DB_USER` | `postgres` | DB user |
| `DB_PASSWORD` | `postgres` | DB password |
| `BIRDEYE_API_KEY` | *(empty)* | **Required** for live PEPE data |
| `BIRDEYE_CHAIN` | `ethereum` | Chain for Birdeye API (PEPE is ERC-20) |
| `PEPE_ADDRESS` | `0x6982508145454ce325ddbe47a25d4ec3d2311933` | PEPE contract address |
| `POLL_INTERVAL` | `2` | Seconds between Birdeye API calls |
| `MAX_BACKOFF` | `60` | Max retry wait on API errors (seconds) |
| `WINDOW_SIZE` | `20` | Trades per signal window |
| `WHALE_THRESHOLD` | `500000000` | Min PEPE token amount to flag a whale |

### 4 — Install dependencies

```bash
pip install -r requirements.txt
```

### 5 — Run

```bash
python scripts/run_pipeline.py
```

---

## 🖥️ Terminal Output

```
[12:01:05] PEPE BUY   12,500,000 tokens  │  BUY=12,500,000  SELL=3,200,000  NET=+9,300,000  │  SIGNAL: BULLISH  TREND: UP
[12:01:07] PEPE SELL   8,000,000 tokens  │  BUY=4,100,000   SELL=8,000,000  NET=-3,900,000  │  SIGNAL: BEARISH  TREND: DOWN
[12:01:09] PEPE BUY    6,000,000 tokens  │  BUY=6,000,000   SELL=5,800,000  NET=+200,000    │  SIGNAL: NEUTRAL  TREND: UP  🐋 WHALE DETECTED
```

Colours:
- 🟢 **GREEN** — BULLISH / UP
- 🔴 **RED** — BEARISH / DOWN
- 🟡 **YELLOW** — NEUTRAL / SIDEWAYS / whale alert

---

## 🗄️ PostgreSQL Tables

### `transactions`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment |
| type | TEXT | BUY or SELL |
| volume | FLOAT | PEPE token amount |
| price | FLOAT | USD price per PEPE |
| timestamp | TIMESTAMP | Trade time |

### `features`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | — |
| timestamp | TIMESTAMP | Window end time |
| buy_volume | FLOAT | Total PEPE bought in window |
| sell_volume | FLOAT | Total PEPE sold in window |
| net_volume | FLOAT | buy − sell |
| trade_count | INT | Trades in window |
| avg_trade_size | FLOAT | Mean PEPE per trade |
| max_trade_size | FLOAT | Largest trade in window |
| whale_flag | BOOLEAN | True if max ≥ WHALE_THRESHOLD |

### `signals`
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | — |
| timestamp | TIMESTAMP | Signal time |
| signal | TEXT | BULLISH / BEARISH / NEUTRAL |
| pepe_trend | TEXT | UP / DOWN / SIDEWAYS |

---

## 🔌 Offline / Development Mode

If `BIRDEYE_API_KEY` is not set **or** `aiohttp` is not installed,
the pipeline automatically falls back to **synthetic PEPE trade generation** (~1 Hz).
This lets you develop and test without a live internet connection.

---

## 📜 Logs

A `trading_pipeline.log` file is written to the working directory with
`DEBUG`-level detail. The console shows `INFO` and above.

---

## 🛑 Stopping

Press **Ctrl-C** — the pipeline closes the DB connection pool gracefully.
