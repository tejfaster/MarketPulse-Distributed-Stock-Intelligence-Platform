# 📈 MarketPulse — Distributed Stock Intelligence Platform

A real-time, end-to-end data engineering pipeline built on a **2-node home cluster** (Mac + Windows), processing live stock market data through a Bronze/Silver/Gold medallion architecture.

---

## 🏗️ Architecture

```
Yahoo Finance API
       │
       ▼
  Kafka Producer (Mac)
  [stock-prices topic]
       │
       ▼
  Bronze Layer (Raw) ✅
  Delta Lake
       │
       ▼
  Silver Layer (Cleaned) ✅
  PySpark + Technical Indicators
  (RSI, MACD, Bollinger Bands, EMA)
       │
       ▼
  Gold Layer (Aggregated) ✅
  PySpark → PostgreSQL
  (stock_prices, technical_signals, stock_summary)
       │
       ▼
  Power BI Dashboard ⏳
```

---

## 🖥️ Cluster Setup

| Node | OS | Role | CPU | Memory |
|------|----|------|-----|--------|
| tejs-MacBook-Pro.local | macOS (ARM) | Spark Master + Kafka Broker + PostgreSQL | 10 cores | 15 GiB |
| LAPTOP-46KBTNB2 (WSL2) | Ubuntu 22.04 | Spark Worker | 8 threads | 14.8 GiB |

### Cluster Configuration
- **Spark Master:** `spark://172.23.181.20:7077`
- **Kafka Broker:** `172.23.181.20:9092`
- **PostgreSQL:** `172.23.181.20:5432`
- **Data Sync:** Syncthing (Mac ↔ Windows)
- **Python Version:** 3.11 (unified across both nodes)
- **Path Resolution:** Symlink on Windows WSL2 maps Mac paths to local paths

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka 3.7.0 (KRaft mode) |
| Processing | Apache Spark 3.5.1 (Standalone Cluster) |
| Storage | Delta Lake 3.1.0 |
| Serving | PostgreSQL 17.4 |
| Orchestration | Apache Airflow ⏳ |
| Dashboard | Power BI ⏳ |
| ML Signals | RAPIDS cuML (CUDA GPU) ⏳ |
| Language | Python 3.11 (Anaconda) |
| Data Sync | Syncthing |

---

## 📁 Project Structure

```
MarketPulse/
├── producers/
│   └── stock_producer.py           # Live stock → Kafka (17 symbols)
├── pipelines/
│   ├── bronze/
│   │   └── kafka_to_bronze.py      # Kafka → Delta Lake (raw)
│   ├── silver/
│   │   └── bronze_to_silver.py     # Delta → RSI, MACD, Bollinger
│   └── gold/
│       └── silver_to_gold.py       # Delta → PostgreSQL (3 tables)
├── dags/
│   └── marketpulse_dag.py          # Airflow DAGs ⏳
├── docs/
│   └── architecture-decisions.md   # Technical decision log
├── config/
│   └── .env.example                # Environment variables template
├── tests/
│   ├── delta_log_checking.py       # Delta log inspection
│   ├── test_spark_delta.py         # Silver layer verification
│   └── debug_cluster.py            # Cluster hostname + path debug
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Java 11
- Apache Spark 3.5.1
- Apache Kafka 3.7.0
- PostgreSQL 17+
- Syncthing (for data sync between nodes)

### 1 — Clone the repo
```bash
git clone https://github.com/tejfaster/MarketPulse-Distributed-Stock-Intelligence-Platform.git
cd MarketPulse-Distributed-Stock-Intelligence-Platform
```

### 2 — Install dependencies
```bash
pip install -r requirements.txt
```

### 3 — Configure environment
```bash
cp config/.env.example config/.env
# Edit .env with your credentials
```

### 4 — Start Kafka (Mac)
```bash
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties
```

### 5 — Start Spark Cluster (Mac)
```bash
/opt/spark/sbin/start-master.sh
```

### 6 — Start Spark Worker (Windows WSL2)
```bash
PYSPARK_PYTHON=/usr/bin/python3.11 /opt/spark/sbin/start-worker.sh spark://172.23.181.20:7077
```

### 7 — Run Stock Producer
```bash
python producers/stock_producer.py
```

### 8 — Run Bronze Layer
```bash
spark-submit \
  --master spark://172.23.181.20:7077 \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  pipelines/bronze/kafka_to_bronze.py
```

### 9 — Run Silver Layer
```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  pipelines/silver/bronze_to_silver.py
```

### 10 — Run Gold Layer
```bash
spark-submit \
  --master spark://172.23.181.20:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  pipelines/gold/silver_to_gold.py
```

---

## 📊 Data Flow

### Bronze Layer ✅
- Raw tick data ingested from Kafka topic `stock-prices`
- Stored as Delta Lake at `MarketPulse-data/bronze/stocks`
- No transformations — raw as received from yfinance

### Silver Layer ✅
- Reads Bronze Delta, cleans and validates OHLCV data
- Technical indicators computed per symbol:
  - **RSI** — Relative Strength Index (14-period)
  - **MACD** — Moving Average Convergence Divergence
  - **Bollinger Bands** — Upper, Lower, Width (20-period)
  - **EMA** — Exponential Moving Averages (12, 26)
- Buy/Sell/Hold signal generated based on RSI thresholds
- Runs on local Spark (`local[*]`) on Mac

### Gold Layer ✅
- Reads Silver Delta on full cluster (Mac Master + Windows Worker)
- Writes three tables to PostgreSQL via JDBC:

| Table | Rows | Description |
|-------|------|-------------|
| `gold.stock_prices` | 18,694 | Historical OHLCV per symbol per date |
| `gold.technical_signals` | 18,694 | RSI, MACD, Bollinger per record |
| `gold.stock_summary` | 17 | Latest snapshot per symbol |

---

## 📈 Stocks Tracked (17 Symbols)

| Category | Symbols |
|----------|---------|
| US Stocks | AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META |
| Indian Stocks | RELIANCE.NS, TCS.NS, INFY.NS |
| Crypto | BTC-USD, ETH-USD |
| Indices | ^NSEI, ^BSESN, ^GSPC, ^DJI, ^IXIC |

---

## 🔧 Cluster Troubleshooting Notes

### Windows WSL2 Python Version
WSL2 defaults to Python 3.10 but Spark requires matching versions across nodes.
```bash
# Fix: set Python 3.11 as system default on Windows WSL2
sudo update-alternatives --set python3 /usr/bin/python3.11
```

### Path Resolution Between Nodes
Mac and Windows use different base paths for the same data. Resolved via symlink:
```bash
# On Windows WSL2
sudo ln -s /home/tejfaster/MarketPulse-Data /Users/tejfaster/Developer/Python/MarketPulse-data
```

### PostgreSQL Network Access
PostgreSQL must accept connections from the cluster network:
```
# postgresql.conf
listen_addresses = '*'

# pg_hba.conf
host    all    all    10.0.0.0/8    md5
```

---

## 🗺️ Roadmap

- [x] Cluster setup (Spark + Kafka)
- [x] Bronze layer ingestion
- [x] Silver layer transformations
- [x] Gold layer → PostgreSQL
- [x] Airflow orchestration
- [ ] Power BI dashboard
- [ ] RAPIDS GPU acceleration

---

## 👤 Author

**Tej Pratap** — Aspiring Data Engineer
- GitHub: [@tejfaster](https://github.com/tejfaster)
- LinkedIn: [Tej Pratap](https://linkedin.com/in/tejfaster)

---

## 📄 License

MIT License — feel free to use and modify.