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
  Bronze Layer (Raw)
  Delta Lake / Parquet
       │
       ▼
  Silver Layer (Cleaned)
  PySpark + Technical Indicators
  (RSI, MACD, Bollinger Bands)
       │
       ▼
  Gold Layer (Aggregated)
  dbt + cuML Signals
       │
       ▼
  PostgreSQL + Power BI Dashboard
```

---

## 🖥️ Cluster Setup

| Node | OS | Role | CPU | Memory |
|------|----|------|-----|--------|
| tejs-MacBook-Pro | macOS | Spark Master + Kafka Broker | 10 cores | — |
| LAPTOP-46KBTNB2 | Windows 10 | Spark Worker | 8 threads | 14.8 GiB |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka 3.7.0 (KRaft mode) |
| Processing | Apache Spark 3.5.1 (Standalone Cluster) |
| Storage | Delta Lake / PostgreSQL |
| Transformation | dbt |
| ML Signals | RAPIDS cuML (CUDA GPU) |
| Orchestration | Apache Airflow |
| Dashboard | Power BI |
| Language | Python 3.12 |

---

## 📁 Project Structure

```
marketpulse/
├── producers/
│   └── stock_producer.py        # Live stock → Kafka
├── pipelines/
│   ├── bronze/
│   │   └── kafka_to_bronze.py   # Kafka → raw storage
│   ├── silver/
│   │   └── bronze_to_silver.py  # Clean + indicators
│   └── gold/
│       └── silver_to_gold.py    # Signals + aggregates
├── dags/
│   └── marketpulse_dag.py       # Airflow DAGs
├── db/
│   └── schema.sql               # PostgreSQL schema
├── dashboard/
│   └── marketpulse.pbix         # Power BI dashboard
├── config/
│   └── .env.example             # Environment variables template
├── tests/
│   └── test_pipeline.py         # Unit tests
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.12+
- Java 11
- Apache Spark 3.5.1
- Apache Kafka 3.7.0
- PostgreSQL 14+

### 1 — Clone the repo
```bash
git clone https://github.com/tejfaster/marketpulse.git
cd marketpulse
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

### 6 — Start Spark Worker (Windows)
```bash
C:\spark\spark-3.5.1-bin-hadoop3\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://10.95.208.20:7077
```

### 7 — Run Stock Producer
```bash
python producers/stock_producer.py
```

---

## 📊 Data Flow

### Bronze Layer
- Raw tick data ingested from Kafka
- Stored as Parquet/Delta Lake
- No transformations — raw as received

### Silver Layer
- Cleaned and validated OHLCV data
- Technical indicators computed:
  - RSI (Relative Strength Index)
  - MACD (Moving Average Convergence Divergence)
  - Bollinger Bands
  - EMA 20/50/200

### Gold Layer
- Aggregated signals per stock
- ML-based anomaly detection (cuML)
- Buy/Sell/Hold signal generation
- Ready for dashboard consumption

---

## 📈 Stocks Tracked

| Symbol | Company |
|--------|---------|
| AAPL | Apple Inc. |
| GOOGL | Alphabet Inc. |
| MSFT | Microsoft Corporation |
| AMZN | Amazon.com Inc. |
| TSLA | Tesla Inc. |
| NVDA | NVIDIA Corporation |
| META | Meta Platforms Inc. |

---

## 🗺️ Roadmap

- [x] Cluster setup (Spark + Kafka)
- [ ] Bronze layer ingestion
- [ ] Silver layer transformations
- [ ] Gold layer signals
- [ ] Airflow orchestration
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
