# Modern Data Stack Trading Platform & Execution Engine

An end-to-end, production-grade data platform and strategy execution engine built with the Modern Data Stack (MDS) on Google Cloud Platform.

The project automates the extraction of OHLCV market data, standardizes and transforms it via dbt and BigQuery, and serves clean endpoints to a dual-mode Python execution engine.

---

## Architecture Overview

1a. Public Market API
* Fetch raw OHLCV price candles from public REST endpoints.

1b. Ingestion Engine (Python)
* Load data directly into Google BigQuery Raw Layer using partitioned append strategy.

1c. Analytics Core (dbt + BigQuery)
* Staging Layer: Deduplication and schema standardization.
* Marts Layer: Calculation of technical indicators (SMA, Bollinger Bands).

1d. Execution Engine (Python)
* Backtest Mode: Read historical endpoints and log results to fact_trades_backtest.
* Live Mode: Read real-time endpoints and log results to fact_trades_live.

1e. BI & Monitoring
* Dashboard integration for performance analytics via Streamlit or Looker Studio.

---

## Key Technical Features

* Incremental Ingestion Strategy: Appends partitioned slices directly into BigQuery raw layers, preventing lock conflicts with downstream readers.
* Idempotent Data Transformation: dbt models use window functions and surrogate keys to remove duplicates and compute technical indicators entirely within BigQuery.
* Data Quality Gatekeeping: Integrated data tests (dbt test) fail early in CI/CD pipelines if custom logical assertions fail.
* Dual-Mode Execution Abstraction: Polymorphic DataProvider implementations allow strategies to operate identically in historical backtesting and real-time trading without changing code logic.
* Containerized Pipeline Execution: Dockerized microservices orchestrate extraction, transformation, and execution isolated in Cloud Run jobs.

---

## Tech Stack & Tooling

* Data Warehouse: Google BigQuery (Partitioned & Clustered Tables)
* Data Transformation: dbt core (dbt-bigquery)
* Language & Automation: Python 3.11+, Pandas, PyArrow
* Orchestration & Infrastructure: Docker, GCP Cloud Run, Cloud Scheduler
* Testing & Quality: dbt-tests, Custom SQL Assertions

---

## Highlighted Code Snippets & Architecture Solutions

### 2a. Zero-Downtime Incremental Transformation (dbt)

```
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'open_time'],
    partition_by={"field": "open_time", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["symbol"]
) }}

WITH base_stg AS (
    SELECT * FROM {{ ref('stg_ohlcv') }}
    {% if is_incremental() %}
      WHERE open_time >= TIMESTAMP_SUB((SELECT MAX(open_time) FROM {{ this }}), INTERVAL 3 DAY)
    {% endif %}
)
SELECT
    symbol,
    open_time,
    price_close,
    AVG(price_close) OVER (
        PARTITION BY symbol ORDER BY open_time 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20,
    ROUND(STDDEV(price_close) OVER (
        PARTITION BY symbol ORDER BY open_time 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 4) AS stddev_20
FROM base_stg
{% if is_incremental() %}
  WHERE open_time > (SELECT MAX(open_time) FROM {{ this }})
{% endif %}
```

### 2b. Custom Business Logic Quality Gate

tests/assert_high_greater_than_low.sql
```
SELECT
    symbol, open_time, price_high, price_low
FROM {{ ref('mart_ohlcv_indicators') }}
WHERE price_high < price_low
   OR price_high < price_open
   OR price_high < price_close
```
### 2c. Polymorphic Strategy Engine (Backtest vs. Live Abstraction)

src/execution/base_strategy.py
```
from abc import ABC, abstractmethod
import pandas as pd

class DataProvider(ABC):
    @abstractmethod
    def fetch_data(self, symbol: str) -> pd.DataFrame:
        pass

class BaseStrategy(ABC):
    def __init__(self, data_provider: DataProvider, logger_target_table: str):
        self.data_provider = data_provider
        self.logger_target_table = logger_target_table

    def execute_tick(self, symbol: str):
        df = self.data_provider.fetch_data(symbol)
        latest = df.iloc[-1]
        
        if latest['price_close'] > latest['sma_20']:
            self.on_buy_signal(symbol, latest)
        elif latest['price_close'] < latest['sma_20']:
            self.on_sell_signal(symbol, latest)

    @abstractmethod
    def on_buy_signal(self, symbol: str, tick_data: pd.Series):
        pass
```
---

## Local Development Setup

3a. Clone and configure environment:
* git clone https://github.com/kgudalewski/trading_project_demo.git
* cd trading-data-platform-demo
* cp .env.example .env

3b. Build and run containers:
* docker-compose up --build

3c. Run data quality tests:
* docker-compose run dbt dbt test
