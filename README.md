# Iran-Israel Conflict: Impact on Indian Financial Markets

A Databricks-based capstone project that analyses how Iran-Israel geopolitical events (Oct 2023 – Mar 2025) propagate through Indian equity, commodity, currency, and volatility markets. Built on Azure with the **Medallion Architecture** (Bronze → Silver → Gold) and Unity Catalog governance.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Research Questions](#research-questions)
3. [Architecture](#architecture)
4. [Data Sources](#data-sources)
5. [Project Structure](#project-structure)
6. [Medallion Architecture](#medallion-architecture)
   - [Bronze Layer](#bronze-layer)
   - [Silver Layer](#silver-layer)
   - [Gold Layer](#gold-layer)
7. [Key Performance Indicators (KPIs)](#key-performance-indicators-kpis)
8. [Dashboards](#dashboards)
9. [Workflow Orchestration](#workflow-orchestration)
10. [Technology Stack](#technology-stack)
11. [Setup & Execution](#setup--execution)
12. [Unity Catalog Structure](#unity-catalog-structure)
13. [Author](#author)

---

## Project Overview

The Iran-Israel conflict has global economic ramifications — particularly for India, which is heavily dependent on crude oil imports. This project quantifies those effects by correlating a curated timeline of geopolitical events (strikes, retaliations, sanctions) with daily movements in Indian equities (Nifty 50, Sensex, sectoral indices), Brent crude, USD/INR, India VIX, gold, and Foreign Institutional Investor (FII) flows.

**Period of Analysis:** October 2023 – March 2025  
**Total Trading Days Covered:** ~364 NSE trading days

---

## Research Questions

1. **Volatility Shock** — Do HIGH/CRITICAL conflict events trigger India VIX spikes? How long does it take for VIX to decay back to baseline?
2. **Event-Market Reaction** — What is the T+1, T+3, and T+5 day Nifty return profile after each conflict event? Can event severity predict the direction?
3. **Crude-Nifty Correlation** — Does the Brent crude–Nifty correlation strengthen during active conflict windows?
4. **FII Flow Analysis** — Do FII outflows accelerate around HIGH/CRITICAL events? Is there a systematic FII selling pattern?
5. **Rupee & Inflation Channel** — How do crude oil shocks transmit through USD/INR depreciation and CPI inflation?
6. **Sector Performance** — Which sectors (Energy, Auto, IT, Bank) outperform or underperform during conflict vs. non-conflict weeks?

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        Azure Data Lake (ADLS Gen2)                 │
│   abfss://capstonecontainer@iranisrael65.dfs.core.windows.net      │
│                                                                    │
│   └── landing_zone/                                                │
│       ├── market_data/          (yfinance parquet)                 │
│       ├── alpha_vantage/        (Brent crude CSV)                  │
│       ├── events/               (conflict events CSV)              │
│       ├── fii_data/             (FII/DII flows CSV)                │
│       └── macro/                (CPI, WTI data)                    │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────────┐
│                  Unity Catalog: iran_israel_capstone_project       │
│                                                                    │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐                   │
│   │  BRONZE   │───▶│  SILVER   │───▶│   GOLD   │                   │
│   │ Raw ingest│    │ Cleaned & │    │ Analytics│                   │
│   │ as-is     │    │ enriched  │    │ & KPIs   │                   │
│   └──────────┘      └──────────┘    └──────────┘                   │
│                                                                    │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────────────┐
│                    AI/BI Dashboards (Lakeview)                     │
│   • Market Impact Dashboard   • Event Reaction KPIs                │
│   • Volatility Shock KPIs     • FII Flow Analysis                  │
│   • Rupee Inflation Channel                                        │
└────────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Description | Ingestion Method | Landing Format |
| --- | --- | --- | --- |
| **Yahoo Finance (yfinance)** | OHLCV data for 14 tickers (Nifty 50, Sensex, Nifty Bank, Nifty Energy, Nifty Auto, Nifty IT, HAL, IndiGo, Asian Paints, ONGC, Brent Crude, USD/INR, Gold, India VIX) | Python API (`yfinance`) | Parquet |
| **Alpha Vantage** | Brent crude oil daily prices (reconciliation source) | REST API | CSV |
| **Curated Event Timeline** | 10+ Iran-Israel conflict events with severity, crude risk, expected T+1 direction | Manual curation | CSV |
| **FII/DII Data** | Foreign & Domestic Institutional Investor buy/sell flows | NSDL/SEBI published data | CSV |
| **Macro: CPI** | India Consumer Price Index (monthly) | FRED / RBI | CSV |
| **Macro: WTI** | WTI crude oil prices (supplementary) | FRED | CSV |

---

## Project Structure

```
Iran-Israel-capstone-Project/
│
├── README.md                              # This file
├── 00_capstone_project_setup              # Catalog & schema creation
│
├── bronze/                                # Raw data ingestion
│   ├── 01_ingest_yfinance_bronze          # Market data via yfinance API → ADLS → Bronze
│   ├── 01_ingest_data_bronze_old          # Legacy/alternate market ingestion
│   ├── 02_ingest_alpha_vantage_bronze     # Brent crude from Alpha Vantage
│   ├── 03_ingest_events_bronze            # Conflict events CSV → Bronze
│   └── 04_ingest_fii_bronze              # FII/DII flows CSV → Bronze
│
├── silver/                                # Cleaning & enrichment
│   ├── silver_event_dim                   # Event dimension table (deduped, typed)
│   ├── silver_daily_market                # Unified daily market table with returns
│   └── silver_layer_old                   # Legacy silver processing
│
└── gold/                                  # Analytical KPI tables
    ├── gold_volatility_shock              # VIX shock detection & event decay
    ├── gold_event_market_reaction         # T+1/T+3/T+5 event reaction windows
    ├── gold_crude_nifty_daily_correlation # Brent–Nifty return correlation
    ├── gold_fii_flow_analysis             # FII outflow patterns around events
    ├── gold_rupee_inflation_channel       # Crude → USD/INR → CPI transmission
    └── gold_sector_performance            # Weekly sector alpha (conflict vs. normal)
```

---

## Medallion Architecture

### Bronze Layer

Raw data ingested **as-is** from external sources into Delta tables with added `ingestion_timestamp` and `source_file` audit columns.

| Table | Description | Key Columns |
| --- | --- | --- |
| `bronze.market_data` | Daily OHLCV for 14 tickers | `trade_date`, `open`, `high`, `low`, `close`, `volume`, `ticker`, `asset_name` |
| `bronze.events` | Curated conflict event timeline | `event_date`, `event_id`, `event_type`, `severity`, `crude_risk`, `description`, `t_plus_1_expected` |
| `bronze.fii_raw` | FII/DII institutional flows | `date`, `fii_net_buy_sell_cr`, `fii_gross_buy_cr`, `fii_gross_sell_cr` |
| `bronze.macro_cpi_raw` | India CPI monthly | `record_date`, `value` |
| `bronze.macro_brent_alpha_raw` | Brent crude (Alpha Vantage) | `record_date`, `value` |
| `bronze.macro_wti_raw` | WTI crude oil | `record_date`, `value` |

### Silver Layer

Cleaned, type-cast, deduplicated, and enriched data. The silver layer creates two core tables:

**`silver.event_dim`** — Dimension table of conflict events:
* Typed columns (`event_date` as DATE, `severity` as STRING)
* Deduplication and validation
* Fields: `event_id`, `event_type`, `severity`, `crude_risk`, `t_plus_1_expected`

**`silver.daily_market_clean`** — Unified daily market fact table:
* Pivoted from 14 tickers into a single wide row per trading day
* Computed **daily returns** (%) for Nifty, Sensex, Nifty Bank, sectoral indices, Brent, USD/INR, Gold, individual stocks
* Computed **rolling returns**: 5-day and 20-day for Nifty
* Computed **India VIX 20-day moving average**
* **Left-joined with events**: each trading day tagged with `event_id`, `event_type`, `severity`, `crude_risk`
* Derived `days_since_last_event` for temporal proximity analysis
* Built from NSE trading calendar (Mon–Fri, excluding public holidays)

### Gold Layer

Business-level analytics tables, each targeting a specific research question:

| Gold Table | KPI Focus | Key Metrics |
| --- | --- | --- |
| `gold.gold_vix_daily` | VIX shock detection | `vix_to_ma_ratio`, `is_vix_shock` (VIX ≥ 1.5× 20d MA), shock days per month |
| `gold.gold_vix_event_decay` | Post-event VIX decay & Nifty recovery | `vix_spike_pct`, `vix_decay_days`, `max_drawdown_pct`, `nifty_recovery_days`, `nifty_recovered_within_30td` |
| `gold.gold_event_market_reaction` | Event reaction windows | `nifty_t1_return`, `nifty_t3_return`, `nifty_t5_return`, `brent_t1_return`, `usdinr_t1_change`, `direction_prediction_correct` |
| `gold.event_reaction_windows` | Simplified event reactions | Nifty close at T, T+1, T+3, T+5 with computed returns |
| `gold.severity_correlation` | Severity vs. market reaction | `severity_bucket`, Nifty returns by severity level |
| `gold.gold_crude_nifty_daily_correlation` | Crude–equity correlation | `brent_daily_change_pct`, `nifty_daily_return_pct`, `in_conflict_window`, `sign_reversal` |
| `gold.gold_fii_flow_analysis` | Institutional flow patterns | `fii_net_cr`, `is_hc_event_day`, `fii_is_selling`, `within_5td_of_hc`, `fii_outflow_rank` |
| `gold.gold_rupee_inflation_channel` | Macro transmission channel | `brent_mom_change`, `cad_impact_pct_gdp`, `usdinr_monthly_change_pct`, `is_conflict_month`, `cpi_mom_change_curr` |
| `gold.gold_sector_performance` | Sector-level alpha | `sector_weekly_return`, `sector_alpha`, `is_conflict_week` (Energy, Auto, IT, Bank sectors) |

---

## Key Performance Indicators (KPIs)

### KPI 1 — VIX Shock Detection
* **Metric:** Days where India VIX ≥ 1.5× its 20-day moving average
* **Granularity:** Monthly aggregation of shock days and peak VIX ratio

### KPI 2 — Post-Event VIX Decay
* **Metric:** Number of trading days for VIX to return within 10% of pre-event level
* **Window:** Up to 60 trading days post-event
* **Scope:** HIGH and CRITICAL severity events only

### KPI 3 — Nifty Shock-Recovery
* **Metric:** Maximum drawdown (%) and recovery time (trading days) after each event
* **Window:** 30 trading days for recovery detection
* **Output:** Recovery rate (% of events where Nifty recovered within 30 TDs)

### KPI 4 — Event Direction Prediction
* **Metric:** T+1 actual direction vs. curated expected direction
* **Output:** `direction_prediction_correct` boolean, accuracy rate

### KPI 5 — FII Selling on Event Days
* **Metric:** Proportion of HIGH/CRITICAL event days with net FII selling
* **Analysis:** FII outflow rank overlap with conflict proximity (within 5 trading days)

### KPI 6 — Crude-Nifty Correlation
* **Metric:** Pearson correlation of daily Brent vs. Nifty returns, sign-reversal rate
* **Segmentation:** Conflict window vs. non-conflict window

### KPI 7 — Rupee & Inflation Transmission
* **Metric:** Brent month-over-month change → USD/INR depreciation → CPI impact
* **Analysis:** Current account deficit impact as % of GDP

### KPI 8 — Sector Alpha
* **Metric:** Weekly sector return minus Nifty weekly return
* **Sectors:** Energy, Auto, IT, Bank (plus individual stocks: HAL, IndiGo, Asian Paints, ONGC)

---

## Dashboards

Five interactive Lakeview dashboards provide visual KPI summaries:

| Dashboard | Description |
| --- | --- |
| **Iran-Israel Conflict: Market Impact Dashboard** | Comprehensive overview of all market impacts — Nifty, VIX, Brent, USD/INR |
| **Iran-Israel Event Market Reaction KPIs** | T+1/T+3/T+5 return profiles, direction prediction accuracy |
| **Volatility Shock — KPI Dashboard** | VIX spike timeseries, decay periods, shock-recovery patterns |
| **FII Flow Analysis — KPI Dashboard** | FII selling patterns, outflow ranks, institutional behaviour during events |
| **Rupee Inflation Channel — KPI Dashboard** | Brent → Rupee → CPI transmission, CAD impact visualization |

---

## Workflow Orchestration

The entire pipeline is orchestrated via a **Databricks Workflow** named `capstone_workflow`, which sequences:

1. **Bronze ingestion** — All 4 bronze notebooks (yfinance, Alpha Vantage, events, FII)
2. **Silver transformation** — Event dimension + daily market clean
3. **Gold analytics** — All 6 gold notebooks in parallel/sequence

---

## Technology Stack

| Component | Technology |
| --- | --- |
| **Cloud Provider** | Microsoft Azure |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |
| **Compute** | Databricks (Serverless / Interactive Clusters) |
| **Data Governance** | Unity Catalog |
| **Table Format** | Delta Lake |
| **Processing** | Apache Spark (PySpark) |
| **Data APIs** | yfinance, Alpha Vantage |
| **Dashboards** | Databricks AI/BI Dashboards (Lakeview) |
| **Orchestration** | Databricks Workflows |
| **Language** | Python, SQL |

---

## Setup & Execution

### Prerequisites
* Databricks workspace on Azure
* Access to ADLS Gen2 storage account (`iranisrael65`)
* Unity Catalog enabled
* Python libraries: `yfinance`

### Steps

1. **Run the setup notebook:**
   ```
   00_capstone_project_setup
   ```
   Creates the `iran_israel_capstone_project` catalog and `bronze`, `silver`, `gold` schemas.

2. **Execute Bronze ingestion notebooks (in order):**
   ```
   bronze/01_ingest_yfinance_bronze      → bronze.market_data
   bronze/02_ingest_alpha_vantage_bronze  → bronze.macro_brent_alpha_raw
   bronze/03_ingest_events_bronze         → bronze.events
   bronze/04_ingest_fii_bronze            → bronze.fii_raw
   ```

3. **Execute Silver transformation notebooks:**
   ```
   silver/silver_event_dim     → silver.event_dim
   silver/silver_daily_market  → silver.daily_market_clean
   ```

4. **Execute Gold analytics notebooks:**
   ```
   gold/gold_volatility_shock              → gold.gold_vix_daily, gold.gold_vix_event_decay
   gold/gold_event_market_reaction         → gold.gold_event_market_reaction, gold.event_reaction_windows
   gold/gold_crude_nifty_daily_correlation → gold.gold_crude_nifty_daily_correlation
   gold/gold_fii_flow_analysis             → gold.gold_fii_flow_analysis
   gold/gold_rupee_inflation_channel       → gold.gold_rupee_inflation_channel
   gold/gold_sector_performance            → gold.gold_sector_performance
   ```

5. **Or run the entire pipeline via the `capstone_workflow` job.**

---

## Unity Catalog Structure

```
iran_israel_capstone_project (Catalog)
│
├── bronze (Schema)
│   ├── market_data
│   ├── events
│   ├── fii_raw
│   ├── macro_cpi_raw
│   ├── macro_brent_alpha_raw
│   └── macro_wti_raw
│
├── silver (Schema)
│   ├── event_dim
│   └── daily_market_clean
│
└── gold (Schema)
    ├── gold_vix_daily
    ├── gold_vix_event_decay
    ├── gold_event_market_reaction
    ├── event_reaction_windows
    ├── severity_correlation
    ├── gold_crude_nifty_daily_correlation
    ├── gold_fii_flow_analysis
    ├── gold_rupee_inflation_channel
    └── gold_sector_performance
```

---


