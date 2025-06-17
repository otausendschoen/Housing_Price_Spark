# Housing_Price_Spark

---

## Goal

Predict housing prices in Barcelona based on Idealista data, enriched with:
- **Income data** (2017, projected to 2020 using modeling)
- **Unemployment data** (2020)
- **Neighborhood & district geo-identifiers** (via lookup tables)

This project also flags price anomalies by analyzing residuals of predicted vs. actual listing prices.

---

## Pipeline Overview

### A. Data Management Backbone

#### A.1 Explore and Select KPIs
We explored raw datasets (income, unemployment, Idealista listings) and selected key performance indicators (KPIs) such as:
- Price per m²
- Rooms / Size / Floor
- Neighborhood unemployment
- Income index (RFD)

#### A.2 Data Formatting
- Standardized all datasets into Spark DataFrames
- Normalized names (accents, case, punctuation)
- Joined with lookup tables to ensure harmonized IDs
- Exported to `/Formatted_Zone/` as `.parquet`

#### A.3 Data Exploitation
- Created final feature matrix: income + unemployment + Idealista
- Joined on district and neighborhood identifiers
- Aggregated unemployment per barrio
- Output saved in `/Exploitation_Zone/` as `.parquet`

---

##  Model Training

In the next stage (`analysis.py`), we:
- Load the exploitation data
- Train and evaluate multiple Spark ML models (e.g., Linear Regression, Random Forest)
- Rank models by validation performance (accuracy, MAE, R²)
- Automatically select the best model for deployment

---

All raw files are stored under `Landing_Zone/`:
- `Income/*.csv` (Barcelona income per barrio/district)
- `lookup_tables/*.csv` (district and neighborhood mappings)
- `idealista/*.json` (2020 housing listings)
- `unemployment.json` (monthly unemployment counts)
