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

## Model Training and Evaluation

In the `analysis.py` script, we load the exploitation datasets and train predictive models to estimate **price per square meter** (`priceByArea`) using property-level and socioeconomic features.

### Step 1: Income Forecast for 2020

Since the income data only went up to 2017, we forecasted average family income for 2018–2020 using a **3-year rolling average** per neighborhood. The results were appended to the existing dataset to ensure alignment with 2020 unemployment and housing data.

Missing values (due to neighborhoods lacking prior income records) were filled with the **average income at the district level**.

### Step 2: Feature Engineering

We enriched the dataset with:
- Property features (e.g., size, rooms, bathrooms, typology)
- Location data (district/neighborhood)
- Forecasted income and annual unemployment

Missing values were handled using logical imputations:
- Booleans (`hasLift`, `hasParkingSpace`) defaulted to `False`
- Missing `floor` values set to `"unknown"`
- `parkingPrice` set to 0 where applicable

Categorical variables were encoded using `StringIndexer` and `OneHotEncoder`. Features were assembled using `VectorAssembler`.

### Step 3: Model Training

We trained and evaluated two regression models using PySpark ML:

| Model         | RMSE (€/m²) |
|---------------|-------------|
| GBTRegressor  | **787.27**  |
| RandomForest  | 1177.45     |

Both models were tracked using **MLflow**, logging:
- Model type and hyperparameters
- RMSE on validation set
- Trained pipeline artifact

The best-performing model (GBT) was automatically registered to the `Production` stage as:

We implemented an Airflow pipeline to orchestrate the entire process. The DAG includes three sequential tasks:

1. `raw_to_formatted.py`: standardizes raw data
2. `formatted_to_exploitation.py`: merges and enriches datasets
3. `analysis.py`: trains and registers ML models

### DAG Screenshot

![Screenshot from 2025-06-23 19-14-44](https://github.com/user-attachments/assets/17a15edb-0953-4e37-a5d4-2aab9535a8bb)

### Run Instructions

1. Install Airflow and initialize DB:
   ```bash
   pip install apache-airflow
   airflow db migrate
   airflow standalone

2. Copy the DAG to your Airflow dags/ folder.
3. Trigger the DAG
   ```bash
   airflow dags trigger housing_price_pipeline
