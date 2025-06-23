# This file laods the data from the exploitation and we analyze it and fit the model

#%%
import pandas as pd
import numpy as np
import os
# For visualization seaborn
import seaborn as sns

from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import log1p
from pyspark.sql.functions import expm1
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import mlflow
import mlflow.spark
from pyspark.ml.regression import RandomForestRegressor
from mlflow.tracking import MlflowClient



#%%
# This code cell sets JAVA_HOME to Java 11 and restarts the Spark session
# MAKE SURE JAVA 11 IS INSTALLED ON YOUR SYSTEM
# INSTALL WITH `sudo apt install openjdk-11-jdk` on Ubuntu or similar commands for other OS
print("Setting up Java 11 for Spark...")
# Set JAVA_HOME (update the path after installation if needed)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
print("JAVA_HOME set to:", os.environ["JAVA_HOME"])

print("Starting Spark session...")
# Now try creating a Spark session again

try:
    spark = SparkSession.builder.appName("Landing_to_Formatted").getOrCreate()
    print("Spark session created successfully!")
except Exception as e:
    print("Failed to start Spark session")
    print(e)



#%%
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

exploitation_zone_path = os.path.join(SCRIPT_DIR, "../Exploitation_Zone")

# Ensure exploitation zone path exists
os.makedirs(exploitation_zone_path, exist_ok=True)

# Load exploitation data
income_df = spark.read.parquet(os.path.join(exploitation_zone_path, "income_extended.parquet"))
idealista_unemployment_df = spark.read.parquet(os.path.join(exploitation_zone_path, "idealista_unemployment.parquet"))

# Show schema and a few rows of income_extended.parquet
print("income_extended.parquet")
income_df.printSchema()
income_base = income_df.select("neighborhood_id", "year", "average_family_income")
income_base.show(10, truncate=False)

# Show schema and a few rows of idealista_unemployment.parquet
print("\n idealista_unemployment.parquet")
idealista_unemployment_df.printSchema()
idealista_unemployment_df.show(10, truncate=False)

# Select relevant columns
income_base = income_df.select("neighborhood_id", "year", "average_family_income")

# Predict income for 2018–2020 using rolling average
window = Window.partitionBy("neighborhood_id").orderBy("year").rowsBetween(-2, 0)

income_extended = income_base
for forecast_year in [2018, 2019, 2020]:
    income_rolling = income_extended.withColumn("rolling_3yr_avg", F.avg("average_family_income").over(window))
    forecast = income_rolling.filter(F.col("year") == forecast_year - 1) \
        .select("neighborhood_id", "rolling_3yr_avg") \
        .withColumnRenamed("rolling_3yr_avg", "average_family_income") \
        .withColumn("year", F.lit(forecast_year))
    income_extended = income_extended.unionByName(forecast)

# Display result
income_extended.orderBy("neighborhood_id", "year").show()

# Filter only forecasted rows (years >= 2018)
forecasted_only = income_extended.filter(F.col("year") >= 2018)

# Show the forecasted results sorted by neighborhood and year
forecasted_only.orderBy("neighborhood_id", "year").show()
#%%

#checking if the forcast values are coherent

# Convert to Pandas for plotting
plot_df = income_extended.orderBy("neighborhood_id", "year").toPandas()
plot_df["year"] = plot_df["year"].astype(int)

# Get list of neighborhoods
neighborhoods = plot_df["neighborhood_id"].unique()

# Create a plot per neighborhood
for nb in neighborhoods:
    subset = plot_df[plot_df["neighborhood_id"] == nb]
    plt.figure(figsize=(8, 4))
    sns.lineplot(data=subset, x="year", y="average_family_income", marker="o")
    plt.title(f"Income Evolution - Neighborhood {nb}")
    plt.ylabel("Average Family Income")
    plt.xlabel("Year")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# 1. Filter forecasted income for 2020
income_2020 = income_extended.filter(F.col("year") == 2020)

# 2. Add district/neighborhood names from income_df
# We'll select distinct mappings to avoid duplication
name_lookup = income_df.select("neighborhood_id", "district_n", "neighborhood_n").distinct()

# Join with 2020 forecast
income_df_with_2020 = name_lookup.join(
    income_2020.select("neighborhood_id", "average_family_income").withColumnRenamed("average_family_income", "income_2020_forecast"),
    on="neighborhood_id",
    how="left"
)

# 3. Keep only relevant columns
income_df_with_2020 = income_df_with_2020.select("district_n", "neighborhood_n", "income_2020_forecast")

# 4. Join with Idealista unemployment data
joined_df = idealista_unemployment_df.join(
    income_df_with_2020,
    on=["district_n", "neighborhood_n"],
    how="left"
)

# 5. Show the result
joined_df.show()
#%%

# Check for missing income forecasts
joined_df.filter(F.col("income_2020_forecast").isNull()).select("district_n", "neighborhood_n").show()

# Check available years for affected neighborhoods
missing_ids = joined_df.filter(F.col("income_2020_forecast").isNull()) \
    .select("district_n", "neighborhood_n").distinct()

income_df.join(missing_ids, on=["district_n", "neighborhood_n"], how="inner") \
    .select("neighborhood_n", "year", "average_family_income") \
    .orderBy("neighborhood_n", "year").show(100, truncate=False)

"""No rows exist in income_df for those neighborhoods with missing forecast. That's why the rolling average could not be computed, and consequently, the forecasted income for 2020 is null.

Thus we'll fill the missing value with overall district average.
"""

# Step 1: Compute average income forecast for each district
district_avg_income = income_df_with_2020.groupBy("district_n").agg(
    F.avg("income_2020_forecast").alias("district_avg_income")
)

# Step 2: Join back to get the district average for each row
joined_df_filled = joined_df.join(district_avg_income, on="district_n", how="left")

# Step 3: Create a new column with filled values
joined_df_filled = joined_df_filled.withColumn(
    "income_2020_forecast_filled",
    F.coalesce("income_2020_forecast", "district_avg_income")
)

# Optional: Drop the helper column if you don't need it
joined_df_filled = joined_df_filled.drop("district_avg_income")

# Show result
joined_df_filled.select("district_n", "neighborhood_n", "income_2020_forecast", "income_2020_forecast_filled").show()

joined_df_filled.filter(F.col("income_2020_forecast_filled").isNull()).select("district_n", "neighborhood_n").show()

joined_df_filled.printSchema()
joined_df_filled.show()
#%%
"""# Predictive Analysis

We chose the Gradient Boosted Trees Regressor (GBTRegressor) for predicting housing prices because it effectively captures complex, nonlinear relationships between property characteristics and socioeconomic context. Unlike linear models, which assume a straight-line relationship, GBT can model interactions between features such as income, unemployment, property size, and location. This is especially valuable in the real estate domain, where price dynamics are often influenced by subtle and compound effects. Its strong predictive performance and flexibility make it a suitable choice for identifying pricing patterns and anomalies in a diverse urban housing market.


For tree-based models like GBTRegressor, normalization (like min-max scaling or standardization) is not required and often not beneficial.

Tree models:

- Split on thresholds (< / >)

- Are scale-invariant to numeric features

goal: to predict price by area = price by square meter (and not price as it
"""
# Manually extract subfields from parkingSpace and detailedType
model_df = joined_df_filled.select(
    col("priceByArea").alias("price_per_sqm"),
    col("neighborhood_n_reconciled").alias("location"),
    "bathrooms",
    col("detailedType.typology").alias("typology"),
    col("detailedType.subTypology").alias("sub_typology"),
    "distance",
    "exterior", "floor", "has360", "has3DTour", "hasLift",
    "hasPlan", "hasStaging", "hasVideo", "newDevelopment",
    "numPhotos", "operation",
    col("parkingSpace.hasParkingSpace").alias("hasParkingSpace"),
    col("parkingSpace.isParkingSpaceIncludedInPrice").alias("parkingIncluded"),
    col("parkingSpace.parkingSpacePrice").alias("parkingPrice"),
    "propertyType", "rooms", "size", "status",
    "topNewDevelopment", "newDevelopmentFinished",
    "annual_unemployment", "income_2020_forecast", "income_2020_forecast_filled", "longitude", "latitude"
)

# Preview result
model_df.show(5, truncate=False)
model_df.printSchema()

# Number of rows
row_count = model_df.count()

# Number of columns
col_count = len(model_df.columns)

print(f"Shape: ({row_count}, {col_count})")


# Count nulls per column
null_counts = model_df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in model_df.columns])
null_counts.show(truncate=False)

"""## Inputing missing values"""

# Drop due to NAs
model_df = model_df.drop("sub_typology", "income_2020_forecast", "newDevelopmentFinished")

# Confirm new shape and columns
print(f"New shape: ({model_df.count()}, {len(model_df.columns)})")
model_df.printSchema()

# Keep hasParkingSpace and parkingIncluded and we impute missing values as False

# Step 1: Ensure hasParkingSpace and parkingIncluded are already imputed
model_df = model_df.withColumn("hasParkingSpace", when(col("hasParkingSpace").isNull(), False).otherwise(col("hasParkingSpace")))
model_df = model_df.withColumn("parkingIncluded", when(col("parkingIncluded").isNull(), False).otherwise(col("parkingIncluded")))

# Step 2: Impute parkingPrice conditionally
model_df = model_df.withColumn(
    "parkingPrice",
    when(
        col("parkingPrice").isNull() & (col("hasParkingSpace") == True) & (col("parkingIncluded") == True),
        0.0
    ).when(
        col("parkingPrice").isNull(),
        0.0
    ).otherwise(col("parkingPrice"))
)

#floor is a string. We impute missing values with "unknown":
model_df = model_df.withColumn(
    "floor",
    when(col("floor").isNull(), "unknown").otherwise(col("floor"))
)

# Impute Missing hasLift with False. Listings typically mention if there's a lift (elevator) when there is one. Missing often means no lift or not specified, which models can treat as absence.

model_df = model_df.withColumn(
    "hasLift",
    when(col("hasLift").isNull(), False).otherwise(col("hasLift"))
)

# Count nulls per column
null_counts = model_df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in model_df.columns])
null_counts.show(truncate=False)

model_df.printSchema()

"""## Encode Categorical Features in PySpark"""

categorical_cols = [
    "location",         # neighborhood name
    "typology",         # e.g. flat, duplex
    "floor",            # string format like "3", "unknown"
    "operation",        # e.g. sale, rent
    "propertyType",     # e.g. flat, house
    "status"            # e.g. good, new
]
StringIndexer
# Step 1: StringIndexers for all categorical columns
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
    for col in categorical_cols
]

# Step 2: OneHotEncoders for the indexed columns
encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
    for col in categorical_cols
]
#%%

"""## Assemble Feature Vector - Train-Test Split - Train Gradient Boosted Trees Regressor"""

# 1. Feature Definitions

categorical_cols = ["location", "typology", "floor", "operation", "propertyType", "status"]
numeric_cols = ["rooms", "size", "bathrooms", "numPhotos", "parkingPrice", "annual_unemployment", "income_2020_forecast_filled"]
boolean_cols = [
    "exterior", "has360", "has3DTour", "hasLift", "hasPlan",
    "hasStaging", "hasVideo", "newDevelopment", "hasParkingSpace",
    "parkingIncluded", "topNewDevelopment"
]

## 2. Indexers and Encoders

# StringIndexer for categorical features
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]

# OneHotEncoder for indexed columns
encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec") for col in categorical_cols]

# Collect all final feature columns
cat_vec_cols = [f"{col}_vec" for col in categorical_cols]
all_features = cat_vec_cols + numeric_cols + boolean_cols

# 3. Assemble All Features

assembler = VectorAssembler(inputCols=all_features, outputCol="features")

''' First model: Gradient Boosted Trees Regressor (GBTRegressor) '''

## 4. GBT Regressor Model

gbt = GBTRegressor(featuresCol="features", labelCol="price_per_sqm", maxIter=100)

# 5. Full Pipeline

pipeline = Pipeline(stages=indexers + encoders + [assembler, gbt])

# 6. Train-Test Split

train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)

# 7. Fit Model

model = pipeline.fit(train_df)

"""## Evaluate Model"""

# 8. Predict and Evaluate

predictions = model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="price_per_sqm", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

#%%

"""-------------------------------------------------------

### Second regression model: RandomForestRegressor
"""


rf = RandomForestRegressor(featuresCol="features", labelCol="price_per_sqm", numTrees=50)
pipeline_rf = Pipeline(stages=indexers + encoders + [assembler, rf])
model_rf = pipeline_rf.fit(train_df)
predictions_rf = model_rf.transform(test_df)
rmse_rf = evaluator.evaluate(predictions_rf)
print(f" RF RMSE: {rmse_rf:.2f}")


# Set or create experiment
mlflow.set_experiment("house_price_prediction")

# GBT Experiment Run
with mlflow.start_run(run_name="GBT") as gbt_run:
    mlflow.log_param("model_type", "GBTRegressor")
    mlflow.log_param("maxIter", 100)
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(model, "model")
    gbt_run_id = gbt_run.info.run_id

# RF Experiment Run
with mlflow.start_run(run_name="RandomForest") as rf_run:
    mlflow.log_param("model_type", "RandomForestRegressor")
    mlflow.log_param("numTrees", 50)
    mlflow.log_metric("rmse", rmse_rf)
    mlflow.spark.log_model(model_rf, "model")
    rf_run_id = rf_run.info.run_id



client = MlflowClient()
best_model_run_id = gbt_run_id if rmse < rmse_rf else rf_run_id
best_model_name = "best_house_price_model"

# Register the best model
model_uri = f"runs:/{best_model_run_id}/model"
client.create_registered_model(best_model_name)
model_version = client.create_model_version(best_model_name, model_uri, best_model_run_id)

# Transition best model to 'Production'
client.transition_model_version_stage(
    name=best_model_name,
    version=model_version.version,
    stage="Production"
)

print(f"Best model ({'GBT' if rmse < rmse_rf else 'RF'}) deployed to PRODUCTION")
# %%
