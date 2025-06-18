# This file laods the data from the exploitation and we analyze it and fit the model

#%%
import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession # SPARK VERSION 3.3.2
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

# In this section we will predict the 2020 income based on the previous years
# We will use a rolling average of the previous 3 years to predict the next year because pyspark does not have a built-in time series forecasting model



# Load your exploitation zone data
exploitation_zone_path = "../Exploitation_Zone"

income_df = spark.read.parquet(os.path.join(exploitation_zone_path, "income_extended.parquet"))
idealista_unemployment_df = spark.read.parquet(os.path.join(exploitation_zone_path, "idealista_unemployment.parquet"))

# Keep only relevant columns
income_base = income_df.select("neighborhood_id", "year", "average_family_income")

# Step 1: Rolling average to predict 2018 from 2015–2017
window = Window.partitionBy("neighborhood_id").orderBy("year").rowsBetween(-2, 0)
income_rolling = income_base.withColumn("rolling_3yr_avg", F.avg("average_family_income").over(window))

forecast_2018 = income_rolling.filter(F.col("year") == 2017) \
    .select("neighborhood_id", "rolling_3yr_avg") \
    .withColumnRenamed("rolling_3yr_avg", "average_family_income") \
    .withColumn("year", F.lit(2018))

# Step 2: Append forecast_2018 to the base data
income_extended = income_base.unionByName(forecast_2018)

# Step 3: Rolling average to predict 2019 from 2016–2018
income_rolling = income_extended.withColumn("rolling_3yr_avg", F.avg("average_family_income").over(window))

forecast_2019 = income_rolling.filter(F.col("year") == 2018) \
    .select("neighborhood_id", "rolling_3yr_avg") \
    .withColumnRenamed("rolling_3yr_avg", "average_family_income") \
    .withColumn("year", F.lit(2019))

income_extended = income_extended.unionByName(forecast_2019)

# Step 4: Rolling average to predict 2020 from 2017–2019
income_rolling = income_extended.withColumn("rolling_3yr_avg", F.avg("average_family_income").over(window))

forecast_2020 = income_rolling.filter(F.col("year") == 2019) \
    .select("neighborhood_id", "rolling_3yr_avg") \
    .withColumnRenamed("rolling_3yr_avg", "average_family_income") \
    .withColumn("year", F.lit(2020))

income_extended = income_extended.unionByName(forecast_2020)

# Final result: income_extended now contains original data + forecasts for 2018–2020
income_extended.orderBy("neighborhood_id").show()

# %%
