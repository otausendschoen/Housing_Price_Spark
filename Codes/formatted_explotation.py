#THIS FILE INCLUDES THE Formatted to explotation pipeline,WE USE IT TO LOAD DATA FROM THE FORMATTED ZONE TO PUT IT INTO THE EXPLOTATION ZONE

#%%
print("importing necessary libraries")
import pandas as pd
import os
import numpy as np
from pyspark.sql import SparkSession # SPARK VERSION 3.3.2
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import first
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

# Let's read the parquett files:

exploitation_zone_path = "../Exploitation_Zone"

# Save the extended dataframes to the exploitation zone
if not os.path.exists(exploitation_zone_path):
    os.makedirs(exploitation_zone_path)

formatted_zone_path = "../Formatted_Zone"

# 1. Load parquet files using Spark
income_df = spark.read.parquet(os.path.join(formatted_zone_path, "income.parquet"))
income_lookup_df = spark.read.parquet(os.path.join(formatted_zone_path, "lookup_income.parquet"))
unemployment_df = spark.read.parquet(os.path.join(formatted_zone_path, "unemployment.parquet"))
idealista_df = spark.read.parquet(os.path.join(formatted_zone_path, "idealista.parquet"))
idealista_lookup_df = spark.read.parquet(os.path.join(formatted_zone_path, "lookup_idealista.parquet"))


#%%
# 1) Income: bring in any lookup fields by ID (if you need them at all).
#    (If income_df already has district_id & neighborhood_id, you can skip this.)
income_df_extended = income_df.join(
    income_lookup_df.select("district_n", "neighborhood_n", "district_n_reconciled", 
                            "neighborhood_n_reconciled", "district_id", "neighborhood_id"),
    on=["district_n", "neighborhood_n"],
    how="left"
)




# 2) Idealista: same story
idealista_df_extended = idealista_df.join(
    idealista_lookup_df.select("district_n", "neighborhood_n", "district_n_reconciled", 
                               "neighborhood_n_reconciled", "district_id", "neighborhood_id"),
    on=["district_n", "neighborhood_n"],
    how="left"
)


# 3) Unemployment
unemployment_df_extended = unemployment_df.join(
    income_lookup_df.select("district_n", "neighborhood_n", "district_n_reconciled", 
                            "neighborhood_n_reconciled", "district_id", "neighborhood_id"),
    on=["district_n", "neighborhood_n"],
    how="left"
)
unemployment_agg = unemployment_df_extended.groupBy(
    "district_id", "neighborhood_id", "district_n", "neighborhood_n", 
    "district_n_reconciled", "neighborhood_n_reconciled"
).agg(_sum("count").alias("annual_unemployment"))


# we decided not to do this yet because we need to project income before joining it with unemployment
'''
# 4) unemployment + income
unemp_income_df = unemployment_agg.join(
    income_df_extended.select("district_id", "neighborhood_id", 
                              "average_family_income", "population"),
    on=["district_id", "neighborhood_id"],
    how="left"
)


# 5) Now we add it to the Idealista data:
idealista_unemp_income_df = idealista_df_extended.join(
    unemp_income_df.select("district_id", "neighborhood_id", 
                           "annual_unemployment", "average_family_income", "population"),
    on=["district_id", "neighborhood_id"],
    how="left"
)
'''
# 4) unemployment + idealista
idealista_unemp_df = idealista_df_extended.join(
    unemployment_agg.select("district_id", "neighborhood_id", 
                            "annual_unemployment", ),
    on=["district_id", "neighborhood_id"],
    how="left"
)

#%%

# exporting it to the exploitation zone
idealista_unemp_df.write.mode("overwrite").parquet(os.path.join(exploitation_zone_path, "idealista_unemployment.parquet"))
income_df_extended.write.mode("overwrite").parquet(os.path.join(exploitation_zone_path, "income_extended.parquet"))
print("Data exported to the exploitation zone successfully!")
#%%
# Stop the Spark session
spark.stop()
print("Spark session stopped.")
#%%