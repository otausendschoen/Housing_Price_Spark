# This file laods the data from the exploitation and we analyze it and fit the model

#%%
import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession # SPARK VERSION 3.3.2

#%%

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

# load the data

exploitation_zone_path = "../Exploitation_Zone"


income_df = spark.read.parquet(os.path.join(exploitation_zone_path, "income_extended.parquet"))
idealista_unemployment_df = spark.read.parquet(os.path.join(exploitation_zone_path, "idealista_unemployment.parquet"))
#%%


