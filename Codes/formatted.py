#THIS FILE INCLUDES THE LANDING PAGE CODE,WE USE IT TO LOAD DATA FROM THE LANDING ZONE AND FORMAT IT INTO THE FORMATTED ZONE


#%%
# This chunk includes all the imports and libraries we need to use
print("Importing necessary libraries...")
import pandas as pd
import os
from pyspark.sql import SparkSession


#%%
# USE PYSPARK INSTEAD:::
income_path = "../Landing_Zone/Income/2017_Distribució_territorial_renda_familiar.csv"
lookup_income_path = "../Landing_Zone/lookup_tables/income_opendatabcn_extended.csv"
lookup_idealista_path = "../Landing_Zone/lookup_tables/idealista_extended.csv"

def quick_summary(df, name="df"):
    print(f"\n Summary of {name}")
    print("Columns:", df.columns.tolist())
    print("Data types:\n", df.dtypes)
    print("Nulls:\n", df.isnull().sum())
    for col in df.select_dtypes(include='object'):
        print(f"🔸 {col}: {df[col].nunique()} unique values")
        print(df[col].unique()[:10])  # print sample


income_df = pd.read_csv(income_path)

quick_summary(income_df, "Income Data")

'''
We can observe the following:
1. The `Nom_Districte` column contains the names of districts in Barcelona.
2. The `Nom_Barri` column contains the names of neighborhoods.
3. The `Codi_Barri` column contains the codes for neighborhoods.
4. The `Renda_Familiar_Media` column contains the average family income for each neighborhood. (Numeric)
'''


# After confirming that these makes sense, we proceed do the same with the remaining data.
print("Loading lookup income data from:", lookup_income_path)
lookup_income = pd.read_csv(lookup_income_path)
quick_summary(lookup_income, "Lookup Income Data")

'''

We can observe the following:
1. The `district` column contains the names of districts in Barcelona.
2. The `district_id` column contains the codes/IDs for districts.
3. The `neighborhood` column contains the names of neighborhoods.
4. The `neighborhood_id` column contains the codes/IDs for neighborhoods. They differ from the `Codi_Barri` column in the income data.
5. the `district_n_reconciled` column contains the names of districts after reconciliation.
6. the `district_n` column contains the names of districts in Barcelona in lower case
7. the `neighborhood_n` column contains the names of neighborhoods in Barcelona in lower case
8. the `neighborhood_n_reconciled` column contains the names of neighborhoods after reconciliation.

'''



print("Loading lookup idealista data from:", lookup_idealista_path)
lookup_idealista = pd.read_csv(lookup_idealista_path)
lookup_idealista.head()
quick_summary(lookup_idealista, "Lookup Idealista Data")

'''
We can observe the following:

It's the same as the lookup income data?? (but for idealista data.?)
'''




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


# Load the tables into Spark DataFrames
income_df = spark.read.option("header", True).option("delimiter", ",").csv(income_path)
lookup_income = spark.read.option("header", True).csv(lookup_income_path)
lookup_idealista = spark.read.option("header", True).csv(lookup_idealista_path)

#view the dataframes
income_df.show(5)
lookup_income.show(5)
lookup_idealista.show(5)

#%%