#THIS FILE INCLUDES THE LANDING PAGE CODE,WE USE IT TO LOAD DATA FROM THE LANDING ZONE AND FORMAT IT INTO THE FORMATTED ZONE


#%%
# This chunk includes all the imports and libraries we need to use
# IMPORTANT: USE PYTHON 3.10.9
print("Importing necessary libraries...")
import pandas as pd
import os
from pyspark.sql import SparkSession # SPARK VERSION 3.3.2
from pyspark.sql.types import StringType
import unicodedata
import json
from pyspark.sql.functions import col, lower, regexp_replace, udf
from pyspark.sql.types import StringType



#%%
# EXPLORARION: CHECKING OUT ALL THE FILES TO SEE WHAT/HOW WE NEED TO STANDARDIZE:::

# Paths to the data files
income_path = "../Landing_Zone/Income/2017_Distribució_territorial_renda_familiar.csv"
lookup_income_path = "../Landing_Zone/lookup_tables/income_opendatabcn_extended.csv"
lookup_idealista_path = "../Landing_Zone/lookup_tables/idealista_extended.csv"
base_dir_idealista = "../Landing_Zone/Idealista/"
dir_unemployment = "../Landing_Zone/Unemployment/2020_atur_per_sexe.json"


# Function to quickly summarize a DataFrame
def quick_summary(df, name="df"):
    print(f"\n SUMMARY OF {name}")
    print(" - Columns:", df.columns.tolist())
    print(" - Data types:\n", df.dtypes)
    print(" - Nulls:\n", df.isnull().sum())
    for col in df.columns:
        print(f" - {col}: {df[col].nunique()} unique values")
        print(df[col].unique()[:10])  # print sample

# 1. Inome Data
print("Loading income data from:", income_path)
income = pd.read_csv(income_path)

quick_summary(income, "Income Data")

'''
We can observe the following:
1. The `Nom_Districte` column contains the names of districts in Barcelona.
2. The `Nom_Barri` column contains the names of neighborhoods.
3. The `Codi_Barri` column contains the codes (1-73) for neighborhoods.
4. The `Renda_Familiar_Media` column contains the average family income for each neighborhood. (Numeric)
5. The `Codi_Districte` column contains the labels (1-10) for districts.
6. the `Poblacio` column contains the population of each neighborhood.
'''


# After confirming that these makes sense, we proceed do the same with the remaining data.
# 2. Lookup Income Data
print("Loading lookup income data from:", lookup_income_path)
lookup_income = pd.read_csv(lookup_income_path)
quick_summary(lookup_income, "Lookup Income Data")

'''

We can observe the following:
1. The `district` column contains the names of districts in Barcelona. They are the same as the `Nom_Districte` column in the income data.
2. The `district_id` column contains the codes/IDs for districts.
3. The `neighborhood` column contains the names of neighborhoods.
4. The `neighborhood_id` column contains the codes/IDs for neighborhoods. They differ from the `Codi_Barri` column in the income data.
5. the `district_n_reconciled` column contains the names of districts after reconciliation.
6. the `district_n` column contains the names of districts in Barcelona in lower case
7. the `neighborhood_n` column contains the names of neighborhoods in Barcelona in lower case
8. the `neighborhood_n_reconciled` column contains the names of neighborhoods after reconciliation.

'''


# 3. Lookup Idealista Data
print("Loading lookup idealista data from:", lookup_idealista_path)
lookup_idealista = pd.read_csv(lookup_idealista_path)
lookup_idealista.head()
quick_summary(lookup_idealista, "Lookup Idealista Data")

'''
We can observe the following:

It's the same as the lookup income data?? (but for idealista data.?)
'''
# 4. Idealista Data
#READING ALL JSON FILES FROM IDEALISTA
idealista_files = [f for f in os.listdir(base_dir_idealista) if f.startswith('2020')]
print("Found Idealista JSON files:", idealista_files)
print("Loading Idealista data...")

all_dfs = []
for fname in idealista_files:
    path = os.path.join(base_dir_idealista, fname)
    # 1) load the raw JSON
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)

    all_dfs.append(df)

idealista = pd.concat(all_dfs, ignore_index=True)
quick_summary(idealista, "Idealista Data")

'''

We can observe the following:
1. The `propertyCode` column contains the unique codes for each property.
2. The `thumbnail` column contains the URLs of the property pictures.
3. The `externalReference` column contains the external reference codes for properties.
4. The `numPhotos` column contains the number of photos for each property.
5. The `price` column contains the price of the property.
6. The `propertyType` column contains the type of property (e.g., flat, house).
7. The `operation` column contains the type of operation (e.g., sale, rent).
8. The `size` column contains the size of the property in square meters.
9. The `rooms` column contains the number of rooms in the property.
10. The `bathrooms` column contains the number of bathrooms in the property.
11. The `address` column contains the address of the property.
12. The `province` column contains the province of the property.
13 . The `municipality` column contains the municipality of the property.
14. The `district` column contains the district of the property. These do not strictly match the lookup tables, so we will need to reconcile them.  
15. The `neighborhood` column contains the neighborhood of the property. These do not strictly match the lookup tables, so we will need to reconcile them.
16. The `latitude` column contains the latitude of the property.
17. The `longitude` column contains the longitude of the property.
18. The `showAddress` column indicates whether the address is shown or not.
19. The `URL` column contains the URL of the property listing.
20. The `distance` column contains the distance of the property from a reference point.
21. The `hasVideo` column indicates whether the property has a video or not.
22. The `status` column contains the status of the property (e.g., good, renew, newdevelopment, nan).
23. The `newDevelopment` column indicates whether the property is a new development or not.
24. The `priceByArea` column contains the price per square meter of the property.
25. The `hasPlan` column indicates whether the property has a plan or not.
26. The `has3DTour` column indicates whether the property has a 3D tour or not.
27. The `has360` column indicates whether the property has a 360-degree view or not.
28. The `hasStaging` column indicates whether the property has staging or not.
29. The `topNewDevelopment` column indicates whether the property is a top new development or not.
30. The `parkingSpace` column indicates whether the property has a parking space or not.
31. The `parkingSpace.isParkingSpaceIncludedInPrice` column indicates whether the parking space is included in the price or not.
32. The `detailedType` column contains the detailed type of the property (e.g., flat, house, penthouse).
33. The `detailedType.subTypology` column contains the sub-typology of the property (e.g., flat, house, penthouse).
34. The `suggestedTexts.subtitle` column contains the subtitle of the property listing.
35. The `suggestedTexts.title` column contains the title of the property listing.
36. The `floor` column contains the floor of the property.
37. The `hasLift` column indicates whether the property has a lift or not.
38. The `parkingSpace.isParkingSpaceIncludedInPrice` column indicates whether the parking space is included in the price or not.
39. The `newDevelopmentFinished` column indicates whether the new development is finished or not.

'''
#READING UNEMPLOYMENT DATA FROM JSON FILE
j = json.load(open(dir_unemployment, "r", encoding="utf-8"))
print(j)
records = j["result"]["records"]
print(records)
# Convert the JSON data to a DataFrame
unemployment = pd.json_normalize(records)
quick_summary(unemployment, "Unemployment Data")

'''
We can observe the following:
1. The Codi_Districte column contains the codes for districts in Barcelona.
2. The Nom_Districte column contains the names of districts in Barcelona.
3. The Sexe column identifies the gender category of the population (here only “Homes”).
4. The Demanda_ocupacio column indicates the type of demand—either registered unemployment (“Atur Registrat”) or non-unemployed demand (“Demanda No Aturats”).
5. The Nom_Barri column lists the names of neighbourhoods within each district.
6. The Codi_Barri column holds the numeric codes corresponding to each neighbourhood.
7. The Mes column contains the month number for the data snapshot (all “1”, i.e. January).
8. The Any column contains the year of the data (all “2020”).
9. The Nombre column gives the headcount of people for each district-neighbourhood-sex-demand combination.
10. The _id column is a unique identifier for each record.

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


# STANDARDIZATION: ALIGNING COLUMNS WITH SPARK

# 1. LOAD INTO SPARK

# Load the tables into Spark DataFrames
# income:
income_df = spark.read.option("header", True).option("delimiter", ",").csv(income_path)
# income lookup:
lookup_income_df = spark.read.option("header", True).csv(lookup_income_path)
# idealista lookup:
lookup_idealista_df = spark.read.option("header", True).csv(lookup_idealista_path)
# idealista:
idealista_files = [f for f in os.listdir(base_dir_idealista) if f.startswith('2020')]
idealista_dfs = []
for fname in idealista_files:
    path = os.path.join (base_dir_idealista, fname)
    idealista_df = spark.read.json(path)
    idealista_dfs.append(idealista_df)
idealista_df = idealista_dfs[0]
for df in idealista_dfs[1:]:
    idealista_df = idealista_df.unionByName(df, allowMissingColumns=True)

# Unemployment:
j = json.load(open(dir_unemployment, "r", encoding="utf-8"))
records = j["result"]["records"]

unemployment_df = spark.createDataFrame(records)
unemployment_df.show(5)

#%%
# STANDARDIZATION: ALIGNING COLUMNS WITH SPARK

# 2. STANDARDIZE COLUMNS

# Income

from pyspark.sql.functions import col

income_df_st = income_df.select(
    col("Nom_Districte").           alias("district"),
    col("Codi_Districte").cast("int").alias("district_id"),
    col("Any").            cast("int").alias("year"),
    col("Nom_Barri").               alias("neighborhood"),
    col("Codi_Barri").   cast("int").alias("neighborhood_id"),
    col("Índex RFD Barcelona = 100").cast("double").alias("average_family_income"),
    col("Població").       cast("int").alias("population")
)

# Unemployment
unemployment_df_st = unemployment_df.select(
    col("Codi_Districte").cast("int").alias("district_id"),
    col("Nom_Districte").alias("district"),
    col("Codi_Barri").cast("int").alias("neighborhood_id"),
    col("Nom_Barri").alias("neighborhood"),
    col("Any").cast("int").alias("year"),
    col("Mes").cast("int").alias("month"),
    col("Nombre").cast("int").alias("count"),
    col("Demanda_ocupacio").alias("demand_type"),
    col("Sexe").alias("sex")
)



# Idealista


idealista_df_st = (
    idealista_df
      .withColumn("bathrooms",  col("bathrooms").cast("int"))
      .withColumn("numPhotos",  col("numPhotos").cast("int"))
      .withColumn("rooms",      col("rooms").cast("int"))
      .withColumn("price",      col("price").cast("double"))
      .withColumn("priceByArea",col("priceByArea").cast("double"))
      .withColumn("size",       col("size").cast("double"))
      .withColumn("latitude",   col("latitude").cast("double"))
      .withColumn("longitude",  col("longitude").cast("double"))
)

# Note, for idealista, the district column is not the district so we need to use the lookup table to get the correct district and neighborhood names
# lookup:
# |      district|        neighborhood|district_n_reconciled|    district_n|district_id|neighborhood_n_reconciled|      neighborhood_n|neighborhood_id|

#La Font d'en Fargues	la font d en fargues	
#idealista: 
# El Guinardó	el guinardo
# El Camp de l'Arpa del Clot	el camp de l arpa del clot
#%%

income_df_st.select("neighborhood").distinct().orderBy("neighborhood").show(100, truncate=False)

idealista_df_st.select("neighborhood").distinct().orderBy("neighborhood").show(100, truncate=False)

# --> we need to use lookup tables to standardize the neighborhood names

#%%

idealista_df.select("neighborhood").distinct().orderBy("neighborhood").show(100, truncate=False)

lookup_idealista_df.select("neighborhood").distinct().orderBy("neighborhood").show(100, truncate=False)






#%%

# Lets see if we can find the neighborhoods in the lookup table that match the idealista neighborhoods
# We will use a UDF to normalize the neighborhood names in both DataFrames
# Function to normalize text: lower case, strip accents, and collapse spaces
def strip_accents(txt):
    if txt is None:
        return None
    decomposed = unicodedata.normalize("NFD", txt)
    no_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return no_marks

strip_udf = udf(strip_accents, StringType())

from pyspark.sql.functions import trim

def normalize(colname):
    return trim(
      lower(
        regexp_replace(
          regexp_replace(
            regexp_replace(   # 1) strip accents
              strip_udf(colname),
              "-", " "        # 2) hyphens → spaces
            ),
            "[’']", " "      # 3) apostrophes → spaces
          ),
          r"\s+", " "        # 4) collapse multi-spaces → single
        )
      )
    )

# 3) apply normalization
idealista_df_norm = idealista_df_st \
   .withColumn("district_n",     normalize("district")) \
   .withColumn("neighborhood_n", normalize("neighborhood"))

# 4) now filter your normalized DF
barcelona = idealista_df_norm.filter(
    (col("municipality") == "Barcelona") &
    col("district_n").isNotNull() &
    col("neighborhood_n").isNotNull()
)

print("Before filtering & dropping nulls:", idealista_df_norm.count())
print("After filter to Barcelona & non-null keys:", barcelona.count())

# WE CAN NOW CHECK IF THE NEIGHBORHOODS IN THE IDEALISTA DATA MATCH THE LOOKUP TABLES BY JOINING
# NOTE: IN THE FORMATTED ZONE, WE NORMALLY DON'T JOIN BUT WE DO IT HERE TO CHECK THE MATCHES

# Inner-join on both normalized keys
joined = barcelona.join(
    lookup_idealista_df,
    on=[
        barcelona.neighborhood_n == lookup_idealista_df.neighborhood_n
        , barcelona.district_n == lookup_idealista_df.district_n

    ],
    how="inner"
)

print("After inner join:", joined.count())

print("final normalized data in `idealista_df_norm`")
idealista_df_norm = barcelona
#%%
# If we only get a few rows, we can check what rows were not matched. NOTE: this was an iterative process, so we may have to run this multiple times to get all the rows matched.:
# WE WILL THUS COMMENT THIS OUT FOR NOW, BUT YOU CAN UNCOMMENT IT TO CHECK THE MATCHES
'''
# 1) Barcelona‐only normalized districts
barcelona_districts = (
    df_norm
      .filter(col("municipality") == "Barcelona")
      .select("district_n")
      .distinct()
)

# 2) Lookup’s normalized districts
lookup_districts = (
    lookup_idealista_df
      .select("district_n")
      .distinct()
)

# 3) Which district_n appear in Barcelona but NOT in the lookup?
barcelona_districts.join(
    lookup_districts,
    on="district_n",
    how="left_anti"
).show(truncate=False)

# 4) Which district_n appear in the lookup but NOT in your Barcelona feed?
lookup_districts.join(
    barcelona_districts,
    on="district_n",
    how="left_anti"
).show(truncate=False)

'''

#%%

# Lastly, we will do the same process for the income data. NOTE: after checking the income data, we can see that the neighborhood names match the lookup table, so we don't need to normalize them. We will nevertheless do it for consistency and future proofing.

income_df_norm = income_df_st \
   .withColumn("district_n",     normalize("district")) \
   .withColumn("neighborhood_n", normalize("neighborhood"))

# 4) now filter your normalized DF
barcelona = income_df_norm.filter(
    col("district_n").isNotNull() &
    col("neighborhood_n").isNotNull()
)

print("Before filtering & dropping nulls:", income_df_norm.count())
print("After filter to Barcelona & non-null keys:", barcelona.count())

# Inner-join on both normalized keys
joined = barcelona.join(
    lookup_income_df,
    on=[
        barcelona.neighborhood_n == lookup_income_df.neighborhood_n
        , barcelona.district_n == lookup_income_df.district_n

    ],
    how="inner"
)

print("After inner join:", joined.count())

print("Final Income Data in `income_df_norm`")
income_df_norm = barcelona

#%%

# LASTLY, WE DO THE SAME WITH UNEMPLOYMENT DATA

unemployment_df_norm = unemployment_df_st \
    .withColumn("district_n", normalize("district"))\
    .withColumn("neighborhood_n", normalize("neighborhood"))


unemp_clean = unemployment_df_norm.filter(
    (col("district")     != "No consta") &
    (col("neighborhood") != "No consta") &
    (col("district_n").isNotNull()) &
    (col("neighborhood_n").isNotNull())
)
print("Before filtering & dropping nulls:", unemployment_df_norm.count())
print("After filter to Barcelona & non-null keys:", unemp_clean.count())


# Inner-join on both normalized keys
joined = unemp_clean.join(
    lookup_income_df,
    on=[
        unemp_clean.neighborhood_n == lookup_income_df.neighborhood_n
        , unemp_clean.district_n == lookup_income_df.district_n

    ],
    how="inner"
)

print("After inner join:", joined.count())
print("Final Unemployment Data in `unemployment_df_norm`")

unemployment_df_norm = unemp_clean


#%%

# Save the standardized DataFrames to the Formatted Zone
formatted_dir = "../Formatted_Zone/"
if not os.path.exists(formatted_dir):
    os.makedirs(formatted_dir)
# Save income data
income_df_norm.write.mode("overwrite").parquet(os.path.join(formatted_dir, "income.parquet"))
# Save unemployment data
unemployment_df_norm.write.mode("overwrite").parquet(os.path.join(formatted_dir, "unemployment.parquet"))
# Save idealista data
idealista_df_norm.write.mode("overwrite").parquet(os.path.join(formatted_dir, "idealista.parquet"))
# Save lookup income data
lookup_income_df.write.mode("overwrite").parquet(os.path.join(formatted_dir, "lookup_income.parquet"))
# Save lookup idealista data
lookup_idealista_df.write.mode("overwrite").parquet(os.path.join(formatted_dir, "lookup_idealista.parquet"))
# Print success message
print("Data successfully standardized and saved to the Formatted Zone!")

#%%