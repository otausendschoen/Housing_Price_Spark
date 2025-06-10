#THIS FILE INCLUDES THE LANDING PAGE CODE,WE USE IT TO LOAD DATA FROM THE LANDING ZONE AND FORMAT IT INTO THE FORMATTED ZONE


#%%
import pandas as pd
# USE PYSPARK INSTEAD:::

print("Loading data...")
income_path = "../Landing_Zone/Income/2017_Distribució_territorial_renda_familiar.csv"
lookup_income_path = "../Landing_Zone/income_opendatabcn_extended.csv"
lookup_idealista_path = "../Landing_Zone/idealista_extended.csv"

income_df = pd.read_csv(income_path)
income_df.head()

lookup_income = pd.read_csv(lookup_income_path)
lookup_income.head()

lookup_idealista = pd.read_csv(lookup_idealista_path)
lookup_idealista.head()





#%%
