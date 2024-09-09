import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc

B19001_Headers = ['Less than $10,000', 'Percent Less than $10,000', '$10,000 to $14,999', 'Percent $10,000 to $14,999',
                  '$15,000 to $24,999', 'Percent $15,000 to $24,999', '$25,000 to $34,999', 'Percent $25,000 to $34,999',
                  '$35,000 to $49,999', 'Percent $35,000 to $49,999', '$50,000 to $74,999', 'Percent $50,000 to $74,999',
                  '$75,000 to $99,999', 'Percent $75,000 to $99,999', '$100,000 to $149,999',
                  'Percent $100,000 to $149,999', '$150,000 to $199,999', 'Percent $150,000 to $199,999',
                  '$200,000 or more', 'Percent $200,000 or more',]


Database_df_Headers = ['Less than $10,000', 'Percent Less than $10,000', '$10,000 to $14,999',
                       'Percent $10,000 to $14,999', '$15,000 to $24,999', 'Percent $15,000 to $24,999',
                       '$25,000 to $34,999', 'Percent $25,000 to $34,999', '$35,000 to $49,999',
                       'Percent $35,000 to $49,999', '$50,000 to $74,999', 'Percent $50,000 to $74,999',
                       '$75,000 to $99,999', 'Percent $75,000 to $99,999', '$100,000 to $149,999',
                       'Percent $100,000 to $149,999', '$150,000 to $199,999', 'Percent $150,000 to $199,999',
                       '$200,000 or more', 'Percent $200,000 or more']

year = 2012
year2  = year - 4
B19001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"
B19001_request = requests.get(B19001_URL)
B19001_data = B19001_request.json()
B19001_income = pd.DataFrame(B19001_data[1:], columns = B19001_data[0], )



B19001_income = pd.concat([pd.DataFrame([pd.to_numeric(B19001_income[e], errors = 'coerce')
                        for e in B19001_income.columns if e not in
                        ['GEO_ID','NAME','state','county','county subdivision']]).T,
                        B19001_income[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

# Get the names of the communities from the dataframe from the API
ordered_communities = []
for i in B19001_income["NAME"]:
    ordered_communities.append(i)
print(ordered_communities)

for i in B19001_Headers:
    B19001_income.insert(0, column= i, value= "NAN")

# Calculate each columns values for table B19001 Sum
B19001_income['Less than $10,000'] = B19001_income.loc[:,  ["B19001_002E"]].sum(axis = 1)
B19001_income['$10,000 to $14,999'] = B19001_income.loc[:,  ["B19001_003E"]].sum(axis = 1)
B19001_income['$15,000 to $24,999'] = B19001_income.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1)
B19001_income['$25,000 to $34,999'] = B19001_income.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1)
B19001_income['$35,000 to $49,999'] = B19001_income.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1)
B19001_income['$50,000 to $74,999'] = B19001_income.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1)
B19001_income['$75,000 to $99,999'] = B19001_income.loc[:,  ["B19001_013E"]].sum(axis = 1)
B19001_income['$100,000 to $149,999'] = B19001_income.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1)
B19001_income['$150,000 to $199,999'] = B19001_income.loc[:,  ["B19001_016E"]].sum(axis = 1)
B19001_income['$200,000 or more'] = B19001_income.loc[:,  ["B19001_017E"]].sum(axis = 1)

# Perform each table calculation for percentages Sum / Total
B19001_income['Percent Less than $10,000'] = B19001_income.loc[:,  ["B19001_002E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $10,000 to $14,999'] = B19001_income.loc[:,  ["B19001_003E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $15,000 to $24,999'] = B19001_income.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $25,000 to $34,999'] = B19001_income.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $35,000 to $49,999'] = B19001_income.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $50,000 to $74,999'] = B19001_income.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $75,000 to $99,999'] = B19001_income.loc[:,  ["B19001_013E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $100,000 to $149,999'] = B19001_income.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $150,000 to $199,999'] = B19001_income.loc[:,  ["B19001_016E"]].sum(axis = 1) / B19001_income["B19001_001E"]
B19001_income['Percent $200,000 or more'] = B19001_income.loc[:,  ["B19001_017E"]].sum(axis = 1) / B19001_income["B19001_001E"]

# Show the table we pulled with  all the added on columns for  the database dataframe
print("\nB19001 Table")
print(B19001_income.to_string())

# Initialize the Database Dataframe, use NANs for missing values then fill the columns in from B19001
community_series = pd.Series(ordered_communities)
Database_df = pd.DataFrame(np.nan, index = range(len(ordered_communities)), columns = Database_df_Headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", B19001_income["NAME"])
Database_df.insert(0, "STATE", "MA")

# Fill in data from B19001
Database_df['Less than $10,000'] = B19001_income['Less than $10,000']
Database_df['$10,000 to $14,999'] = B19001_income['$10,000 to $14,999']
Database_df['$15,000 to $24,999'] = B19001_income['$15,000 to $24,999']
Database_df['$25,000 to $34,999'] = B19001_income['$25,000 to $34,999']
Database_df['$35,000 to $49,999'] = B19001_income['$35,000 to $49,999']
Database_df['$50,000 to $74,999'] = B19001_income['$50,000 to $74,999']
Database_df['$75,000 to $99,999'] = B19001_income['$75,000 to $99,999']
Database_df['$100,000 to $149,999'] = B19001_income['$100,000 to $149,999']
Database_df['$150,000 to $199,999'] = B19001_income['$150,000 to $199,999']
Database_df['$200,000 or more'] = B19001_income['$200,000 or more']

Database_df['Percent Less than $10,000'] = B19001_income['Percent Less than $10,000']
Database_df['Percent $10,000 to $14,999'] = B19001_income['Percent $10,000 to $14,999']
Database_df['Percent $15,000 to $24,999'] = B19001_income['Percent $15,000 to $24,999']
Database_df['Percent $25,000 to $34,999'] = B19001_income['Percent $25,000 to $34,999']
Database_df['Percent $35,000 to $49,999'] = B19001_income['Percent $35,000 to $49,999']
Database_df['Percent $50,000 to $74,999'] = B19001_income['Percent $50,000 to $74,999']
Database_df['Percent $75,000 to $99,999'] = B19001_income['Percent $75,000 to $99,999']
Database_df['Percent $100,000 to $149,999'] = B19001_income['Percent $100,000 to $149,999']
Database_df['Percent $150,000 to $199,999'] = B19001_income['Percent $150,000 to $199,999']
Database_df['Percent $200,000 or more'] = B19001_income['Percent $200,000 or more']

# Clean up the community names
patterns = [", Franklin County, Massachusetts",
            ", Hampden County, Massachusetts",
            ", Hampshire County, Massachusetts",
            " town",
            " city",
            " Town"]
# Create a single regex pattern that matches any of the unwanted substrings
combined_pattern = "|".join(map(re.escape, patterns))

# Apply the regex substitution to the entire column
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].apply(lambda x: re.sub(combined_pattern, "", x))

# If you want to strip any leading/trailing whitespace
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].str.strip()

Database_df = Database_df.sort_values(by="COMMUNITY")

print("\nDatabase Dataframe")
print(Database_df.to_string())







