import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc

'''
This script is designed to fully automate the acquisition of data from table B19001 for town, county, and state levels 
of geography. Additionally code is being developed to include Pioneer Valley and PVPC region which are fundamentally
row aggregates of county level data. By running this script the user will make an API call to the census bureau in 3
seperate instances. First it will call for the county subdivision level data, then again for the county level data,
last it will call for the state level data (just a single row). Next the data will be converted into a format that is 
readably by python and put into a dataframe or a table. These tables will then be merged so the county and state level
data are in-line with county sub-division level data in one table. Following this columns are inserted into this merged
dataframe and then data is extracted from the dataset and filled into the new columns. This is the data we want in our
final output. Once all the data is reported and calculated in these columns that we made we then initialize an empty 
table that will be the final container for the data we need. Once the database dataframe is made we then move all the 
data from the columns we made in the original dataframe into the new database dataframe. This database dataframe is 
complete with all the static information that is typical to any other table we keep on file indicating STATE, TIME_TYPE,
TIME_VALUE, YEAR, and each table specific column. Finally now that we have all the data we need in one spot with the 
extra data stripped away we can clean up the community names so they are less descriptive and more readable at a glance.
Lastly now that the table is full  of accurate and readable information we can print this table to a csv file designated
to a directory and with the year of the data request built into the name. 

NOTE: if you have a file open with the same
as a file you are trying to create it will throw an error. For example you have Census_Income_2022 open. When you try to 
get census income for 2021 it will work but 2022 will not because its open, make sure you close it. This WILL overwrite 
any data in existing files with the same name. Comment out the last line of code to skip writing to csv by adding a 
hashtag to the beginning of the line this will render the line completely neutralized. 

From this (live code)
Database_df.to_csv("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Income Data/Census Income " + str(year) + ".csv")

To this (dead code) note the # symbol
# Database_df.to_csv("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Income Data/Census Income " + str(year) + ".csv")

'''

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

dataframes = []

year = 2022
year2  = year - 4
B19001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"
B19001_URL_CNTY = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=county:011,013,015&in=state:25"
B19001_URL_STATE = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B19001)&for=state:25"
URL = [B19001_URL, B19001_URL_CNTY, B19001_URL_STATE]

B19001_request = requests.get(B19001_URL)

for i in URL:
    request = requests.get(i)
    JSON  = request.json()
    df =  pd.DataFrame(JSON[1:], columns=JSON[0], )
    print(df.shape)
    print(df.to_string())
    dataframes.append(df)


# Merge the dataframes
merged_df = pd.concat(dataframes, join='inner', ignore_index=True)


# Convert all but  the listed columns into a numeric data type so calculations can be performed

merged_df = pd.concat([pd.DataFrame([pd.to_numeric(merged_df[e], errors = 'coerce')
                        for e in merged_df.columns if e not in
                        ['GEO_ID','NAME','state']]).T,
                        merged_df[['GEO_ID','NAME','state']]], axis = 1)


# print(merged_df.dtypes.to_string())

# for i  in  merged_df['NAME']:
#     print(i)


ordered_communities = []
for i in merged_df["NAME"]:
    ordered_communities.append(i)
# print(f"ordered_communities:\n{ordered_communities}")


for i in B19001_Headers:
    merged_df.insert(0, column= i, value= "NAN")



# Calculate each columns values for table B19001 Sum
merged_df['Less than $10,000'] = merged_df.loc[:,  ["B19001_002E"]].sum(axis = 1)
merged_df['$10,000 to $14,999'] = merged_df.loc[:,  ["B19001_003E"]].sum(axis = 1)
merged_df['$15,000 to $24,999'] = merged_df.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1)
merged_df['$25,000 to $34,999'] = merged_df.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1)
merged_df['$35,000 to $49,999'] = merged_df.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1)
merged_df['$50,000 to $74,999'] = merged_df.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1)
merged_df['$75,000 to $99,999'] = merged_df.loc[:,  ["B19001_013E"]].sum(axis = 1)
merged_df['$100,000 to $149,999'] = merged_df.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1)
merged_df['$150,000 to $199,999'] = merged_df.loc[:,  ["B19001_016E"]].sum(axis = 1)
merged_df['$200,000 or more'] = merged_df.loc[:,  ["B19001_017E"]].sum(axis = 1)


# Perform each table calculation for percentages Sum / Total
merged_df['Percent Less than $10,000'] = merged_df.loc[:,  ["B19001_002E"]].sum(axis = 1) / merged_df["B19001_001E"]
merged_df['Percent $10,000 to $14,999'] = round(merged_df.loc[:,  ["B19001_003E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $15,000 to $24,999'] = round(merged_df.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $25,000 to $34,999'] = round(merged_df.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $35,000 to $49,999'] = round(merged_df.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $50,000 to $74,999'] = round(merged_df.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $75,000 to $99,999'] = round(merged_df.loc[:,  ["B19001_013E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $100,000 to $149,999'] = round(merged_df.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $150,000 to $199,999'] = round(merged_df.loc[:,  ["B19001_016E"]].sum(axis = 1) / merged_df["B19001_001E"],4)
merged_df['Percent $200,000 or more'] = round(merged_df.loc[:,  ["B19001_017E"]].sum(axis = 1) / merged_df["B19001_001E"],4)

print("\n\nMerged Dataframe")
print(merged_df.to_string())

community_series = pd.Series(ordered_communities)
Database_df = pd.DataFrame(np.nan, index = range(len(ordered_communities)), columns = Database_df_Headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", merged_df["NAME"])
Database_df.insert(0, "STATE", "MA")

# Fill in data from B19001
Database_df['Less than $10,000'] = merged_df['Less than $10,000']
Database_df['$10,000 to $14,999'] = merged_df['$10,000 to $14,999']
Database_df['$15,000 to $24,999'] = merged_df['$15,000 to $24,999']
Database_df['$25,000 to $34,999'] = merged_df['$25,000 to $34,999']
Database_df['$35,000 to $49,999'] = merged_df['$35,000 to $49,999']
Database_df['$50,000 to $74,999'] = merged_df['$50,000 to $74,999']
Database_df['$75,000 to $99,999'] = merged_df['$75,000 to $99,999']
Database_df['$100,000 to $149,999'] = merged_df['$100,000 to $149,999']
Database_df['$150,000 to $199,999'] = merged_df['$150,000 to $199,999']
Database_df['$200,000 or more'] = merged_df['$200,000 or more']

Database_df['Percent Less than $10,000'] = round(merged_df['Percent Less than $10,000'], 4)
Database_df['Percent $10,000 to $14,999']= round(merged_df['Percent $10,000 to $14,999'], 4)
Database_df['Percent $15,000 to $24,999']= round(merged_df['Percent $15,000 to $24,999'], 4)
Database_df['Percent $25,000 to $34,999']= round(merged_df['Percent $25,000 to $34,999'], 4)
Database_df['Percent $35,000 to $49,999']= round(merged_df['Percent $35,000 to $49,999'], 4)
Database_df['Percent $50,000 to $74,999']= round(merged_df['Percent $50,000 to $74,999'], 4)
Database_df['Percent $75,000 to $99,999']= round(merged_df['Percent $75,000 to $99,999'], 4)
Database_df['Percent $100,000 to $149,999']= round(merged_df['Percent $100,000 to $149,999'], 4)
Database_df['Percent $150,000 to $199,999'] = round(merged_df['Percent $150,000 to $199,999'], 4)
Database_df['Percent $200,000 or more'] = round(merged_df['Percent $200,000 or more'], 4)


# Clean up the community names
patterns = [", Franklin County, Massachusetts",
            ", Hampden County, Massachusetts",
            ", Hampshire County, Massachusetts",
            " town",
            " city",
            " Town",
            ", Massachusetts"]
# Create a single regex pattern that matches any of the unwanted substrings
combined_pattern = "|".join(map(re.escape, patterns))

# Apply the regex substitution to the entire column
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].apply(lambda x: re.sub(combined_pattern, "", x))

# If you want to strip any leading/trailing whitespace
Database_df["COMMUNITY"] = Database_df["COMMUNITY"].str.strip()

Database_df = Database_df.sort_values(by="COMMUNITY")

print("\n\nDatabase Dataframe")
print(Database_df.to_string())

# Database_df.to_csv("C:/Users/jtilsch/OneDrive - Pioneer Valley Planning Commission/Desktop/Projects/Database Design/Data/Census Income Data/Census Income " + str(year) + ".csv")






