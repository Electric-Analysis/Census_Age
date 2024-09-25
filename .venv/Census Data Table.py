import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc
import time


'''
This script is designed to pull data from the very large Census_Data table. This table has a large number of tables
that it is pulling from therefore we need to list out every table, the year, and every row for those tables. 

'''

start = time.time()

api_key = "&key=49099c23404527970655eaa39f3d39c606843dfc"

year = 2022
timevalue = year-4

# List out all the tables being accessed in the URL
URL_tables = ['B02001','B03001','B05002','B05007','B06009','B08006','B09005','B15003','B16004','B17024','B17025',
              'B17026','B19001','B19013','B19053','B19055','B19057','B19059','B19083','B19113','B05009']


# URL_tables = ['B02001','B03001']

# List out all the headers from the Census Data Table, these must be in the same order as what we have in the database
# When looping through the headers on the actual table itself use this variable as it refers specifically to the table
# itself
dataframe_df_headers = ['CEN_POP','CEN_WORKERS','CEN_URBANPOP','CEN_RURALPOP','CEN_WHITE','CEN_BLACK','CEN_ASIAN',
                        'CEN_OTHRACE','CEN_TWO_MORE_NOT_HISP','MINORITY_PERCENT','CEN_HISPANIC','CEN_HOUSEHOLDS',
                        'CEN_MARRHOU','CEN_SINGPARHOU','CEN_SINGPAR_CHILD','CEN_ENGLISH','CEN_SPANISH','CEN_ASIANLANG',
                        'CEN_OTHLANG','CEN_FORBORN','CEN_RECENTFORBORN','CEN_NET_DOM_MIGR','CEN_SAMEHOUSE',
                        'CEN_SAMECOUNTY','CEN_WORKLIVE','CEN_PUBLICTRANS','CEN_NOHSDIP','CEN_BADEG','CEN_MEDHHINC',
                        'CEN_SELFEMPINC','CEN_SOCSECINC','CEN_PAINC','CEN_RETINC','CEN_MEDFAMINC','CEN_FAMECON_SELF',
                        'CEN_PERCAPINC','CEN_INCOME_INEQ','CEN_POVTOTALPOP','CEN_POOR','CEN_POVRATE','CENPOV200',
                        'CEN_CHILD_POV','CEN_FAM_POV','CEN_POV_FOR_BORN','CEN_POV_FOR_BORN_NAT',
                        'CEN_POV_FOR_BORN_NON_CIT','MINORITY_POV_CONC','CEN_HHINC_0-9999','CEN_PERC_HHINC_0-9999',
                        'CEN_HHINC_10000-14999','CEN_PERC_HHINC_10000-14999','CEN_HHINC_15000-24999',
                        'CEN_PERC_HHINC_15000-24999','CEN_HHINC_25000-34999','CEN_PERC_HHINC_25000-34999',
                        'CEN_HHINC_35000-49999','CEN_PERC_HHINC_35000-49999','CEN_HHINC_50000-74999',
                        'CEN_PERC_HHINC_50000-74999','CEN_HHINC_75000-99999','CEN_PERC_HHINC_75000-99999',
                        'CEN_HHINC_100000-149999','CEN_PERC_HHINC_100000-149999','CEN_HHINC_150000-199999',
                        'CEN_PERC_HHINC_150000-199999','CEN_HHINC_200000_PLUS','CEN_PERC_HHINC_200000_PLUS']



# List out community names for the database_df communities column
dataframe_df_communities = ['Agawam','Amherst','Ashfield','Belchertown','Bernardston','Blandford','Brimfield',
                            'Buckland','Charlemont','Chester','Chesterfield','Chicopee','Colrain','Conway','Cummington',
                            'Deerfield','East Longmeadow','Easthampton','Erving',
                            # 'Franklin County',
                            'Gill','Goshen',
                            'Granby','Granville','Greenfield','Hadley','Hampden',
                            # 'Hampden County','Hampshire County',
                            'Hatfield','Hawley','Heath','Holland','Holyoke','Huntington','Leverett','Leyden',
                            'Longmeadow','Ludlow',
                            # 'Massachusetts',
                            'Middlefield','Monroe','Monson','Montague',
                            'Montgomery','New Salem','Northampton','Northfield','Orange','Palmer','Pelham',
                            # 'Pioneer Valley',
                            'Plainfield',
                            # 'PVPC Region',
                            'Rowe','Russell','Shelburne','Shutesbury',
                            'South Hadley','Southampton','Southwick','Springfield','Sunderland','Tolland','Wales',
                            'Ware','Warwick','Wendell','West Springfield','Westfield','Westhampton','Whately',
                            'Wilbraham','Williamsburg','Worthington']

# this table is missing 2 values, base table and api access have 65 values, 67 headers
address_book_df = pd.DataFrame({"Table Headers": dataframe_df_headers,
                                "Base Table":['B02001','NA','NA','NA','B02001','B02001','B02001','B02001','B02001',
                                              'B02001','B03001','B19001','B09005','NA','B05009','B16004','B16004',
                                              'B16004','B16004','B05002','B05007','NA','NA','NA','NA','B08006','B15003',
                                              'B06009','B19013','B19053','B19055','B19057','B19059','B19113','NA','NA',
                                              'B19083','B17025','B17025','B17025','B17024','B17024','B17026','B17025',
                                              'NA','NA','NA','B19001','NA','B19001','NA','B19001','NA','B19001','NA',
                                              'B19001','NA','B19001','NA','B19001','NA','B19001','NA','B19001','NA',
                                              'B19001','NA'],
                                "API Access": [1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0,
                                               1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1,
                                               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ],
                                })
init_time = time.time()
print(f"Values Initialized \n{round(init_time - start,4)} seconds\n")

# instantiate the database dataframe, check the column header order and names as well as the communites
Database_df = pd.DataFrame(np.nan, index = range(len(dataframe_df_communities)), columns = dataframe_df_headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{timevalue}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", dataframe_df_communities)
Database_df.insert(0, "STATE", "MA")
table_init = time.time()
print(f"Table Initialized \n{round(table_init - start,4)} seconds\n")

# Get and store all necessary tables to develop the Census Data table in a list
get_tables = time.time()
print(f"Getting tables from Census API {round(get_tables - start,4)}\n")
dataframe_repo = []
for table in URL_tables:
    table_pull = time.time()
    URL = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group(" + table + ")&for=county%20subdivision:*&in=state:25%20county:011,013,015" + api_key
    print(f"{round(table_pull - start,4)} seconds\n{URL}")
    table_request = requests.get(URL)
    JSON_data = table_request.json()
    temp_df = pd.DataFrame(JSON_data[1:], columns = JSON_data[0],)
    dataframe_repo.append(temp_df)
    # print(f"\nAPI Table {table}:\n{temp_df}")
got_tables = time.time()
print(f"Getting tables from Census API\n{got_tables - start}\n")
# print(temp_df.dtypes)

# Assign names to each of the tables in the dataframe Repo so we can get handles on each table
table_B02001 = dataframe_repo[0]
table_B03001 = dataframe_repo[1]
table_B05002 = dataframe_repo[2]
table_B05007 = dataframe_repo[3]
table_B06009 = dataframe_repo[4]
table_B08006 = dataframe_repo[5]
table_B09005 = dataframe_repo[6]
table_B15003 = dataframe_repo[7]
table_B16004 = dataframe_repo[8]
table_B17024 = dataframe_repo[9]
table_B17025 = dataframe_repo[10]
table_B17026 = dataframe_repo[11]
table_B19001 = dataframe_repo[12]
table_B19013 = dataframe_repo[13]
table_B19053 = dataframe_repo[14]
table_B19055 = dataframe_repo[15]
table_B19057 = dataframe_repo[16]
table_B19059 = dataframe_repo[17]
table_B19083 = dataframe_repo[18]
table_B19113 = dataframe_repo[19]
table_B05009 = dataframe_repo[20]

# Put the tables back into the dataframe repo so we have them all on one place again
dataframe_repo = [table_B02001, table_B03001, table_B05002, table_B05007, table_B06009, table_B08006, table_B09005,
                  table_B15003, table_B16004, table_B17024, table_B17025, table_B17026, table_B19001, table_B19013,
                  table_B19053, table_B19055, table_B19057, table_B19059, table_B19083, table_B19113, table_B05009]


for base_table_code, table in zip(URL_tables, dataframe_repo):
    for index, row in address_book_df.iterrows():
        if row['Base Table'] == base_table_code:
            header = row['Table Headers']
            table.insert(0, header, 'NAN')

# Convert all but Geo_ID, Name, State, county, and county subdivision to a float data type

def column_converter(temp_df):
    temp_df = pd.concat([pd.DataFrame([pd.to_numeric(temp_df[e], errors='coerce')
        for e in temp_df.columns if e not in
        ['GEO_ID', 'NAME', 'state', 'county', 'county subdivision']]).T,
        temp_df[['GEO_ID', 'NAME', 'state', 'county', 'county subdivision']]], axis=1)
    return temp_df

def community_cleaner(table):
    # List the string patterns that we will delete
    patterns = [", Franklin County, Massachusetts",
                ", Hampden County, Massachusetts",
                ", Hampshire County, Massachusetts",
                " town",
                " city",
                " Town"]
    # Create a single regex pattern that matches any of the unwanted substrings
    combined_pattern = "|".join(map(re.escape, patterns))
    # Apply the regex substitution to the entire column
    table["NAME"] = table["NAME"].apply(lambda x: re.sub(combined_pattern, "", x))
    # If you want to strip any leading/trailing whitespace
    table["NAME"] = table["NAME"].str.strip()
    table = table.sort_values(by="NAME")
    return table


# Allocate community values for B02001
print("\ntable_B02001")
table_B02001 = column_converter(table_B02001)
table_B02001['CEN_POP'] = table_B02001.loc[:,  ["B02001_001E"]].sum(axis = 1)
table_B02001['CEN_WHITE'] = table_B02001.loc[:,  ["B02001_002E"]].sum(axis = 1)
table_B02001['CEN_BLACK'] = table_B02001.loc[:,  ["B02001_003E"]].sum(axis = 1)
table_B02001['CEN_ASIAN'] = table_B02001.loc[:,  ["B02001_005E"]].sum(axis = 1)
table_B02001['CEN_OTHRACE'] = table_B02001.loc[:,  ["B02001_007E"]].sum(axis = 1)
table_B02001['CEN_TWO_MORE_NOT_HISP'] = table_B02001.loc[:,  ["B02001_008E"]].sum(axis = 1)
table_B02001['MINORITY_PERCENT'] = table_B02001.loc[:,  ["B02001_002E"]].sum(axis = 1) / table_B02001["B02001_001E"]
table_B02001 = community_cleaner(table_B02001)
print(table_B02001.to_string())
#
# # Allocate community values for B03001
# print("\ntable_B03001")
# table_B03001 = column_converter(table_B03001)
# table_B03001['CEN_HISPANIC'] = table_B03001.loc[:,  ["B03001_001E"]].sum(axis = 1)
# print(table_B03001.to_string())
#
# # Allocate community values for B05002
# print("\ntable_B05002")
# table_B05002 = column_converter(table_B05002)
# table_B05002['CEN_FORBORN'] = table_B05002.loc[:,  ["B05002_013E"]].sum(axis = 1)
# print(table_B05002.to_string())
#
# # Allocate community values for B05007
# print("\ntable_B05007")
# table_B05007 = column_converter(table_B05007)
# table_B05007['CEN_RECENTFORBORN'] = table_B05007.loc[:,  ["B05007_002E"]].sum(axis = 1)
# print(table_B05007.to_string())
#
# # Allocate community values for B05009
# print("\ntable_B05009")
# table_B05009 = column_converter(table_B05009)
# table_B05009['CEN_SINGPAR_CHILD'] = (table_B05009.loc[:,  ["B05009_013E"]].sum(axis = 1)
#                                      + table_B05009.loc[:,  ["B05009_031E"]].sum(axis = 1))
# print(table_B05009.to_string())
#
# # Allocate community values for B06009
# print("\ntable_B06009")
# table_B06009 = column_converter(table_B06009)
# table_B06009['CEN_BADEG'] = (table_B06009.loc[:,  ["B06009_005E"]].sum(axis = 1) +
#                                      table_B06009.loc[:,  ["B06009_006E"]].sum(axis = 1))
# print(table_B06009.to_string())
#
# # Allocate community values for B08006
# print("\ntable_B08006")
# table_B08006 = column_converter(table_B08006)
# table_B08006['CEN_PUBLICTRANS'] = table_B08006.loc[:,  ["B08006_008E"]].sum(axis = 1)
# print(table_B08006.to_string())
#
# # Allocate community values for B09005
# print("\ntable_B09005")
# table_B09005 = column_converter(table_B09005)
# table_B09005['CEN_MARRHOU'] = table_B09005.loc[:,  ["B09005_001E"]].sum(axis = 1)
# print(table_B09005.to_string())
#
# # Allocate community values for B15003
# print("\ntable_B15003")
# table_B15003 = column_converter(table_B15003)
# table_B15003['CEN_NOHSDIP'] = table_B15003.loc[:,  ["B15003_002E"]].sum(axis = 1)
# print(table_B15003.to_string())
#
# # Allocate community values for B16004
# print("\ntable_B16004")
# table_B16004 = column_converter(table_B16004)
# table_B16004['CEN_ENGLISH'] = ((table_B16004.loc[:,  ["B16004_003E"]].sum(axis = 1) +
#                                table_B16004.loc[:,  ["B16004_025E"]].sum(axis = 1)) +
#                                table_B16004.loc[:,  ["B16004_053E"]].sum(axis = 1))
# table_B16004['CEN_SPANISH'] = ((table_B16004.loc[:,  ["B16004_004E"]].sum(axis = 1) +
#                                table_B16004.loc[:,  ["B16004_026E"]].sum(axis = 1)) +
#                                table_B16004.loc[:,  ["B16004_048E"]].sum(axis = 1))
# table_B16004['CEN_ASIANLANG'] = ((table_B16004.loc[:,  ["B16004_014E"]].sum(axis = 1) +
#                                table_B16004.loc[:,  ["B16004_036E"]].sum(axis = 1)) +
#                                table_B16004.loc[:,  ["B16004_058E"]].sum(axis = 1))
# table_B16004['CEN_OTHLANG'] = ((table_B16004.loc[:,  ["B16004_019E"]].sum(axis = 1) +
#                                table_B16004.loc[:,  ["B16004_041E"]].sum(axis = 1)) +
#                                table_B16004.loc[:,  ["B16004_063E"]].sum(axis = 1))
# print(table_B16004.to_string())
#
# # Allocate community values for B17024
# print("\ntable_B17024")
# table_B17024 = column_converter(table_B17024)
# table_B17024['CEN_CHILD_POV'] = table_B17024.loc[:, ["B17024_003E", "B17024_004E", "B17024_005E", "B17024_016E",
#                                                      "B17024_017E", "B17024_018E", "B17024_029E", "B17024_030E",
#                                                      "B17024_031E", "B17024_042E", "B17024_043E", "B17024_044E",
#                                                      "B17024_055E", "B17024_056E", "B17024_057E", "B17024_068E",
#
#                                                      "B17024_069E", "B17024_070E", "B17024_081E", "B17024_082E",
#                                                      "B17024_083E", "B17024_094E", "B17024_095E", "B17024_096E",
#                                                      "B17024_107E", "B17024_108E", "B17024_109E", "B17024_120E",
#                                                      "B17024_121E", "B17024_122E"
#                                                      ]].sum(axis = 1)
#
# table_B17024['CENPOV200'] = table_B17024.loc[:,   ["B17024_003E", "B17024_004E", "B17024_005E", "B17024_006E",
#                                                    "B17024_007E", "B17024_008E", "B17024_009E", "B17024_010E",
#                                                    "B17024_016E", "B17024_017E", "B17024_018E", "B17024_019E",
#                                                    "B17024_020E", "B17024_021E", "B17024_022E", "B17024_023E",
#
#                                                    "B17024_029E", "B17024_030E", "B17024_031E", "B17024_032E",
#                                                    "B17024_033E", "B17024_034E", "B17024_035E", "B17024_036E",
#                                                    "B17024_042E", "B17024_043E", "B17024_044E", "B17024_045E",
#                                                    "B17024_046E", "B17024_047E", "B17024_048E", "B17024_049E",
#
#                                                    "B17024_055E", "B17024_056E", "B17024_057E", "B17024_058E",
#                                                    "B17024_059E", "B17024_060E", "B17024_061E", "B17024_062E",
#                                                    "B17024_068E", "B17024_069E", "B17024_070E", "B17024_071E",
#                                                    "B17024_072E", "B17024_073E", "B17024_074E", "B17024_075E",
#
#                                                    "B17024_081E", "B17024_082E", "B17024_083E", "B17024_084E",
#                                                    "B17024_085E", "B17024_086E", "B17024_087E", "B17024_088E",
#                                                    "B17024_094E", "B17024_095E", "B17024_096E", "B17024_097E",
#                                                    "B17024_098E", "B17024_099E", "B17024_100E", "B17024_101E",
#
#                                                    "B17024_107E", "B17024_108E", "B17024_109E", "B17024_110E",
#                                                    "B17024_111E", "B17024_112E", "B17024_113E", "B17024_114E",
#                                                    "B17024_120E", "B17024_121E", "B17024_122E", "B17024_123E",
#                                                    "B17024_124E", "B17024_125E", "B17024_126E", "B17024_127E"
#                                                    ]].sum(axis = 1)
# print(table_B17024.to_string())
#
# # Allocate community values for B17025
# print("\ntable_B17025")
# table_B17025 = column_converter(table_B17025)
# table_B17025['CEN_POVRATE'] = table_B17025.loc[:,  ["B17025_002E"]].sum(axis = 1) / table_B17025["B17025_001E"]
# table_B17025['CEN_POVTOTALPOP'] = table_B17025.loc[:,  ["B17025_001E"]].sum(axis = 1)
# table_B17025['CEN_POOR'] = table_B17025.loc[:,  ["B17025_002E"]].sum(axis = 1)
# table_B17025['CEN_POV_FOR_BORN'] = table_B17025.loc[:,  ["B17025_006E"]].sum(axis = 1)
# print(table_B17025.to_string())
#
# # Allocate community values for B17026
# print("\ntable_B17026")
# table_B17026 = column_converter(table_B17026)
# table_B17026['CEN_FAM_POV'] = table_B17026.loc[:,  ["B17026_002E","B17026_003E","B17026_004E"]].sum(axis = 1)
# print(table_B17026.to_string())
#
# # Allocate community values for B19001
# print("\ntable_B19001")
# table_B19001 = column_converter(table_B19001)
# table_B19001['CEN_HOUSEHOLDS'] = table_B19001.loc[:,  ["B19001_001E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_0-9999'] = table_B19001.loc[:,  ["B19001_002E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_10000-14999'] = table_B19001.loc[:,  ["B19001_003E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_15000-24999'] = table_B19001.loc[:,  ["B19001_004E","B19001_005E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_25000-34999'] = table_B19001.loc[:,  ["B19001_006E","B19001_007E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_35000-49999'] = table_B19001.loc[:,  ["B19001_008E","B19001_009E","B19001_010E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_50000-74999'] = table_B19001.loc[:,  ["B19001_011E","B19001_012E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_75000-99999'] = table_B19001.loc[:,  ["B19001_013E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_100000-149999'] = table_B19001.loc[:,  ["B19001_014E","B19001_015E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_150000-199999'] = table_B19001.loc[:,  ["B19001_016E"]].sum(axis = 1)
# table_B19001['CEN_HHINC_200000_PLUS'] = table_B19001.loc[:,  ["B19001_017E"]].sum(axis = 1)
# print(table_B19001.to_string())
#
# # Allocate community values for B19013
# print("\ntable_B19013")
# table_B19013 = column_converter(table_B19013)
# table_B19013['CEN_MEDHHINC'] = table_B19013.loc[:,  ["B19013_001E"]].sum(axis = 1)
# print(table_B19013.to_string())
#
# # Allocate community values for B19053
# print("\ntable_B19053")
# table_B19053 = column_converter(table_B19053)
# table_B19053['CEN_SELFEMPINC'] = table_B19053.loc[:,  ["B19053_002E"]].sum(axis = 1)
# print(table_B19053.to_string())
#
# # Allocate community values for B19055
# print("\ntable_B19055")
# table_B19055 = column_converter(table_B19055)
# table_B19055['CEN_SOCSECINC'] = table_B19055.loc[:,  ["B19055_002E"]].sum(axis = 1)
# print(table_B19055.to_string())
#
# # Allocate community values for B19057
# print("\ntable_B19057")
# table_B19057 = column_converter(table_B19057)
# table_B19057['CEN_PAINC'] = table_B19057.loc[:,  ["B19057_001E"]].sum(axis = 1)
# print(table_B19057.to_string())
#
# # Allocate community values for B19059
# print("\ntable_B19059")
# table_B19059 = column_converter(table_B19059)
# table_B19059['CEN_RETINC'] = table_B19059.loc[:,  ["B19059_001E"]].sum(axis = 1)
# print(table_B19059.to_string())
#
# # Allocate community values for B19083
# print("\ntable_B19083")
# table_B19083 = column_converter(table_B19083)
# table_B19083['CEN_INCOME_INEQ'] = table_B19083.loc[:,  ["B19083_001E"]].sum(axis = 1)
# print(table_B19083.to_string())
#
# # Allocate community values for B19113
# print("\ntable_B19113")
# table_B19113 = column_converter(table_B19113)
# table_B19113['CEN_MEDFAMINC'] = table_B19113.loc[:,  ["B19113_001E"]].sum(axis = 1)
# print(table_B19113.to_string())


print(f"\nDatabase Dataframe:\n{Database_df}")

end = time.time()
print(f"Run Time: {round(end-start,4)} seconds")




















