import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

# https://www.reddit.com/r/learnpython/comments/jo9825/learning_to_use_the_censusgov_api_i_have_a_lot_of/   #useful code for constructing string
url = "https://api.census.gov/data/2010/acs/acs5?get=group(B01001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"   #THIS  IS BIS

Census_Row_Specifiers = ['B09001_003','B09001_004','B09001_005','B09001_006','B09001_007','B09001_008','B09001_009',
                   'B01001_031','B01001_007','B01001_008','B01001_009','B01001_010','B01001_032','B01001_033',
                   'B01001_034','B01001_011','B01001_035','B01001_012','B01001_036','B01001_013','B01001_037',
                   'B01001_014','B01001_038','B01001_015','B01001_039','B01001_016','B01001_040','B01001_017',
                   'B01001_041','B01001_018','B01001_019','B01001_042','B01001_043','B01001_020','B01001_021',
                   'B01001_044','B01001_045','B01001_022','B01001_046','B01001_023','B01001_047','B01001_024',
                   'B01001_048','B01001_025','B01001_049']

communities = ['Agawam','Amherst','Ashfield','Belchertown','Bernardston','Blandford','Brimfield','Buckland',
               'Charlemont','Chester','Chesterfield','Chicopee','Colrain','Conway','Cummington','Deerfield',
               'East Longmeadow','Easthampton','Erving','Franklin County','Gill','Goshen','Granby','Granville',
               'Greenfield','Hadley','Hampden County','Hampden','Hampshire County','Hatfield','Hawley','Heath','Holland',
               'Holyoke','Huntington','Leverett','Leyden','Longmeadow','Ludlow','Middlefield','Monroe','Monson','Montague',
               'Montgomery','New Salem','Northampton','Northfield','Orange','Palmer','Pelham','Plainfield',
               'PVPC Region', 'Pioneer Valley','Rowe','Russell','Shelburne','Shutesbury','South Hadley','Southampton',
               'Southwick','Springfield','Sunderland','Tolland','Wales','Ware','Warwick','Wendell','West Springfield',
               'Westfield','Westhampton','Whately','Wilbraham','Williamsburg','Worthington']

Database_df_Headers = ['UNDER_3_YEARS','3_4_YEARS','5_YEARS','6_8_YEARS','9_11_YEARS','12_14_YEARS','15_17_YEARS',
                       '18_19_YEARS','20_24_YEARS','25_29_YEARS','30_34_YEARS','35_39_YEARS','40_44_YEARS','45_49_YEARS',
                       '50_54_YEARS','55_59_YEARS','60_64_YEARS','65_69_YEARS','70_74_YEARS','75_79_YEARS','80_84_YEARS',
                       '85+_YEARS','CEN_POP_O4','CEN_POP_U18','CEN_POP_O64','CEN_POP_O24']

B01001_Headers = ["18_19_YEARS", "20_24_YEARS", "25_29_YEARS", "30_34_YEARS", "35_39_YEARS", "40_44_YEARS",
                  "45_49_YEARS", "50_54_YEARS", "55_59_YEARS", "60_64_YEARS", "65_69_YEARS", "70_74_YEARS",
                  "75_79_YEARS", "80_84_YEARS", "85+_YEARS"]

year = "2021
api_key = "49099c23404527970655eaa39f3d39c606843dfc"
r = requests.get(url)

data = r.json()
B01001_Age = pd.DataFrame(data[1:], columns=data[0], )

# https://stackoverflow.com/questions/44602139/pandas-convert-all-column-from-string-to-number-except-two
# The above link will show how the line below converts all but set columns to numeric for calculations
# df2 = pd.concat([pd.DataFrame([pd.to_numeric(df[e],errors='coerce') for e in df.columns if e not in ['Name','Job']]).T, df[['Name','Job']]],axis=1)

B01001_Age = pd.concat([pd.DataFrame([pd.to_numeric(B01001_Age[e], errors= 'coerce')
                        for e in B01001_Age.columns if e not in
                        ['GEO_ID','NAME','state','county','county subdivision']]).T,
                        B01001_Age[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

#Instantiate new columns in the table
for i in B01001_Headers:
    B01001_Age.insert(0, column= i, value= "NAN")

# Calculate each column's values
B01001_Age["18_19_YEARS"] = B01001_Age.loc[:, ["B01001_007E","B01001_031E"]].sum(axis = 1)
B01001_Age["20_24_YEARS"] = B01001_Age.loc[:, ["B01001_008E","B01001_009E",
                                               "B01001_010E", "B01001_032E",
                                               "B01001_033E", "B01001_034E"]].sum(axis = 1)
B01001_Age["25_29_YEARS"] = B01001_Age.loc[:, ["B01001_011E","B01001_035E"]].sum(axis = 1)
B01001_Age["30_34_YEARS"] = B01001_Age.loc[:, ["B01001_012E","B01001_036E"]].sum(axis = 1)
B01001_Age["35_39_YEARS"] = B01001_Age.loc[:, ["B01001_013E","B01001_037E"]].sum(axis = 1)
B01001_Age["40_44_YEARS"] = B01001_Age.loc[:, ["B01001_014E","B01001_038E"]].sum(axis = 1)
B01001_Age["45_49_YEARS"] = B01001_Age.loc[:, ["B01001_015E","B01001_039E"]].sum(axis = 1)
B01001_Age["50_54_YEARS"] = B01001_Age.loc[:, ["B01001_016E","B01001_040E"]].sum(axis = 1)
B01001_Age["55_59_YEARS"] = B01001_Age.loc[:, ["B01001_017E","B01001_041E"]].sum(axis = 1)
B01001_Age["60_64_YEARS"] = B01001_Age.loc[:, ["B01001_018E","B01001_019E",
                                               "B01001_042E", "B01001_043E"]].sum(axis = 1)
B01001_Age["65_69_YEARS"] = B01001_Age.loc[:, ["B01001_020E","B01001_021E",
                                               "B01001_044E", "B01001_045E"]].sum(axis = 1)
B01001_Age["70_74_YEARS"] = B01001_Age.loc[:, ["B01001_022E","B01001_046E"]].sum(axis = 1)
B01001_Age["75_79_YEARS"] = B01001_Age.loc[:, ["B01001_023E","B01001_047E"]].sum(axis = 1)
B01001_Age["80_84_YEARS"] = B01001_Age.loc[:, ["B01001_024E","B01001_048E"]].sum(axis = 1)
B01001_Age["85+_YEARS"]   = B01001_Age.loc[:, ["B01001_025E","B01001_049E"]].sum(axis = 1)


print(B01001_Age.to_string())

# Construct the base table that is formated for the database
community_series = pd.Series(communities)
Database_df = pd.DataFrame(np.nan, index = range(len(communities)), columns = Database_df_Headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", "...")
Database_df.insert(0, "TIME_TYPE", "Annual")
Database_df.insert(0, "COMMUNITY", communities)
Database_df.insert(0, "STATE", "MA")
#get the community column in the database df
check = Database_df['COMMUNITY'][0:65]

# print(Database_df.to_string())


