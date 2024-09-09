import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc

# year = int(input("Enter the year of data you want to pull: "))
year = 2009
year2 = year - 4
print(f"{year2} - {year}")
# https://api.census.gov/data/2022/acs/acs5?get=NAME,B01001_001E&for=county:*&in=state:*
# https://www.reddit.com/r/learnpython/comments/jo9825/learning_to_use_the_censusgov_api_i_have_a_lot_of/   #useful code for constructing string
B01001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B01001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"   #THIS  IS BIS
B09001_URL = "https://api.census.gov/data/"+str(year)+"/acs/acs5?get=group(B09001)&for=county%20subdivision:*&in=state:25%20county:011,013,015"   #THIS  IS BIS

# B01001_URL = "https://api.census.gov/data/2022/acs/acs5?get=group(B01001)&for=tract:*&in=state:25%20county:011,013,015"
# B09001_URL = "https://api.census.gov/data/2022/acs/acs5?get=group(B09001)&for=tract:*&in=state:25%20county:011,013,015"


Census_Row_Specifiers = ['B09001_003','B09001_004','B09001_005','B09001_006','B09001_007','B09001_008','B09001_009',
                   'B01001_031','B01001_007','B01001_008','B01001_009','B01001_010','B01001_032','B01001_033',
                   'B01001_034','B01001_011','B01001_035','B01001_012','B01001_036','B01001_013','B01001_037',
                   'B01001_014','B01001_038','B01001_015','B01001_039','B01001_016','B01001_040','B01001_017',
                   'B01001_041','B01001_018','B01001_019','B01001_042','B01001_043','B01001_020','B01001_021',
                   'B01001_044','B01001_045','B01001_022','B01001_046','B01001_023','B01001_047','B01001_024',
                   'B01001_048','B01001_025','B01001_049']

subdivisions  = ["02095","05560","09595","12505","14885","15200","16670","21780","25730","27060","29475",
                 "29650","35180","35285","42040","42285","45490","47835","51265","58335","61135","61905",
                 "68400","73265","74525","79110","00840","06085","08470","13485","13660","19645","26675",
                 "28075","30665","30840","36300","37175","42145","42530","52144","58650","65825","67000",
                 "70045","72390","76030","77890","79740","01370","04825","13590","16040","19370","26290",
                 "26535","27690","29265","31785","40990","46330","52560","54030","62745","64145","72880",
                 "76380","79915","82175"]

ordered_communities = ["Ashfield", "Bernardston", "Buckland", "Charlemont", "Colrain", "Conway", "Deerfield", "Erving",
                       "Gill", "Greenfield", "Hawley", "Heath", "Leverett", "Leyden", "Monroe", "Montague",
                       "New Salem", "Northfield", "Orange", "Rowe", "Shelburne", "Shutesbury", "Sunderland",
                       "Warwick", "Wendell", "Whately", "Agawam", "Blandford", "Brimfield", "Chester",
                       "Chicopee", "East Longmeadow", "Granville", "Hampden", "Holland", "Holyoke", "Longmeadow",
                       "Ludlow", "Monson", "Montgomery", "Palmer", "Russell", "Southwick", "Springfield",
                       "Tolland", "Wales", "Westfield", "West Springfield", "Wilbraham", "Amherst",
                       "Belchertown", "Chesterfield", "Cummington", "Easthampton", "Goshen", "Granby", "Hadley",
                       "Hatfield", "Huntington", "Middlefield", "Northampton", "Pelham", "Plainfield",
                       "Southampton", "South Hadley", "Ware", "Westhampton", "Williamsburg", "Worthington"]

communities = ['Agawam' ,'Amherst','Ashfield','Belchertown','Bernardston','Blandford','Brimfield','Buckland',
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

B09001_Headers = ["UNDER_3_YEARS","3_4_YEARS","5_YEARS","6_8_YEARS","9_11_YEARS","12_14_YEARS", "15_17_YEARS"]


api_key = "49099c23404527970655eaa39f3d39c606843dfc"
B01001_request = requests.get(B01001_URL)
B09001_request = requests.get(B09001_URL)

B01001_data = B01001_request.json()
B01001_Age = pd.DataFrame(B01001_data[1:], columns = B01001_data[0], )
# Sort by county subdivision
B01001_Age.sort_values('GEO_ID', ascending = 0)

B09001_data = B09001_request.json()
B09001_Age = pd.DataFrame(B09001_data[1:], columns = B09001_data[0], )
# Sort by county subdivision
B09001_Age.sort_values('GEO_ID', ascending = 0)


#Preprocessing, zip two columns from imported tables together and check that they both match and have membership in the communities list
# print(B01001_Age["NAME"])

# https://stackoverflow.com/questions/44602139/pandas-convert-all-column-from-string-to-number-except-two
# The above link will show how the line below converts all but set columns to numeric for calculations
# df2 = pd.concat([pd.DataFrame([pd.to_numeric(df[e],errors='coerce') for e in df.columns if e not in ['Name','Job']]).T, df[['Name','Job']]],axis=1)

# Change all but the descriptive columns to numeric for calculations
B01001_Age = pd.concat([pd.DataFrame([pd.to_numeric(B01001_Age[e], errors = 'coerce')
                        for e in B01001_Age.columns if e not in
                        ['GEO_ID','NAME','state','county','county subdivision']]).T,
                        B01001_Age[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

B09001_Age = pd.concat([pd.DataFrame([pd.to_numeric(B09001_Age[e], errors = 'coerce')
                        for e in B09001_Age.columns if e not in
                        ['GEO_ID','NAME','state','county','county subdivision']]).T,
                        B09001_Age[['GEO_ID','NAME','state','county','county subdivision']]], axis = 1)

#Instantiate new columns in the table
for i in B01001_Headers:
    B01001_Age.insert(0, column= i, value= "NAN")

for i in B09001_Headers:
    B09001_Age.insert(0, column= i, value= "NAN")


# Check to make sure towns are aligned and data doesn't get mismatched, make this more robust if you feel the need
truth_table = []
for i, j in zip(B01001_Age["county subdivision"], B09001_Age["county subdivision"]):
    if i == j:
        truth_table.append(0)
        print(f"B01001: {i}\tB09001: {j} T")
    else:
        print(False)
        print(f"B01001: {i}\tB09001{j}\t FALSE")
        truth_table.append(1)

# print(truth_table)





# Calculate each column's values for table B01001
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


# Calculate each column's values for table B09001


B09001_Age["UNDER_3_YEARS"] =  B09001_Age.loc[:, ["B09001_003E"]]#.sum(axis = 1)
B09001_Age["3_4_YEARS"] =  B09001_Age.loc[:, ["B09001_004E"]].sum(axis = 1)
B09001_Age["5_YEARS"] =  B09001_Age.loc[:, ["B09001_005E"]].sum(axis = 1)
B09001_Age["6_8_YEARS"] =  B09001_Age.loc[:, ["B09001_006E"]].sum(axis = 1)
B09001_Age["9_11_YEARS"] =  B09001_Age.loc[:, ["B09001_007E"]].sum(axis = 1)
B09001_Age["12_14_YEARS"] =  B09001_Age.loc[:, ["B09001_008E"]].sum(axis = 1)
B09001_Age["15_17_YEARS"] =  B09001_Age.loc[:, ["B09001_009E"]].sum(axis = 1)

print("\nTable: B01001")
print(B01001_Age.to_string())

print("\nTable: B09001")
print(B09001_Age.to_string())

# Construct the base table that is formated for the database
community_series = pd.Series(ordered_communities)
Database_df = pd.DataFrame(np.nan, index = range(len(ordered_communities)), columns = Database_df_Headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{year2}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", B01001_Age["NAME"])
Database_df.insert(0, "STATE", "MA")



#Insert data from B01001 Table
Database_df["18_19_YEARS"] = B01001_Age["18_19_YEARS"]
Database_df["20_24_YEARS"] = B01001_Age["20_24_YEARS"]
Database_df["25_29_YEARS"] = B01001_Age["25_29_YEARS"]
Database_df["30_34_YEARS"] = B01001_Age["30_34_YEARS"]
Database_df["35_39_YEARS"] = B01001_Age["35_39_YEARS"]
Database_df["40_44_YEARS"] = B01001_Age["40_44_YEARS"]
Database_df["45_49_YEARS"] = B01001_Age["45_49_YEARS"]
Database_df["50_54_YEARS"] = B01001_Age["50_54_YEARS"]
Database_df["55_59_YEARS"] = B01001_Age["55_59_YEARS"]
Database_df["60_64_YEARS"] = B01001_Age["60_64_YEARS"]
Database_df["65_69_YEARS"] = B01001_Age["65_69_YEARS"]
Database_df["70_74_YEARS"] = B01001_Age["70_74_YEARS"]
Database_df["75_79_YEARS"] = B01001_Age["75_79_YEARS"]
Database_df["80_84_YEARS"] = B01001_Age["80_84_YEARS"]
Database_df["85+_YEARS"]   = B01001_Age["85+_YEARS"]

# Insert data from B09001 Table
Database_df["UNDER_3_YEARS"] = B09001_Age ["UNDER_3_YEARS"]
Database_df["3_4_YEARS"] = B09001_Age["3_4_YEARS"]
Database_df["5_YEARS"] =  B09001_Age["5_YEARS"]
Database_df["6_8_YEARS"] = B09001_Age["6_8_YEARS"]
Database_df["9_11_YEARS"] = B09001_Age["9_11_YEARS"]
Database_df["12_14_YEARS"] = B09001_Age["12_14_YEARS"]
Database_df["15_17_YEARS"] = B09001_Age["15_17_YEARS"]

#Calculate data from other columns in final table
Database_df["CEN_POP_O4"] = Database_df.loc[:, ["5_YEARS","6_8_YEARS", "9_11_YEARS", "12_14_YEARS", "15_17_YEARS",
                                                "18_19_YEARS", "20_24_YEARS", "25_29_YEARS", "30_34_YEARS",
                                                "35_39_YEARS", "40_44_YEARS", "45_49_YEARS", "50_54_YEARS",
                                                "55_59_YEARS", "60_64_YEARS", "65_69_YEARS", "70_74_YEARS",
                                                "75_79_YEARS", "80_84_YEARS", "85+_YEARS"]].sum(axis = 1)

Database_df["CEN_POP_U18"] = Database_df.loc[:, ["UNDER_3_YEARS","3_4_YEARS","5_YEARS","6_8_YEARS", "9_11_YEARS",
                                                 "12_14_YEARS", "15_17_YEARS"]].sum(axis = 1)

Database_df["CEN_POP_O64"] = Database_df.loc[:, ["65_69_YEARS", "70_74_YEARS", "75_79_YEARS", "80_84_YEARS",
                                                 "85+_YEARS"]].sum(axis = 1)

Database_df["CEN_POP_O24"] = Database_df.loc[:, ["25_29_YEARS", "30_34_YEARS","35_39_YEARS", "40_44_YEARS",
                                                 "45_49_YEARS", "50_54_YEARS","55_59_YEARS", "60_64_YEARS",
                                                 "65_69_YEARS", "70_74_YEARS","75_79_YEARS", "80_84_YEARS",
                                                 "85+_YEARS"]].sum(axis = 1)

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

print("\nDatabase_df")
print(Database_df.to_string())

if sum(truth_table) == 0:
    print("\n\nYou may proceed with downloading data")
else:
    print("\n\nSTOP DATA QUALITY IS POOR, DATA MISALIGNMENT!!!!!")
#  print(Database_df.to_string())



# 88888888ba   8b           d8  88888888ba     ,ad8888ba,       88888888ba,
# 88      "8b  `8b         d8'  88      "8b   d8"'    `"8b      88      `"8b                 ,d
# 88      ,8P   `8b       d8'   88      ,8P  d8'                88        `8b                88
# 88aaaaaa8P'    `8b     d8'    88aaaaaa8P'  88                 88         88  ,adPPYYba,  MM88MMM  ,adPPYYba,
# 88""""""'       `8b   d8'     88""""""'    88                 88         88  ""     `Y8    88     ""     `Y8
# 88               `8b d8'      88           Y8,                88         8P  ,adPPPPP88    88     ,adPPPPP88
# 88                `888'       88            Y8a.    .a8P      88      .a8P   88,    ,88    88,    88,    ,88
# 88                 `8'        88             `"Y8888Y"'       88888888Y"'    `"8bbdP"Y8    "Y888  `"8bbdP"Y8