import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re
import pyodbc


'''
This script is designed to pull data from the very large Census_Data table. This table has a large number of tables
that it is pulling from therefore we need to list out every table, the year, and every row for those tables. 

'''

api_key = "&key=49099c23404527970655eaa39f3d39c606843dfc"

year = 2022
timevalue = year-4

# List out all the tables being accessed in the URL
URL_tables = ['B02001','B03001','B05002','B05007','B06009','B08006','B09005','B15003','B16004','B17024','B17025',
              'B17026','B19001','B19013','B19053','B19055','B19057','B19059','B19083','B19113','B05009']

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
                            'Deerfield','East Longmeadow','Easthampton','Erving','Franklin County','Gill','Goshen',
                            'Granby','Granville','Greenfield','Hadley','Hampden','Hampden County','Hampshire County',
                            'Hatfield','Hawley','Heath','Holland','Holyoke','Huntington','Leverett','Leyden',
                            'Longmeadow','Ludlow','Massachusetts','Middlefield','Monroe','Monson','Montague',
                            'Montgomery','New Salem','Northampton','Northfield','Orange','Palmer','Pelham',
                            'Pioneer Valley','Plainfield','PVPC Region','Rowe','Russell','Shelburne','Shutesbury',
                            'South Hadley','Southampton','Southwick','Springfield','Sunderland','Tolland','Wales',
                            'Ware','Warwick','Wendell','West Springfield','Westfield','Westhampton','Whately',
                            'Wilbraham','Williamsburg','Worthington']

# this table is missing 2 values, base table and api access have 65 values, 67 headers
address_book_df = pd.DataFrame({"API Access":[1,0,0,0,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,0,0,0,0,1,1,1,1,1,1,1,1,1,
                                              0,0,1,1,1,1,1,1,1,1,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,],
                                "Base Table":['B02001','NA','NA','NA','B02001','B02001','B02001','B02001','B02001',
                                              'B02001','B03001','B19001','B09005','NA','B05009','B16004','B16004',
                                              'B16004','B16004','B05002','B05007','NA','NA','NA','NA','B08006','B15003',
                                              'B06009','B19013','B19053','B19055','B19057','B19059','B19113','NA','NA',
                                              'B19083','B17025','B17025','B17025','B17024','B17024','B17026','B17025',
                                              'NA','NA','NA','B19001','NA','B19001','NA','B19001','NA','B19001','NA',
                                              'B19001','NA','B19001','NA','B19001','NA','B19001','NA','B19001','NA',
                                              'B19001','NA'],
                                }, index= dataframe_df_headers)



API_map = [("CEN_POP","B02001","B02001_001"),
           ("CEN_WORKERS","None"),                                                              #No data available via API
           ("CEN_URBANPOP","None"),                                                             #No data available via API
           ("CEN_RURALPOP","None"),                                                             #No data available via API
           ("CEN_WHITE","B02001","B02001_002"),
           ("CEN_BLACK","B02001","B02001_003"),
           ("CEN_ASIAN","B02001","B02001_005"),
           ("CEN_OTHRACE","B02001","B02001_007"),
           ("CEN_TWO_MORE_NOT_HISP","B02001","B02001_008"),
           ("MINORITY_PERCENT","B02001","B02001_001","B02001_002"),
           ("CEN_HISPANIC","B03001","B03001_001"),
           ("CEN_HOUSEHOLDS","B19001","B19001_001"),
           ("CEN_MARRHOU","B09005","B09005_001"),
           ("CEN_SINGPARHOU","None"),                                                        #No data available via API
           ("CEN_SINGPAR_CHILD","B05009","B05009_013", "B05009_031"),
           ("CEN_ENGLISH","B16004","B16004_003","B16004_025","B16004_053"),
           ("CEN_SPANISH","B16004","B16004_004","B16004_026","B16004_048"),
           ("CEN_ASIANLANG","B16004","B16004_014","B16004_036","B16004_058"),
           ("CEN_OTHLANG","B16004","B16004_019","B16004_041","B16004_063"),
           ("CEN_FORBORN","B05002","B05002_013"),
           ("CEN_RECENTFORBORN","B05007","B05007_002"),
           ("CEN_NET_DOM_MIGR","None"),                                                     #No data available via API
           ("CEN_SAMEHOUSE","None"),                                                        #No data available via API
           ("CEN_SAMECOUNTY","None"),                                                       #No data available via API
           ("CEN_WORKLIVE","None"),                                                         #No data available via API
           ("CEN_PUBLICTRANS","B08006","B08006_008"),
           ("CEN_NOHSDIP","B15003","B15003_002"),
           ("CEN_BADEG","B06009","B06009_005","B06009_006"),
           ("CEN_MEDHHINC","B19013","B19013_001"),
           ("CEN_SELFEMPINC","B19053","B19053_002"),
           ("CEN_SOCSECINC","B19055","B19055_002"),
           ("CEN_PAINC","B19057","B19057_001"),
           ("CEN_RETINC","B19059","B19059_001"),
           ("CEN_MEDFAMINC","B19113","B19113_001"),
           ("CEN_FAMECON_SELF","None"),                                                     #No data available via API
           ("CEN_PERCAPINC","None"),                                                        #No data available via API
           ("CEN_INCOME_INEQ","B19083","B19083_001"),
           ("CEN_POVTOTALPOP","B17025","B17025_001"),
           ("CEN_POOR","B17025","B17025_002"),
           ("CEN_POVRATE","B17025","B17025_001","B17025_002"),
           ("CENPOV200","None"),                                                             #Put a pin in this one
           ("CEN_CHILD_POV","B17025","B17025_003"),
           ("CEN_FAM_POV","B17026","B17026_002","B17026_003","B17026_004"),
           ("CEN_POV_FOR_BORN","B17025","B17025_006"),
           ("CEN_POV_FOR_BORN_NAT","None"),                                                 #No data available via API
           ("CEN_POV_FOR_BORN_NON_CIT","None"),                                             #No data available via API
           ("MINORITY_POV_CONC","None"),                                                    #No data available via API
           ("CEN_HHINC_0-9999","B19001","B19001_002"),
           ("CEN_PERC_HHINC_0-9999","None"),                                                #Table calc
           ("CEN_HHINC_10000-14999","B19001","B19001_003"),
           ("CEN_PERC_HHINC_10000-14999","None"),                                           #Table calc
           ("CEN_HHINC_15000-24999","B19001","B19001_004","B19001_005"),
           ("CEN_PERC_HHINC_15000-24999","None"),                                           #Table calc
           ("CEN_HHINC_25000-34999","B19001","B19001_006","B19001_007"),
           ("CEN_PERC_HHINC_25000-34999","None"),                                           #Table calc
           ("CEN_HHINC_35000-49999","B19001","B19001_008","B19001_009","B19001_010"),
           ("CEN_PERC_HHINC_35000-49999","None"),                                           #Table calc
           ("CEN_HHINC_50000-74999","B19001","B19001_011","B19001_012"),
           ("CEN_PERC_HHINC_50000-74999","None"),                                           #Table calc
           ("CEN_HHINC_75000-99999","B19001","B19001_013"),
           ("CEN_PERC_HHINC_75000-99999","None"),                                           #Table calc
           ("CEN_HHINC_100000-149999","B19001","B19001_014","B19001_015"),
           ("CEN_PERC_HHINC_100000-149999","None"),                                         #Table calc
           ("CEN_HHINC_150000-199999","B19001","B19001_016"),
           ("CEN_PERC_HHINC_150000-199999","None"),                                         #Table calc
           ("CEN_HHINC_200000_PLUS","B19001","B19001_017"),
           ("CEN_PERC_HHINC_200000_PLUS","None")]                                           #Table calc

# instantiate the database dataframe, check the column header order and names as well as the communites
Database_df = pd.DataFrame(np.nan, index = range(len(dataframe_df_communities)), columns = dataframe_df_headers)
Database_df.insert(0, "YEAR", year)
Database_df.insert(0, "TIME_VALUE", f"{timevalue}-{year}")
Database_df.insert(0, "TIME_TYPE", "5-Year-Estimates")
Database_df.insert(0, "COMMUNITY", dataframe_df_communities)
Database_df.insert(0, "STATE", "MA")



for header, table in zip(dataframe_df_headers, URL_tables):
    URL = "https://api.census.gov/data/" + str(year) + "/acs/acs5?get=group("+table+")&for=county%20subdivision:*&in=state:25%20county:011,013,015" + api_key


print(Database_df.to_string())
# for row in API_map:
#     print(row[1])

print(address_book_df.to_string())

















