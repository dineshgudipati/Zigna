# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 15:02:18 2022

@author: user
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 18:06:11 2022

@author: user
"""

import xlsxwriter
import os

import pandas as pd
from googlesearch import search
# import requests
from bs4 import BeautifulSoup
import re
import urllib
import numpy as np
import time
from datetime import datetime
start_time = datetime.now()

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent','Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')]
urllib.request.install_opener(opener)

i = "https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0377.html"




data = []
# main_link = i.rsplit("/",1)[0]
reqs = urllib.request.urlopen(i,timeout=300)# 5 min
soup = BeautifulSoup(reqs, 'html.parser')



table = soup.find_all("table")[2]
rows = table.find("tr").text
c = 0
x = rows.split('\n')

df = pd.DataFrame({'col':x})

df.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\ms_drg_reference_tables.xlsx",index=False)

# wb = xlsxwriter.Workbook("ms_drg_reference_tables.xlsx")
# ws = wb.add_worksheet()
# row = 0
# col = 0
# for line in data:
# 	for item in line:
# 		ws.write(row, col, item)
# 		col += 1
# 	row += 1
# 	col = 0
 
# wb.close()

# os.system("ms_drg_reference_tables.xlsx")





# for row in rows:
#     if c == 0:
        
#         print(row)
#     cols = row.find_all('td')
#     cols = [ele.text.strip() for ele in cols]
#     data.append([ele for ele in cols])
 



# links = []
# data = []
# table = soup.find_all("table")[2]
# rows = table.find_all("tr")



# for i in soup.find_all('a',class_ = "cdref"):
#     links.append(main_link+"/" +str(i.get('href')))
  

# for k in links:
#     print(k)
#     reqs = urllib.request.urlopen(k,timeout=300)
#     soup = BeautifulSoup(reqs, 'html.parser')
    
#     table = soup.find_all("table")[2]
#     rows = table.find_all("tbody")

#     for row in rows:
#         cols = row.find_all('tbody')
#         cols = [ele.text.strip() for ele in cols]
# #         data.append([ele for ele in cols])

    
# wb = xlsxwriter.Workbook("drg1_Reference_tables.xlsx")
# ws = wb.add_worksheet()
# row = 0
# col = 0
# for line in data:
# 	for item in line:
# 		ws.write(row, col, item)
# 		col += 1
# 	row += 1
# 	col = 0
 
# wb.close()

# os.system("drg_Reference_tables_tables.xlsx")
