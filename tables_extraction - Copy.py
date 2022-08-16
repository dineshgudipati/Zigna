# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 18:06:11 2022

@author: user
"""

import xlsxwriter
import os


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

i = "https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0388.html"





main_link = i.rsplit("/",1)[0]
reqs = urllib.request.urlopen(i,timeout=300)# 5 min
soup = BeautifulSoup(reqs, 'html.parser')


links = []
data = []

for i in soup.find_all('a',class_ = "cdref"):
    links.append(main_link+"/" +str(i.get('href')))
  

for k in links:
    print(k)
    reqs = urllib.request.urlopen(k,timeout=300)
    soup = BeautifulSoup(reqs, 'html.parser')
    
    table = soup.find_all("table")[2]
    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols])
   
wb = xlsxwriter.Workbook("MCC_Reference_tables.xlsx")
ws = wb.add_worksheet()
row = 0
col = 0
for line in data:
	for item in line:
		ws.write(row, col, item)
		col += 1
	row += 1
	col = 0
 
wb.close()

os.system("MCC_Reference_tables.xlsx")
