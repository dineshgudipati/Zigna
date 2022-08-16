# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 13:38:05 2022

@author: user
"""

import pandas as pd
import numpy as np 
import re as re

import xlsxwriter
import os

import pandas as pd
from googlesearch import search
# import requests
from bs4 import BeautifulSoup
import re
import urllib
import numpy as np

################### Text files extraction ####################
df = pd.read_fwf(r'D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_B.txt', header = None, delimiter = ' ')
  
new_header = df.iloc[0]
df = df[1:] #take the data less the header row
df.columns = new_header 


df1 = pd.read_fwf(r'D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_A.txt', header = None, delimiter = ' ')
new_header1 = df1.iloc[0]
df1 = df1[1:] #take the data less the header row
df1.columns = new_header1
df1['description'] = df1[df1.columns[3:]].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1)
df1.drop("Description", axis=1, inplace=True)
df1 = df1.loc[:, df1.columns.notna()]


df3 = pd.read_fwf(r'D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_D_E.txt', header = None, delimiter = ' ')
new_header2 = df3.iloc[0]
df3 = df3[1:] #take the data less the header row
df3.columns = new_header2
df3['surgical_category'] = df3[df3.columns[3:]].apply(lambda x: ','.join(x.dropna().astype(str)),axis=1)
    

df3.drop("SURGICAL CATEGORY", axis=1, inplace=True)
df3 = df3.loc[:, df3.columns.notna()]

Appendix_A = df1
Appendix_B = df
Appendix_E = df3

Appendix_A.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_a_ms_drg_reference.xlsx",index=False)

Appendix_B.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_b.xlsx",index=False)
Appendix_E.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_e.xlsx",index=False)

#################setting the text files as per requirement for Data Model ##################


Appendix_B = Appendix_B[["I10 Dx", 'DRG(s)']]
Appendix_B.fillna(method='ffill', inplace=True)

Appendix_B["DRG1"] = Appendix_B["DRG(s)"].str.contains("-")
Appendix_B


Appendix_B['DRG1']= np.where((Appendix_B['DRG1']==True), Appendix_B['DRG(s)'], Appendix_B['DRG1'])
Appendix_B

Appendix_B['DRG1']= np.where((Appendix_B['DRG1']==False), '0-0', Appendix_B['DRG1'])
Appendix_B

s = Appendix_B.pop('DRG1').str.findall('\d+')

a = [(i, x) for i, (a, b) in s.items() for x in range(int(a), int(b) + 1)]
s = pd.DataFrame(a).set_index(0)[1].rename('DRG1')


Appendix_B = Appendix_B.apply(lambda x: pd.Series(x.dropna().values))

dx_codes= Appendix_B.join(s)
dx_codes.drop("DRG(s)", axis =1, inplace = True)
dx_codes = dx_codes.rename(columns={'DRG1': 'DRG'})


Appendix_E = Appendix_E[["CODE", "MS-DRG"]]

Appendix_E.fillna(method='ffill', inplace=True)


Appendix_E["MS-DRG1"] = Appendix_E["MS-DRG"].str.contains("-")

Appendix_E['MS-DRG1']= np.where((Appendix_E['MS-DRG1']==True), Appendix_E['MS-DRG'], Appendix_E['MS-DRG1'])

Appendix_E['MS-DRG1']= np.where((Appendix_E['MS-DRG1']==False), '0-0', Appendix_E['MS-DRG1'])

k = Appendix_E.pop('MS-DRG1').str.findall('\d+')


b = [(i, x) for i, (a, b) in k.items() for x in range(int(a), int(b) + 1)]

k = pd.DataFrame(a).set_index(0)[1].rename('DRG1')

Px_codes= Appendix_E.join(s)
Px_codes
Px_codes.drop("MS-DRG", axis =1, inplace = True)
Px_codes = Px_codes.rename(columns={'DRG1': 'DRG'})


dx_codes.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model\diagnosis_table.xlsx",index=False)
Px_codes.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model\procedure_tables.xlsx",index=False)


##### web scrapping MCC and CC tables  ##################


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
        
        
wb = xlsxwriter.Workbook("mcc_reference_tables.xlsx")
ws = wb.add_worksheet()
row = 1
col = 0

for line in data:
	for item in line:
		ws.write(row, col, item)
		col += 1
	row += 1
	col = 0
 
wb.close()

j = "https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0389.html"

main_link = j.rsplit("/",1)[0]
reqs = urllib.request.urlopen(j,timeout=300)# 5 min
soup = BeautifulSoup(reqs, 'html.parser')


links1 = []
data1 = []

for j in soup.find_all('a',class_ = "cdref"):
    links1.append(main_link+"/" +str(j.get('href')))
  

for x in links:
    print(x)
    reqs = urllib.request.urlopen(x,timeout=300)
    soup = BeautifulSoup(reqs, 'html.parser')
    
    table = soup.find_all("table")[2]
    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data1.append([ele for ele in cols])

wb = xlsxwriter.Workbook("cc_reference_tables.xlsx")
ws = wb.add_worksheet()
row = 1
col = 0
for line in data1:
	for item in line:
		ws.write(row, col, item)
		col += 1
	row += 1
	col = 0
 
wb.close()

######   list of mcc, cc, non mcc and cc drg tables #############

ms_drg = pd.read_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_a_ms_drg_reference.xlsx")

ms_drg = ms_drg[["DRG", "description"]]


with_mcc =[]
with_cc = []
without_mcc = []
without_cc_mcc = []

ind = []
not_specified = []
for index, j in ms_drg["description"].iteritems():
    print(j)
    try:
        sheet = [ele for ele in j if('WITH MCC' in j)][0]
        with_mcc.append(ms_drg['DRG'][index])
        ind.append(index)
    except:
        None
        
for index, a in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in a if('WITH CC' in a)][0]
        with_cc.append(ms_drg['DRG'][index])
        ind.append(index)

    except:
        None

for index, b in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in b if('WITHOUT CC/MCC' in b)][0]
        without_cc_mcc.append(ms_drg['DRG'][index])
        ind.append(index)

    except:
        None        


for index, c in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in c if('WITHOUT MCC' in c)][0]
        without_mcc.append(ms_drg['DRG'][index])
        ind.append(index)

    except:
        None

a = ms_drg["DRG"].to_list()

y = set(ind) - set(a)
not_specified = list(y)

final_list = [with_mcc,with_cc,without_cc_mcc,without_mcc,not_specified ]

final = pd.DataFrame(final_list)
final = final.T
final.columns = ["with_mcc ", 'with_cc','without_cc_mcc','without_mcc','not_specified']
final

final.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model", index = False)
    
    





