# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 15:50:09 2022

@author: user
"""

import pandas as pd
from googlesearch import search
# import requests
from bs4 import BeautifulSoup
import re
import urllib
import numpy as np
import time
import wget
from datetime import datetime
from bs4 import beautifulsoap
start_time = datetime.now()




df = pd.read_excel(r"C:\Users\user\Documents\rework_download\Shoppable_rework.xlsx")





df["status_shoppable"] = np.nan
# df["status_standard"] = np.nan
df["extension_status"] = np.nan
# df["extension_status1"] = np.nan




links = df["Shoppables_2022"].tolist()

c =0
      

for k in links:
    # print(k[1])
   
    try:
        wget.download(str(k))
        df['status_shoppable'][c] = "Yes"


    except:None
  
    c += 1
    

d = 0
for y in links:
    for i in ['xls','xlsx', 'csv','json', 'pdf', 'zip', 'xlsb', 'xml', 'txt','xlsm']:
        if str(y).split('.')[-1] == i:
            print(True)
            df['extension_status'][d] = 'yes'
        else:
            None
    d +=1
    


df.to_csv(r"C:\Users\user\Documents\rework_download\Shoppable_rework_report.csv")
