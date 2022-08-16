# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 18:12:57 2022

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


ms_drg = pd.read_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Text files from CMS\appendix_a_ms_drg_reference.xlsx")
ms_drg = ms_drg[["DRG", "description"]]


with_mcc =[]
with_cc = []
without_mcc = []
with_cc_or_mcc = []
without_cc_mcc = []

ind = []
not_specified = []


for index, j in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in j if('WITH CC/MCC' in j)][0]
        with_cc_or_mcc.append(ms_drg['DRG'][index])
        # ind.append(index)
    except:
        None

with_cc_or_mcc_df = pd.DataFrame(with_cc_or_mcc)

with_cc_or_mcc_df = with_cc_or_mcc_df.assign(category='with_cc_or_mcc')
with_cc_or_mcc_df.columns = ["drg", "category"]
with_cc_or_mcc_df['drg'] = with_cc_or_mcc_df['drg'].astype(str)
with_cc_or_mcc_df['drg'] = with_cc_or_mcc_df['drg'].str.zfill(3)
for index, a in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in a if('WITH CC' in a)][0]
        with_cc.append(ms_drg['DRG'][index])
        # ind1.append(index)
    
    except:
        None
# withg_cc = [i for i in with_cc if i not in with_cc_or_mcc_df]
# with_cc = [i for i in with_cc if i not in with_cc_or_mcc_df]

# with_cc_df = pd.DataFrame(with_cc)
# with_cc_df = with_cc_df.assign(category='with_cc')
# with_cc_df.columns = ["drg", "category"]
# with_cc_df['drg'] = with_cc_df['drg'].astype(str)
# with_cc_df['drg'] = with_cc_df['drg'].str.zfill(3)
# with_cc = [i for i in with_cc if i not in with_cc_or_mcc_df]
with_cc = [i for i in with_cc if i not in with_cc_or_mcc]

with_cc_df = pd.DataFrame(with_cc)
with_cc_df = with_cc_df.assign(category='with_cc')
with_cc_df.columns = ["drg", "category"]
with_cc_df['drg'] = with_cc_df['drg'].astype(str)
with_cc_df['drg'] = with_cc_df['drg'].str.zfill(3)



# with_cc_df = [i for i in with_cc if i not in with_cc_or_mcc]


for index, j in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in j if('WITH MCC' in j)][0]
        with_mcc.append(ms_drg['DRG'][index])
        ind.append(index)
    except:
        None
with_mcc_df = pd.DataFrame(with_mcc)

with_mcc_df = with_mcc_df.assign(category='with_mcc')
with_mcc_df.columns = ["drg", "category"]
with_mcc_df['drg'] = with_mcc_df['drg'].astype(str)
with_mcc_df['drg'] = with_mcc_df['drg'].str.zfill(3)








for index, b in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in b if('WITHOUT CC/MCC' in b)][0]
        without_cc_mcc.append(ms_drg['DRG'][index])
        ind.append(index)
    
    except:
        None        

without_cc_mcc_df = pd.DataFrame(without_cc_mcc)
without_cc_mcc_df = without_cc_mcc_df.assign(category='without_cc_mcc')
without_cc_mcc_df.columns = ["drg", "category"]
without_cc_mcc_df['drg'] = without_cc_mcc_df['drg'].astype(str)
without_cc_mcc_df['drg'] = without_cc_mcc_df['drg'].str.zfill(3)



for index, c in ms_drg["description"].iteritems():
    
    try:
        sheet = [ele for ele in c if('WITHOUT MCC' in c)][0]
        without_mcc.append(ms_drg['DRG'][index])
        ind.append(index)
    
    except:
        None

without_mcc_df = pd.DataFrame(without_mcc)
without_mcc_df = without_mcc_df.assign(category='without_cc_mcc')
without_mcc_df.columns = ["drg", "category"]
without_mcc_df['drg'] = without_mcc_df['drg'].astype(str)
without_mcc_df['drg'] = without_mcc_df['drg'].str.zfill(3)




required_list = with_cc + with_cc_or_mcc + with_mcc + without_cc_mcc + without_mcc
a = ms_drg["DRG"].to_list()
y = set(a) - set(required_list)

not_specified = list(y)
not_specified_df = pd.DataFrame(not_specified)
not_specified_df = not_specified_df.assign(category='not_specified')
not_specified_df.columns = ["drg", "category"]
not_specified_df['drg'] = not_specified_df['drg'].astype(str)
not_specified_df['drg'] = not_specified_df['drg'].str.zfill(3)

final = pd.concat([with_mcc_df,with_cc_df,without_cc_mcc_df,with_cc_or_mcc_df,without_mcc_df,not_specified_df], axis=0)
final.reset_index()
