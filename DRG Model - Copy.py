# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 17:28:57 2022

@author: user
"""

import numpy as np
import pandas as pd
import os
import glob
import re
from time import time
from statistics import *

start = time()    
claim = pd.read_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Claim.xlsx", header = None, dtype=str)
px_codes = pd.read_csv(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model 1\procedure_tables.csv", dtype=str)
mcc_codes = pd.read_csv(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model 1\mcc_reference_tables.csv")
cc_codes = pd.read_csv(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model 1\cc_reference_tables.csv")
drgs = pd.read_csv(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model 1\mcc_cc_non_mcc_cc.csv" ,converters={'drg':str})
dx_codes = pd.read_csv(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Tables for Data model 1\diagnosis_table.csv", dtype=str)



claim = claim.T
a = len(claim.columns)
# claim = claim.astype(str)

required_drg = []    


if a == 1:
    claim.columns = ["Px_code"]
    claim1 = claim.iloc[0,0]
elif a == 2:
    claim.columns = ["Px_code", "dx_code"]
    claim1 = claim.iloc[0,0]
    claim3 = claim.iloc[0,1]
    
else:
    claim.columns = ["Px_code", "dx_code", "condition"]
    claim1 = claim.iloc[0,0]
    claim2 = claim.iloc[0,2]
    claim3 = claim.iloc[0,1]


# if a == 1:
#     claim1 = claim.iloc[0,0]
# elif a == 2:
#     claim1 = claim.iloc[0,0]
#     claim3 = claim.iloc[0,1]
    
# else:
    
#     claim1 = claim.iloc[0,0]
#     claim2 = claim.iloc[0,2]
#     claim3 = claim.iloc[0,1]




condition_list = []

# px_codes = px_codes.astype(str)

if claim1 != "nan" :
    drglist = []
    mdclist = []
   
    for index, j in px_codes['CODE'].iteritems():
        if str(j) == claim1:
            drglist.append(px_codes['DRG'][index])
            mdclist.append(px_codes['MDC'][index])
        else:
            None
    
    drglist = pd.DataFrame(drglist)
    drglist.columns = ['DRG']
    
    mdclist = pd.DataFrame(mdclist)
    mdclist.columns = ['mdc']
    
    Px_assinged_drg =  pd.concat([drglist,mdclist],axis=1)






# dx_codes = dx_codes.astype(str)
dx_drg = []
dx_mdc =[]


for index, h in dx_codes['I10 Dx'].iteritems():
    if str(h) == claim3:
      
        dx_drg.append(dx_codes['DRG'][index])
        dx_mdc.append(dx_codes['MDC'][index])
    else:
        None
        



dx_mdc = pd.DataFrame(dx_mdc)
dx_mdc.columns = ['mdc']



dx_drg = pd.DataFrame(dx_drg)
dx_drg.columns = ['drg']

dx_df = pd.concat([dx_drg,dx_mdc],axis=1)



if claim1 != "nan":
    assinged_drg = []
    


    
    
    for index, x in dx_df["mdc"].iteritems():
        for index, p in Px_assinged_drg['mdc'].iteritems():
           
            if str(x) == str(p):
                assinged_drg.append(Px_assinged_drg['DRG'][index]) 
            else:
                None
    drg_px_code = []
    [drg_px_code.append(x) for x in assinged_drg if x not in drg_px_code]
else:
    None
    
    
    
# if claim1 == "nan":
#     assinged_drg = []
    


try:
    for index, k in mcc_codes['code'].iteritems():
        if str(k) ==  claim2:
            condition_list.append("with_mcc")
except:
    pass

try:
    for index, k in cc_codes['code'].iteritems():
        if str(k) ==  claim2:
            condition_list.append("with_cc")
except:
    pass



# try:
#     for index, k in cc_codes['Code'].iteritems():
#         if str(k) ==  claim2:
#             condition_list.append("without_mcc")
# except:
#     pass


if len(condition_list) != 0:
    
    condition = condition_list[0]
else:
    None



try:
    
    if condition == "with_mcc":
        for index, b in drgs['category'].iteritems():
            if condition == str(b):
                required_drg.append(drgs["drg"][index])
except:
    pass
            

try:
    
    if condition == "with_cc":
        for index, b in drgs['category'].iteritems():
            if condition == str(b):
                required_drg.append(drgs["drg"][index])
except:
    pass
            

# try:
    
#     if condition == "without_mcc":
#         for index, b in drgs['category'].iteritems():
#             if condition == str(b):
#                 required_drg.append(drgs["drg"][index])
# except:
#     pass

            
  
try:
             
    if len(required_drg) == 0 :
        condition = "without_cc_mcc"
        if condition == "without_cc_mcc":
            for index, b in drgs['category'].iteritems():
                if condition == str(b):
                    
                    required_drg.append(drgs["drg"][index])
except:
    pass


    


# try:   
#     if len(mcc_drg) != 0:
#         required_drg = mcc_drg
# except:
#     pass
# try:   
#     if len(cc_drg) != 0:
#         required_drg = cc_drg
# except:
#     pass
# try:   
#     if len(without_mcc_drg) != 0:
#         required_drg = without_mcc_drg
# except:
#     pass

# try:   
#     if len(with_out_cc_mcc_drg) != 0:
#         required_drg = with_out_cc_mcc_drg
# except:
#     pass



if claim1 == "nan":

    final_drg = [value for value in dx_drg if value in required_drg]



if claim1 != "nan":
    final_drg = [value for value in drg_px_code if value in required_drg]
    

print(f'Time taken to run: {time() - start} seconds')
            
    
    
        
        
#     final_drg = []
#     for index, y in dx_df['drg'].iteritems():
#         for c in required_drg :
#            if str(y) == str(c):
#                final_drg.append(dx_df["drg"][index])
#            else:
#                None
# # L = [i for i in range (1, 1000) if i%3 == 0]



        
