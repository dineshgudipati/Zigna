# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 17:24:53 2022

@author: user
"""


import numpy as np
import pandas as pd

df=pd.read_excel(r"D:/Zigna AI Corp/Zigna AI Corp - Hospital Application_2022-03-09/DRG refernce version 5/Diagnosis_codes_Reference_tables.xlsx")
df = df.dropna(axis=1, how='all')
df1 = df.apply(lambda x: pd.Series(x.dropna().values))
df1.columns =['DX_codes', 'MDC', 'DRG', 'DX_codes', 'MDC', 'DRG','DX_codes', 'MDC', 'DRG']

uniq = df1.columns.unique()
df2 = pd.concat([df1[c].melt()['value'] for c in uniq], axis=1, keys=uniq)




# df2 = df2.drop(["MDC"], axis=1, inplace=True)


# df.fillna(method='ffill', inplace=True)

df2 = df2.apply(lambda x: pd.Series(x.dropna().values))

dropind = []
exceptions = []
for index,j in df2['DRG'].iteritems():
    if len(j) >7 :
        exceptions.append(j)
        dropind.append(index)
    else:
        None

exception_dx_codes = df2.iloc[dropind]
    
df2.drop(dropind, axis=0, inplace=True)


df2["DRG1"] = df2["DRG"].str.contains("-")
df2


df2['DRG1']= np.where((df2['DRG1']==True), df2['DRG'], df2['DRG1'])
df2

df2['DRG1']= np.where((df2['DRG1']==False), '0-0', df2['DRG1'])
df2
df2  = df2.drop(['MDC'], axis =1)

s = df2.pop('DRG1').str.findall('\d+')


a = [(i, x) for i, (a, b) in s.items() for x in range(int(a), int(b) + 1)]


s = pd.DataFrame(a).set_index(0)[1].rename('DRG1')

df3= df2.join(s)
df3
df3.drop("DRG", axis =1, inplace = True)
df3 = df3.rename(columns={'DRG1': 'DRG'})
df3.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Trail copies\DX_codes_refernce.xlsx",index=False)
exception_dx_codes.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Trail copies\DX_codes_exceptions_refernce.xlsx",index=False)



