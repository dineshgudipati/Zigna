
import numpy as np
import pandas as pd

df=pd.read_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Procedure_codes_Reference_tables.xlsx")

df.columns =['PX_codes', 'MDC', 'DRG', 'desc']
df.drop(["MDC","desc"], axis=1, inplace=True)


df.fillna(method='ffill', inplace=True)


df["DRG1"] = df["DRG"].str.contains("-")
df


df['DRG1']= np.where((df['DRG1']==True), df['DRG'], df['DRG1'])
df

df['DRG1']= np.where((df['DRG1']==False), '0-0', df['DRG1'])
df


s = df.pop('DRG1').str.findall('\d+')


a = [(i, x) for i, (a, b) in s.items() for x in range(int(a), int(b) + 1)]


s = pd.DataFrame(a).set_index(0)[1].rename('DRG1')

df1= df.join(s)
df1
df1.drop("DRG", axis =1, inplace = True)
df1 = df1.rename(columns={'DRG1': 'DRG'})
df1.to_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\DRG refernce version 5\Trail copies\PX_code_reference_tables.xlsx",index=False)





