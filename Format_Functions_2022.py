#!/usr/bin/env python
# coding: utf-8

# In[1]:


import getpass
import os
import warnings
import glob
import numpy as np
import pandas as pd
from statistics import *
import re
import time
from datetime import datetime
# from fuzzywuzzy import fuzz
# from fuzzywuzzy import process
from difflib import SequenceMatcher


# In[2]:


payers_list_path = r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\Automation_task\DHC and payerlist\Renaming list (1).xlsx"


DF5=pd.read_excel(payers_list_path,sheet_name='2022_idvars')
DF6=pd.read_excel(payers_list_path,sheet_name='2022_Renaming')
# DF6['Hospital_ID'] = DF6['Hospital_ID'].astype(str).replace('\.0', '', regex=True)
# DF6['Hospital_ID'] = DF6['Hospital_ID'].replace("nan","")


# In[3]:


DF6


# In[4]:


DF8=pd.read_excel(payers_list_path,sheet_name='Dropping columns')
folder = pd.read_excel (r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\shoppable services data files\Research team downloaded\2022\Hospitals with shoppables Links Base file(Final).xlsx", sheet_name = "Raw file")
df7 = folder[["Hospital_Id","iloc"]]




DF6['Hospital_ID'] = DF6['Hospital_ID'].astype(str).replace('\.0', '', regex=True)


# In[5]:


df7


# In[6]:



def df_column_uniquify(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df


# In[7]:


############# wideFormat1018 #############
def wideFormat1018(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df2.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]

                        if CNames['id'].isin(['3295']).any():
                            try:
                                FINAL.drop(['Description'], axis = 1,inplace = True)
                            except:
                                pass
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass






        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1018_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[8]:


################# wideFormat1019 ###############
def wideFormat1019(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})

                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        DF_11=DF_FINAL.iloc[c:]
                        #DF_111 = DF_11
                        #print(DF_11)
                        list2=["Shoppable Services Effective January 1,2021 St. Catherine's  Rehabilitation Hospital",'Shoppable Service','CMG','Charge Code','Description',"Medicine and Surgery Services","Radiology Services","Laboratory and Pathology Services","Evaluation and Mangagement Services"]

                        DF_11["ROW"] = DF_11.iloc[:,0].isin(list2)
                        n =0
                        for index, row in DF_11['ROW'].iteritems():
                            if row == True:

                                DF_11['ROW'][index] = n
                            else:

                                DF_11['ROW'][index] = np.nan

                        DF_11['ROW'].fillna(method='ffill', inplace=True)

                        List_ids=[]
                        for i in DF_11['ROW']:
                            List_ids.append(i)

                            List_ids=list(set(List_ids))
                        list2 = pd.DataFrame()
                        for i in List_ids:
                            list2 = DF_11[DF_11['ROW']==i]
                            list2 = list2.drop(['ROW'],axis=1)
                            list2.columns = list2.iloc[0]
                            list2 = list2.iloc[1:]
                            list2 = list2.dropna(how='all',axis=1)
                            list2 = list2.dropna(how='all',axis=0)                    
                            list2 = list2.reset_index(drop=True)
                            list2['id']=k[1]
                            for i in list2.columns:
                                x = str(i).lower().strip()
                                list2=list2.rename(columns= {i:x})   
                            #print(DF_FINAL1.columns)
                            for i in list2.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(i) == str(r[0]):
                                        list2.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(list2)
                            FINAL = pd.concat(combined_final) 
    #                    Combined_data.append(FINAL)

                else:
                    None 



            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass
        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        #for r in DF6.itertuples(index=False):
            #FINAL.rename(columns={r[0]:r[1]}, inplace=True)
            #Search_List = list(DF8["Dropping columns"])
            #dropping columns
            #FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data = Combined_data.drop_duplicates()      
    Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1019_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[9]:


################ wideFormat1020 ##################
def wideFormat1020(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]

                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]

                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]



                        FINAL
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass
        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass


        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:
                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])





        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 

        FINAL['cpt_hcpcs'].fillna(method='ffill', inplace=True)
        FINAL['description'].fillna(method='ffill', inplace=True)
        try:
            FINAL=FINAL[~(FINAL["cpt_hcpcs"]=='Professional Services not provided by hospital/may be billed separately')]
        except:
            pass
        try:
            cols=[i for i in FINAL.columns if i not in ["description","cpt_hcpcs",'hospital_Id']]
            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
        except:
            pass

        FINAL["lineitem_cnt"] = FINAL.groupby(["description","cpt_hcpcs"])["cpt_hcpcs"].transform('count')
        FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]

        FINAL=FINAL.groupby(['description','cpt_hcpcs','lineitem_cnt','hospital_Id']).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)



        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1020_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[10]:


################# wideFormat1009 ################
def wideFormat1009(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]

                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]

                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]



                        FINAL
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass
        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass


        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:
                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        FINAL['cpt_hcpcs'].fillna(method='ffill', inplace=True)
        FINAL['description'].fillna(method='ffill', inplace=True)
        FINAL['drg'].fillna(method='ffill', inplace=True)
        FINAL['drg'] = FINAL['drg'].fillna(0)


        try:
            cols=[i for i in FINAL.columns if i not in ["description","cpt_hcpcs",'inpatient-outpatient','drg']]
            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
        except:
            pass

        FINAL["lineitem_cnt"] = FINAL.groupby(["description","cpt_hcpcs","drg"])["description"].transform('count')
        FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]

        FINAL=FINAL.groupby(['description','cpt_hcpcs','drg','lineitem_cnt']).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)



        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' ip')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' op')) , 'Outpatient', Sample_output['inpatient_outpatient'])


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1009_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[11]:


################## wideFormat1010 ################
def wideFormat1010(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]

                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]

                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]



                        FINAL
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass
        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass



        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:
                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        try:
            FINAL.dropna(subset = ["procedure_chargenumber"], inplace=True)
        except:
            pass

        try:
            FINAL = FINAL.rename(columns={"description_1":"description"})
        except:
            pass


        try:
            FINAL['description'].fillna(method='ffill', inplace=True)
            FINAL["cpt_hcpcs"].fillna(method="ffill",inplace=True)
            FINAL["lineitem_cnt"] = FINAL.groupby(["description"])["description"].transform('count')

            all_columns = list(FINAL)
            FINAL[all_columns] = FINAL[all_columns].replace(r'^\s*$', '', regex=True)

            cols=[i for i in FINAL.columns if i not in ["description",'cpt_hcpcs',"hospital_Id"]]
            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
            #FINAL["LineItem_cnt"] = FINAL.groupby(["HCPCS"])["HCPCS"].transform('count')
            FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]-1   
            FINAL=FINAL.groupby(['cpt_hcpcs','lineitem_cnt','description',"hospital_Id"]).aggregate(['sum']).reset_index()
            FINAL.columns = FINAL.columns.get_level_values(0)
            FINAL
        except: 
            FINAL['cpt_hcpcs'].fillna(method='ffill', inplace=True)
            FINAL["lineitem_cnt"] = FINAL.groupby(["cpt_hcpcs"])["cpt_hcpcs"].transform('count')

            all_columns = list(FINAL)
            FINAL[all_columns] = FINAL[all_columns].replace(r'^\s*$', '', regex=True)

            cols=[i for i in FINAL.columns if i not in ["description",'cpt_hcpcs','shoppable_service_category',"hospital_Id"]]
            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
            #FINAL["LineItem_cnt"] = FINAL.groupby(["HCPCS"])["HCPCS"].transform('count')
            FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]-1   
            FINAL=FINAL.groupby(['cpt_hcpcs','lineitem_cnt','description','shoppable_service_category',"hospital_Id"]).aggregate(['sum']).reset_index()
            FINAL.columns = FINAL.columns.get_level_values(0)
            FINAL

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1010_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[12]:


######################### wideFormat1011 #################
def wideFormat1011(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]


                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass
        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass



        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])
        try:
            try:
                FINAL['cpt_hcpcs_drg_aprdrg_icdx10'].fillna(method='ffill', inplace=True)
                # FINAL=FINAL[FINAL['Procedure/Charge Number'].notnull()]
                FINAL["lineitem_cnt"] = FINAL.groupby("cpt_hcpcs_drg_aprdrg_icdx10")["cpt_hcpcs_drg_aprdrg_icdx10"].transform('count')
                FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]-1

                FINAL=FINAL[(FINAL["lineitem_cnt"] ==0) | ((FINAL["lineitem_cnt"] >=1) & (FINAL["primary_service_and_ancillary_service"].str.contains("Total",case=False))) ]
                FINAL  
            except:
                FINAL['billing_code'].fillna(method='ffill', inplace=True)
                FINAL=FINAL[FINAL['procedure_chargenumber'].notnull()]
                FINAL["lineitem_cnt"] = FINAL.groupby("billing_code")["billing_code"].transform('count')
                FINAL["lineitem_cnt"]=FINAL["lineitem_cnt"]-1

                FINAL=FINAL[(FINAL["lineitem_cnt"] ==0) | ((FINAL["lineitem_cnt"] >=1) & (FINAL["procedure_chargenumber"].str.contains("CLAIM",case=False))) ]
                FINAL  
                FINAL.drop(["procedure_chargenumber"], axis = 1, inplace = True)
        except:
            pass

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' ip')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' op')) , 'Outpatient', Sample_output['inpatient_outpatient'])


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1011_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[13]:


######################### wideFormat1012 #####################
def wideFormat1012(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass


        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass

        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})




        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:
                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():

            for r in DF6.itertuples(index=False):



                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '   


        new = FINAL['description'].str.split(" ",n=1,expand=True)
        # new = FINAL['description'].str.split(" ",n=2,expand=True)

        FINAL['Cpt Code'] = new[0]

        # FINAL['Cpt Code'] = FINAL['Cpt Code'].astype([np.int64], errors = 'coerce')

        FINAL["Cpt Code"]= np.where(FINAL['Cpt Code'].str.endswith('-') , FINAL['Cpt Code'].replace('-','', regex=True),FINAL['Cpt Code'])
        FINAL['Cpt Code'] = FINAL['Cpt Code'].str.strip()

        # FINAL['Cpt Code'] = FINAL[["Cpt Code"]].to_numpy()
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['Cpt Code1']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '       


        # FINAL['Cpt Code'] = re.sub('[^a-zA-Z]', '', FINAL['Cpt Code'])

        #df['name_code_is_alphanumeric'] = list(map(lambda x: x.isalnum(), df['name_code']))
        # getting all numeric characters.....
        # FINAL["Cpt Code"]= np.where(FINAL['Cpt Code'].str.contains('.') , np.nan,FINAL['Cpt Code'])
        # for index, row in FINAL.iterrows():
        #     if re.findall(r'^[0-9]{2}[0-9]{3}',FINAL['Cpt Code'][index]):
        #         FINAL['Cpt Code1'][index] = FINAL['Cpt Code'][index]
        #     else:
        #         FINAL['Cpt Code1'][index] = np.nan
        FINAL['Cpt Code1'] = pd.to_numeric(FINAL['Cpt Code'],errors="coerce")

        for index, row in FINAL.iterrows():
            if re.findall(r'^[A-Z]{1}[0-9]{4}',FINAL['Cpt Code'][index]):
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code'][index]
            else:
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code1'][index]    
        # FINAL['Cpt Code2'] = np.where((FINAL['Cpt  ==' ') & (Combined_data['name'].str.contains('inpatient')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        # for getting all hcpcs codes
        for index, row in FINAL.iterrows():
            if re.findall(r'^[0-9]{5}',FINAL['Cpt Code'][index]):
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code'].str[:5][index]
            else:
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code1'][index]

        FINAL['Cpt Code1'] = FINAL['Cpt Code1'].astype(str)

        FINAL['Cpt Code1']=FINAL['Cpt Code1'].replace('\.0', '', regex=True)

        FINAL.drop(["Cpt Code"], axis = 1, inplace = True)
        FINAL=FINAL.rename(columns = {'Cpt Code1': "cpt_hcpcs_drg_icd10"}) 

        FINAL=FINAL[FINAL['cpt_hcpcs_drg_icd10'].notnull()]



        df2 = FINAL    
        df2 = df_column_uniquify(df2)
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        # df3=df3[df3['code'].notnull()] 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Sample_output = Combined_data
    # Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1012_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output 


# In[14]:


################# wideFormat1015 ###################
def wideFormat1015(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            DF_11=DF_FINAL.iloc[c:]
                            DF_11[:c]=DF_11[:c].fillna(method='ffill', axis=1)
                            DF_11[:c] = DF_11[:c].fillna(' ')
                            DF_FINAL.columns = (DF_FINAL.iloc[c] +' '+ DF_FINAL.iloc[c+1])
                            DF_FINAL = DF_FINAL.iloc[c+2:]
                            FINAL = DF_FINAL.reset_index(drop=True)
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        DF_11[:c]=DF_11[:c].fillna(method='ffill', axis=1)
                        DF_11[:c] = DF_11[:c].fillna(' ')
                        DF_FINAL.columns = (DF_FINAL.iloc[c] +' '+ DF_FINAL.iloc[c+1])
                        DF_FINAL = DF_FINAL.iloc[c+2:]
                        FINAL = DF_FINAL.reset_index(drop=True)
                        FINAL['id']=k[1]



                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None

            except Exception as e:
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass






        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]


        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1015_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[15]:


################# wideFormat1016 ####################
def wideFormat1016(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str,header=None)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[2])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            DF_FINAL.iloc[:c, :c] = np.nan

                            DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                            DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                            idx = DF_FINAL.index.get_loc(c)
                            DF_11 = DF_FINAL.iloc[idx - c :]
                            j = c+1

                            req_rows = np.where(DF_11.index == j)[0][0]
                            start = max(0, req_rows - j )
                            end = max(1, req_rows)
                            DF_12 = DF_11.iloc[start:end]

                            DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                            DF_12 = DF_12.to_frame()
                            DF_13 = DF_12.T

                            DF_11.drop(DF_11.head(j).index, inplace = True)
                            DF15 = DF_13.append(DF_11)
                            DF16 = DF15.reset_index(drop = True)
                            DF16.columns = DF16.iloc[0]
                            FINAL = DF16[1:]
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_FINAL.iloc[:c, :c] = np.nan

                        DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                        DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                        idx = DF_FINAL.index.get_loc(c)
                        DF_11 = DF_FINAL.iloc[idx - c :]
                        j = c+1

                        req_rows = np.where(DF_11.index == j)[0][0]
                        start = max(0, req_rows - j )
                        end = max(1, req_rows)
                        DF_12 = DF_11.iloc[start:end]

                        DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                        DF_12 = DF_12.to_frame()
                        DF_13 = DF_12.T

                        DF_11.drop(DF_11.head(j).index, inplace = True)
                        DF15 = DF_13.append(DF_11)
                        DF16 = DF15.reset_index(drop = True)
                        DF16.columns = DF16.iloc[0]
                        FINAL = DF16[1:]
                        FINAL['id']=k[1]



                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None

            except Exception as e:
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass






        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat1016_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[16]:


############### longFormat1 ####################
def longFormat1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                df=pd.read_csv(f)
            except:
                df=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(df)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None)

            for sh in sheet_to.keys():

                df = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(df)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for df in non_prep_data:
            try:
                if df.iloc[:, [s]].empty == False:

                    if 'Unnamed: 0' not in df.iloc[:, [0]]:
                        if 'Primary Service and Ancillary Services' not in df.iloc[:, [2]]:

                            DF_13=df.iloc[:, [s]]
                            M=0
                            C=0   

                            for index, row in DF_13.iterrows():
                                if(pd.notnull(row[0])):
                                    M=M+1
                                    break
                                else:
                                    C=C+1

                            if C==0:
                                #This Case works when description is in one row
                                if "Unnamed: 5" in DF_13:
                                    df.columns=df.iloc[0]
                                    FINAL=df.drop([C])
                                    FINAL['id']=k[1]
                                    FINAL 

                                else:
                                    #This Case works when description is not found
                                    FINAL=df
                                    FINAL['id']=k[1]
                                    FINAL
                            elif C>=1:
                                #This Case works when description is more than 1 row
                                #Dropping 'c' rows
                                DF_14=df.iloc[C:]
                                #row values as column names
                                DF_14.columns=DF_14.iloc[0]
                                #Dropping the row
                                FINAL=DF_14.drop([C])
                                FINAL['id']=k[1] 
                                FINAL 
                            else:
                                None
                        else:
                            df['Shoppable Services'].fillna(method='ffill', inplace=True)
                            df=df.rename(columns = {'Shoppable Services': 'ROWS'})
                            df
                            df=df[df['Primary Service and Ancillary Services']!='Primary Service and Ancillary Services']

                            List_ids=[]
                            for i in df['ROWS']:
                                List_ids.append(i)

                                List_ids=list(set(List_ids))

                            combined_df = []
                            for i in List_ids:
                                #select one part of the data
                                df1 = df[df['ROWS']==i] 
                                #Drop the last row
                                #df1 = df1.iloc[:-1]

                                try:

                                    ds=df1.iloc[:1,:]

                                    #ds.drop(["Unnamed: 0", "Unnamed: 1","CPT / HCPCS / ICD-10 Code","Average Unit Count","Rev Code","Charge"], axis = 1, inplace = True) 
                                    ds=ds[['Primary Service and Ancillary Services']] 
                                    ds=ds.reset_index(drop=True)
                                    ds_T=ds.transpose()
                                    DS1=ds_T.rename(columns = {0: "CPT_Description"}) 
                                    DS1=DS1.reset_index(drop=True)
                                    DS1
                                    #inpatient-outpatient
                                    di = df1.iloc[:2,:]
                                    #print(m)
                                    di = di.drop([di.index[0]],axis=0)
                                    di=di[['Primary Service and Ancillary Services']] 
                                    di=di.reset_index(drop=True)
                                    di_T=di.transpose()
                                    di3=di_T.rename(columns = {0: "inpatient-outpatient1"}) 
                                    DS4=di3.reset_index(drop=True)
                                    DS4
                                    #Total of Charges -PART -2
                                    ds=df1[df1['Unnamed: 1'].notnull()]
                                    ds.drop(["ROWS", "Unnamed: 1"], axis = 1, inplace = True) 
                                    ds=ds.reset_index(drop=True)
                                    ds

                                    #Total Charges
                                    ds1=df1[df1['Average Unit Count']=='Total of Charges:']
                                    ds1=ds1[['Charge']] 
                                    ds1=ds1.reset_index(drop=True)
                                    ds1.rename(columns = {'Charge': "Total Charges"},inplace = True)
                                    ds1

                                    DS2=pd.concat([ds,ds1], axis=1)
                                    DS2['LineItem_cnt']=DS2.shape[0]
                                    C=DS2.shape[0]

                                    #Payers
                                    ds= df1.iloc[C+2:,:]
                                    ds=ds[['Primary Service and Ancillary Services','Charge']]
                                    ds=ds[ds['Primary Service and Ancillary Services'].notnull()]
                                    #ds1=ds.dropna() 

                                    #ds['Primary Service and Ancillary Services'] = ds['Primary Service and Ancillary Services'].str.replace('Charge for', '')
                                    ds2=ds.transpose()
                                    ds2.columns = ds2.iloc[0] 

                                    #ds2=ds2.drop(["Primary Service and Ancillary Services"])
                                    ds2=ds2.iloc[1:]
                                    DS3=ds2.reset_index(drop=True)

                                    DS_F=pd.concat([DS1,DS2,DS3,DS4], axis=1)
                                    DS_F = DS_F.dropna(axis='columns', how='all')
                                    DS_F['id']=k[1]


                                except:
                                    pass

                                combined_df.append(DS_F)
                                #combined_df['id']=k[1]
                            combined_df = pd.concat(combined_df)
                            combined_df.drop(["Rev Code","Charge"], axis = 1, inplace = True)
                            FINAL=combined_df[combined_df['CPT_Description'].notnull()]
                            FINAL['CPT_Description1'] = FINAL.CPT_Description.str.split('-',1)
                            FINAL[['CPT/HCPCS/ICD-10 Code','Cpt_description2']] = pd.DataFrame(FINAL.CPT_Description1.tolist(), index= FINAL.index)
                            FINAL.drop(["CPT_Description","CPT_Description1", "Primary Service and Ancillary Services","CPT / HCPCS / ICD-10 Code"], axis = 1, inplace = True)
                            FINAL=FINAL.rename(columns = {'Cpt_description2': "CPT_Description"})
                            FINAL=FINAL[FINAL['CPT/HCPCS/ICD-10 Code'].str.len().le(8)]
                            FINAL                  

                    else:
                        try:
                            try:
                                df=pd.read_csv(f)
                            except:
                                df=pd.read_csv(f,encoding='latin1')
                        except:
                            df = pd.read_excel(f, header = 1 )

                        if 'Unnamed: 0' in df.iloc[:, [0]]:
                            if 'Primary Service and Ancillary Services' not in df.iloc[:, [2]]:

                                DF_13=df.iloc[:, [5]]
                                M=0
                                C=0

                                for index, row in DF_13.iterrows():
                                    if(pd.notnull(row[0])):
                                        M=M+1
                                        break
                                    else:
                                        C=C+1
                                if C==0:
                                    if "Unnamed: 5" in DF_13:
                                        df.columns=df.iloc[0]
                                        FINAL=df.drop([C])
                                        FINAL['id']=k[1]
                                        FINAL 
                                    else:
                                    #This Case works when description is not found
                                        FINAL=df
                                        FINAL['id']=k[1]
                                        FINAL
                                elif C>=1:
                                    DF_14=df.iloc[C:]
                                        #row values as column names
                                    DF_14.columns=DF_14.iloc[0]
                                        #Dropping the row
                                    FINAL=DF_14.drop([C])
                                    FINAL['id']=k[1] 
                                    FINAL
                                    if np.nan in FINAL:
                                        FINAL=FINAL.rename(columns={np.nan: 'inpatient'})
                                        FINAL
                                        FINAL['inpatient'] = FINAL.Inpatient.str.replace('*','')
                                        FINAL['inpatient']=FINAL['inpatient'].mask(FINAL['inpatient'].eq('')|FINAL['inpatient'].isnull()).ffill()
                                        FINAL=FINAL.rename(columns = {'inpatient': "inpatient-outpatient"})
                                        FINAL['id']=k[1] 
                                        FINAL
                                    else:
                                        None
                                else:
                                    None



                            else:
                                try:
                                    df=pd.read_csv(f)
                                except:
                                    df = pd.read_excel(f, header = 1 )

                                df['Unnamed: 0'].fillna(method='ffill', inplace=True)
                                df=df.rename(columns = {'Unnamed: 0': 'ROWS'})
                                df
                                df=df[df['Primary Service and Ancillary Services']!='Primary Service and Ancillary Services']


                                List_ids=[]
                                for i in df['ROWS']:
                                    List_ids.append(i)

                                    List_ids=list(set(List_ids))

                                combined_df = []
                                for i in List_ids:
                                    #select one part of the data
                                    df1 = df[df['ROWS']==i] 
                                    #Drop the last row
                                    #df1 = df1.iloc[:-1]

                                    try:

                                        ds=df1.iloc[:1,:]

                                        #ds.drop(["Unnamed: 0", "Unnamed: 1","CPT / HCPCS / ICD-10 Code","Average Unit Count","Rev Code","Charge"], axis = 1, inplace = True) 
                                        ds=ds[['Primary Service and Ancillary Services']] 
                                        ds=ds.reset_index(drop=True)
                                        ds_T=ds.transpose()
                                        DS1=ds_T.rename(columns = {0: "CPT_Description"}) 
                                        DS1=DS1.reset_index(drop=True)
                                        DS1
                                        #inpatient-outpatient
                                        di = df1.iloc[:2,:]
                                        #print(m)
                                        di = di.drop([di.index[0]],axis=0)
                                        di=di[['Primary Service and Ancillary Services']] 
                                        di=di.reset_index(drop=True)
                                        di_T=di.transpose()
                                        di3=di_T.rename(columns = {0: "inpatient-outpatient1"}) 
                                        DS4=di3.reset_index(drop=True)
                                        DS4
                                        #Total of Charges -PART -2
                                        ds=df1[df1['Unnamed: 1'].notnull()]
                                        ds.drop(["ROWS", "Unnamed: 1"], axis = 1, inplace = True) 
                                        ds=ds.reset_index(drop=True)
                                        ds

                                        #Total Charges
                                        ds1=df1[df1['Average Unit Count']=='Total of Charges:']
                                        ds1=ds1[['Charge']] 
                                        ds1=ds1.reset_index(drop=True)
                                        ds1.rename(columns = {'Charge': "Total Charges"},inplace = True)
                                        ds1

                                        DS2=pd.concat([ds,ds1], axis=1)
                                        DS2['LineItem_cnt']=DS2.shape[0]
                                        C=DS2.shape[0]

                                        #Payers
                                        ds= df1.iloc[C+2:,:]
                                        ds=ds[['Primary Service and Ancillary Services','Charge']]
                                        ds=ds[ds['Primary Service and Ancillary Services'].notnull()]
                                        #ds1=ds.dropna() 

                                        #ds['Primary Service and Ancillary Services'] = ds['Primary Service and Ancillary Services'].str.replace('Charge for', '')
                                        ds2=ds.transpose()
                                        ds2.columns = ds2.iloc[0] 

                                        #ds2=ds2.drop(["Primary Service and Ancillary Services"])
                                        ds2=ds2.iloc[1:]
                                        DS3=ds2.reset_index(drop=True)

                                        DS_F=pd.concat([DS1,DS2,DS3,DS4], axis=1)
                                        #DS_F = DS_F.dropna(axis='columns', how='all')
                                        DS_F['id']=k[1]


                                    except:
                                        pass

                                    combined_df.append(DS_F)
                                    #combined_df['ID']=k[1]
                                combined_df = pd.concat(combined_df)
                                combined_df.drop(["Rev Code","Charge"], axis = 1, inplace = True)
                                FINAL=combined_df[combined_df['CPT_Description'].notnull()]
                                FINAL['CPT_Description1'] = FINAL.CPT_Description.str.split('-',1)
                                FINAL[['CPT/HCPCS/ICD-10 Code','Cpt_description2']] = pd.DataFrame(FINAL.CPT_Description1.tolist(), index= FINAL.index)
                                FINAL.drop(["CPT_Description","CPT_Description1", "Primary Service and Ancillary Services","CPT / HCPCS / ICD-10 Code"], axis = 1, inplace = True)
                                FINAL=FINAL.rename(columns = {'Cpt_description2': "CPT_Description"}) 
                                FINAL=FINAL[FINAL['CPT/HCPCS/ICD-10 Code'].str.len().le(8)]
                                FINAL

                        else:
                            None
            except:
                pass


        try:
            FINAL[['service type','inpatient-outpatient']] = FINAL["inpatient-outpatient1"].str.split(":", 1, expand=True)
            FINAL.drop(["inpatient-outpatient1","service type"], axis = 1, inplace = True)
        except:
            pass
        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})


        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:

                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():

            for r in DF6.itertuples(index=False):

                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])



        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    ##Combined_data.dtypes
    Sample_output = Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Longformat1_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[17]:


##################### longFormat1000 ########################
def longFormat1000(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    for k in CNames.itertuples(index=False):
        f=k[0]

        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                try:
                    DF_FINAL=pd.read_csv(f)
                except:
                    DF_FINAL=pd.read_csv(f,sep="|")
            except:
                DF_FINAL=pd.read_csv(f,encoding="latin1")
            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f, sheet_name = None,dtype=str)
            for sh in sheet_to.keys():
                DF_FINAL=pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows..3
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 0" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})    


                            for i in FINAL.columns.tolist():
                                for k in CNames.itertuples(index=False):
                                    for r in DF6.itertuples(index=False):
                                        if str(k[1]) == str(r[2]):
                                            try:
                                                for i in FINAL.columns.tolist():
                                                    if str(i) == str(r[0]):
                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})    


                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass

                            for i in FINAL.columns.tolist():


                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})    


                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass

                        for i in FINAL.columns.tolist():


                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None    

            except Exception as e:
                print(e)
                pass

        combined_final.append(FINAL)    
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass    

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]    
        except:
            pass

        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass





        Search_List = list(DF8["Dropping columns"])
        try:
            try:
                FINAL["plan"]=FINAL["plan"].astype(str)
                FINAL['payer_name'] = FINAL['payer_name'].astype(str)
                FINAL['payer_name'] = FINAL[['payer_name','plan']].apply(lambda x: ' '.join(x), axis=1)
                FINAL = FINAL.drop(['plan'],axis = 1)
            except:
                FINAL['payer_name'] = FINAL['payer_name'].astype(str)
                FINAL['payer_name'] = FINAL[['payer_name']].apply(lambda x: ' '.join(x), axis=1)
        except:
            pass




        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        df2=FINAL
        df2 = df_column_uniquify(df2)
        # renaming column made the payor to name.So, manually renamed name to name1 
        # name1 is stored in idvars list
        try:
            try:
                df2.rename(columns = {'name': 'name1'}, inplace = True)
            except:
                pass
        except:
            pass

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        #Required format - variable list

        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')   
        df4=df3

        try:
            try:
                df4['name'] = df4[['payer_name', 'name']].apply(lambda x: ' '.join(x), axis=1)
            except:
                df4['name'] = df4[['name1', 'name']].apply(lambda x: ' '.join(x), axis=1)  
        except:
            pass



        df4=df4[df4['cost'].notnull()]
        try:
            df4=df4[df4['hcpcs'].notnull()]
        except:
            pass
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data
    try:
        try:
            Combined_data = Combined_data.drop(['payer_name'],axis = 1)
        except:
            # manually dropped because name is in idvars list
            Combined_data = Combined_data.drop(['name1'],axis = 1)
    except:
        pass
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Combined_data = Combined_data.drop_duplicates()
    try:
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('inpatient')) , 'Inpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('outpatient')) , 'Outpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient_outpatient'])
    except:
        pass
    Sample_output = Combined_data
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross charges'), 'gross charge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('self_pay'), 'self_pay',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('grosscharge'), 'grosscharge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross charge'), 'gross charge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross_charges'), 'gross charge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross_charge'), 'gross charge',Sample_output['name'])


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\longFormat1000_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[18]:


####################### longFormat10 ################
def longFormat10(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1


                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 0" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        #DF_FINAL = DF_FINAL.drop(['Unnamed: 0'],axis = 1)
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None   

            except Exception as e:
                print(e)
                pass

        # if CNames['id'].isin(['3073']).any():
        #     try:
        #         FINAL.drop(['HospitalDescription','StandardChargeQuantity','FootnoteReference'],axis=1,inplace=True)
        #         FINAL= FINAL.rename(columns ={'ServiceType': "inpatient-outpatient",'ChargeCode':"Procedure/Charge Number",'Code':"CPT/HCPCS"})
        #     except:
        #         None
        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x}) 
        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True) 

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
        FINAL['inpatient-outpatient'] = FINAL['inpatient-outpatient'].str.lower()
        #Checking the  Inpatient/Outpatient column exists or not

        try:
            FINAL["payer_name"]=FINAL["payer_name"].astype(str)
            #FINAL["Benfit Type"]=FINAL["Benfit Type"].astype(str)
            FINAL['payer_name'] = FINAL[['payer_name']].apply(lambda x: ' '.join(x), axis=1)
            #FINAL = FINAL.drop(['Benfit Type'],axis = 1)
        except:
            pass
        # FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        try:
            FINAL['inpatient-outpatient'] = FINAL['inpatient-outpatient'].str.lower()
        except:
            None
        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list

        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')

        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        try:
            df3['name'] = df3[['payer_name', 'name']].apply(lambda x: ' '.join(x), axis=1)
            df3 = df3.drop(['payer_name'],axis=1)
        except:
            None


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    # 

    try:
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('inpatient')) , 'Inpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('outpatient')) , 'Outpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('ip')) , 'Inpatient', Combined_data['inpatient_outpatient'])
        Combined_data['inpatient_outpatient'] = np.where((Combined_data['inpatient_outpatient'] ==' ') & (Combined_data['name'].str.contains('op')) , 'Outpatient', Combined_data['inpatient_outpatient'])
    except:
        pass
    Sample_output = Combined_data
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross charges'), 'gross charge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('self_pay'), 'self_pay',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('grosscharge'), 'grosscharge',Sample_output['name'])
    Sample_output['name'] = np.where(Sample_output['name'].str.contains('gross charge'), 'gross charge',Sample_output['name'])
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\longFormat10_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[19]:


################ longFormat2_1 ###############
def longFormat2_1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                df=pd.read_csv(f)
            except:
                df=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(df)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None)

            for sh in sheet_to.keys():

                df = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(df)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for df in non_prep_data:
            try:
                if df.iloc[:, [5]].empty == False:


                    # if 'Unnamed: 0' not in df.iloc[:, [0]]:
                    #     if 'Primary Service and Ancillary Services' not in df.iloc[:, [2]]:

                    DF_13=df.iloc[:, [5]]
                    M=0
                    C=0   

                    for index, row in DF_13.iterrows():
                        if(pd.notnull(row[0])):
                            M=M+1
                            break
                        else:
                            C=C+1

                    if C==0:
                #This Case works when description is in one row
                        if "Unnamed: 3" in DF_13:
                            df.columns=df.iloc[0]
                            FINAL=df.drop([C])
                            FINAL['id']=k[1]
                            FINAL 

                        else:
                                    #This Case works when description is not found
                            FINAL=df
                            FINAL['id']=k[1]
                            FINAL
                    elif C>=1:
                                #This Case works when description is more than 1 row
                                #Dropping 'c' rows
                        DF_14=df.iloc[C:]
                                #row values as column names
                        DF_14.columns=DF_14.iloc[0]
                                #Dropping the row
                        FINAL=DF_14.drop([C])
                        FINAL['id']=k[1] 
                        FINAL

                    else:
                        None


                    df1=FINAL.dropna(axis = 0, how = 'all')
                    df1['Primary Service and Ancillary Services'] = np.where((df1['Shoppable Service'].notnull())&(df1['Primary Service and Ancillary Services'].isnull()),df1['Shoppable Service'],df1['Primary Service and Ancillary Services'])
                    df1['CPT/HCPCS ICD-10 Codes_1'] = np.where((df1['Shoppable Service'].notnull())&(df1['Primary Service and Ancillary Services'].notnull())&(df1['CPT/HCPCS ICD-10 Codes'].notnull()),df1['CPT/HCPCS ICD-10 Codes'], "")
                    df2 = df1.replace(r'^\s*$', np.NaN, regex=True)
                    df2['CPT/HCPCS ICD-10 Codes_1'].fillna(method='ffill', inplace=True)
                    df2=df2.rename(columns = {'CPT/HCPCS ICD-10 Codes_1': 'ROWS'})
                    df3=df2[df2['Primary Service and Ancillary Services']!='Primary Service and Ancillary Services']

                    List_ids=[]
                    for i in df3['ROWS']:
                        List_ids.append(i)

                        List_ids=list(set(List_ids))

                    combined_df = []
                    for i in List_ids:
                    #select one part of the data
                        df4 = df3[df3['ROWS']==i] 

                        try:
                            ds=df4.iloc[:1,:] 
                            ds=ds[['ROWS']]
                            ds=ds.reset_index(drop=True)
                            ds_T=ds.transpose()

                            DS1=ds_T.rename(columns = {0: "CPT_code"}) 
                            DS1=DS1.reset_index(drop=True)

                            ds=df4[df4['Primary Service and Ancillary Services'].notnull()]
                            ds

                            ds_1=ds[ds['CPT/HCPCS ICD-10 Codes'].notnull()]
                            ds_1

                            ds_1.drop(["ROWS"], axis = 1, inplace = True) 
                            ds_1=ds_1.reset_index(drop=True)
                            ds_1

                            ds1=df4[df4['Primary Service and Ancillary Services']=='Total of Standard Charges']
                            ds1=ds1[[' Standard Charge ']]
                            ds1=ds1.reset_index(drop=True)
                            ds1.rename(columns = {' Standard Charge ': "Total Charges"},inplace = True)
                            ds1

                            DS2=pd.concat([ds_1,ds1], axis=1)
                            DS2 

                            DS2['LineItem_cnt']=DS2.shape[0]
                            C=DS2.shape[0]
                            ds= df4.iloc[C+2:,:]
                            ds=ds[['Primary Service and Ancillary Services',' Standard Charge ']]
                            ds=ds[ds['Primary Service and Ancillary Services'].notnull()]
                            a=['Total of Standard Charges']

                            ds = ds[~ds['Primary Service and Ancillary Services'].isin(a)]
                            ds2=ds.transpose()
                            ds2.reset_index(drop=True,inplace=True)
                            ds2.columns = ds2.iloc[0]

                            ds2=ds2.iloc[1:]
                            ds2.reset_index(drop=True,inplace=True)
                            DS_F=pd.concat([DS1,DS2,ds2], axis=1)
                            DS_F = DS_F.dropna(axis='columns', how='all')
                            DS_F.drop(["Primary Service and Ancillary Services","CPT/HCPCS ICD-10 Codes"," Standard Charge "], axis = 1, inplace = True)
                            FINAL1=DS_F[DS_F['Shoppable Service'].notnull()]
                            FINAL1=FINAL1.rename(columns = {'CPT_code': "CPT/HCPCS/ICD-10 Code","Shoppable Service":"description"})
                            FINAL1
                        except:pass
                        combined_df.append(FINAL1)
                                    #combined_df['ID']=k[1]
                    combined_df = pd.concat(combined_df)
                else:
                    None
            except Exception as e:
                print(e)
                pass



        FINAL = combined_df


        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x}) 
        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)
        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df_2=FINAL
        df_2 = df_column_uniquify(df_2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df_3=df_2.melt(id_vars=[col for col in df_2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df_4=df_3
        df_4=df_4[df_4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df_4
        Combined_data.append(df_4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\longFormat2_1_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[20]:


###################### longFormat2 ####################
def longFormat2(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                df=pd.read_csv(f)
            except:
                df=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(df)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None)

            for sh in sheet_to.keys():

                df = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(df)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for df in non_prep_data:
            try:
                if df.iloc[:, [s]].empty == False:

                    if 'Unnamed: 0' not in df.iloc[:, [0]]:
                        if 'Primary Service and Ancillary Services' not in df.iloc[:, [2]]:

                            DF_13=df.iloc[:, [s]]
                            M=0
                            C=0   

                            for index, row in DF_13.iterrows():
                                if(pd.notnull(row[0])):
                                    M=M+1
                                    break
                                else:
                                    C=C+1

                            if C==0:
                                #This Case works when description is in one row
                                if "Unnamed: 5" in DF_13:
                                    df.columns=df.iloc[0]
                                    FINAL=df.drop([C])
                                    FINAL['id']=k[1]
                                    FINAL 

                                else:
                                    #This Case works when description is not found
                                    FINAL=df
                                    FINAL['id']=k[1]
                                    FINAL
                            elif C>=1:
                                #This Case works when description is more than 1 row
                                #Dropping 'c' rows
                                DF_14=df.iloc[C:]
                                #row values as column names
                                DF_14.columns=DF_14.iloc[0]
                                #Dropping the row
                                FINAL=DF_14.drop([C])
                                FINAL['id']=k[1] 
                                FINAL 
                            else:
                                None
                        else:
                            df['Shoppable Services'].fillna(method='ffill', inplace=True)
                            df=df.rename(columns = {'Shoppable Services': 'ROWS'})
                            df
                            df=df[df['Primary Service and Ancillary Services']!='Primary Service and Ancillary Services']

                            List_ids=[]
                            for i in df['ROWS']:
                                List_ids.append(i)

                                List_ids=list(set(List_ids))

                            combined_df = []
                            for i in List_ids:
                                #select one part of the data
                                df1 = df[df['ROWS']==i] 
                                #Drop the last row
                                #df1 = df1.iloc[:-1]

                                try:

                                    ds=df1.iloc[:1,:]

                                    #ds.drop(["Unnamed: 0", "Unnamed: 1","CPT / HCPCS / ICD-10 Code","Average Unit Count","Rev Code","Charge"], axis = 1, inplace = True) 
                                    ds=ds[['Primary Service and Ancillary Services']] 
                                    ds=ds.reset_index(drop=True)
                                    ds_T=ds.transpose()
                                    DS1=ds_T.rename(columns = {0: "CPT_Description"}) 
                                    DS1=DS1.reset_index(drop=True)
                                    DS1
                                    #inpatient-outpatient
                                    di = df1.iloc[:2,:]
                                    #print(m)
                                    di = di.drop([di.index[0]],axis=0)
                                    di=di[['Primary Service and Ancillary Services']] 
                                    di=di.reset_index(drop=True)
                                    di_T=di.transpose()
                                    di3=di_T.rename(columns = {0: "inpatient-outpatient1"}) 
                                    DS4=di3.reset_index(drop=True)
                                    DS4
                                    #Total of Charges -PART -2
                                    ds=df1[df1['Unnamed: 1'].notnull()]
                                    ds.drop(["ROWS", "Unnamed: 1"], axis = 1, inplace = True) 
                                    ds=ds.reset_index(drop=True)
                                    ds

                                    #Total Charges
                                    ds1=df1[df1['Average Unit Count']=='Total of Charges:']
                                    ds1=ds1[['Charge']] 
                                    ds1=ds1.reset_index(drop=True)
                                    ds1.rename(columns = {'Charge': "Total Charges"},inplace = True)
                                    ds1

                                    DS2=pd.concat([ds,ds1], axis=1)
                                    DS2['LineItem_cnt']=DS2.shape[0]
                                    C=DS2.shape[0]

                                    #Payers
                                    ds= df1.iloc[C+2:,:]
                                    ds=ds[['Primary Service and Ancillary Services','Charge']]
                                    ds=ds[ds['Primary Service and Ancillary Services'].notnull()]
                                    #ds1=ds.dropna() 

                                    #ds['Primary Service and Ancillary Services'] = ds['Primary Service and Ancillary Services'].str.replace('Charge for', '')
                                    ds2=ds.transpose()
                                    ds2.columns = ds2.iloc[0] 

                                    #ds2=ds2.drop(["Primary Service and Ancillary Services"])
                                    ds2=ds2.iloc[1:]
                                    DS3=ds2.reset_index(drop=True)

                                    DS_F=pd.concat([DS1,DS2,DS3,DS4], axis=1)
                                    DS_F = DS_F.dropna(axis='columns', how='all')
                                    DS_F['id']=k[1]


                                except:
                                    pass

                                combined_df.append(DS_F)
                                #combined_df['id']=k[1]
                            combined_df = pd.concat(combined_df)
                            combined_df.drop(["Rev Code","Charge"], axis = 1, inplace = True)
                            FINAL=combined_df[combined_df['CPT_Description'].notnull()]
                            FINAL['CPT_Description1'] = FINAL.CPT_Description.str.split('-',1)
                            FINAL[['CPT/HCPCS/ICD-10 Code','Cpt_description2']] = pd.DataFrame(FINAL.CPT_Description1.tolist(), index= FINAL.index)
                            FINAL.drop(["CPT_Description","CPT_Description1", "Primary Service and Ancillary Services","CPT / HCPCS / ICD-10 Code"], axis = 1, inplace = True)
                            FINAL=FINAL.rename(columns = {'Cpt_description2': "CPT_Description"})
                            FINAL=FINAL[FINAL['CPT/HCPCS/ICD-10 Code'].str.len().le(8)]
                            FINAL                  

                    else:
                        try:
                            try:
                                df=pd.read_csv(f)
                            except:
                                df=pd.read_csv(f,encoding='latin1')
                        except:
                            df = pd.read_excel(f, header = 1 )

                        if 'Shoppable Services' in df.iloc[:, [0]]:
                            if 'Primary Service and Ancillary Services' not in df.iloc[:, [2]]:

                                DF_13=df.iloc[:, [s]]
                                M=0
                                C=0

                                for index, row in DF_13.iterrows():
                                    if(pd.notnull(row[0])):
                                        M=M+1
                                        break
                                    else:
                                        C=C+1
                                if C==0:
                                    if "Unnamed: 5" in DF_13:
                                        df.columns=df.iloc[0]
                                        FINAL=df.drop([C])
                                        FINAL['id']=k[1]
                                        FINAL 
                                    else:
                                    #This Case works when description is not found
                                        FINAL=df
                                        FINAL['id']=k[1]
                                        FINAL
                                elif C>=1:
                                    DF_14=df.iloc[C:]
                                        #row values as column names
                                    DF_14.columns=DF_14.iloc[0]
                                        #Dropping the row
                                    FINAL=DF_14.drop([C])
                                    FINAL['id']=k[1] 
                                    FINAL
                                    if np.nan in FINAL:
                                        FINAL=FINAL.rename(columns={np.nan: 'inpatient'})
                                        FINAL
                                        FINAL['inpatient'] = FINAL.Inpatient.str.replace('*','')
                                        FINAL['inpatient']=FINAL['inpatient'].mask(FINAL['inpatient'].eq('')|FINAL['inpatient'].isnull()).ffill()
                                        FINAL=FINAL.rename(columns = {'inpatient': "inpatient-outpatient"})
                                        FINAL['id']=k[1] 
                                        FINAL
                                    else:
                                        None
                                else:
                                    None



                            else:
                                try:
                                    df=pd.read_csv(f)
                                except:
                                    df = pd.read_excel(f, header = 1 )

                                df['Shoppable Services'].fillna(method='ffill', inplace=True)
                                df=df.rename(columns = {'Shoppable Services': 'ROWS'})
                                df
                                df=df[df['Primary Service and Ancillary Services']!='Primary Service and Ancillary Services']


                                List_ids=[]
                                for i in df['ROWS']:
                                    List_ids.append(i)

                                    List_ids=list(set(List_ids))

                                combined_df = []
                                for i in List_ids:
                                    #select one part of the data
                                    df1 = df[df['ROWS']==i] 
                                    #Drop the last row
                                    #df1 = df1.iloc[:-1]

                                    try:

                                        ds=df1.iloc[:1,:]

                                        #ds.drop(["Unnamed: 0", "Unnamed: 1","CPT / HCPCS / ICD-10 Code","Average Unit Count","Rev Code","Charge"], axis = 1, inplace = True) 
                                        ds=ds[['Primary Service and Ancillary Services']] 
                                        ds=ds.reset_index(drop=True)
                                        ds_T=ds.transpose()
                                        DS1=ds_T.rename(columns = {0: "CPT_Description"}) 
                                        DS1=DS1.reset_index(drop=True)
                                        DS1
                                        #inpatient-outpatient
                                        di = df1.iloc[:2,:]
                                        #print(m)
                                        di = di.drop([di.index[0]],axis=0)
                                        di=di[['Primary Service and Ancillary Services']] 
                                        di=di.reset_index(drop=True)
                                        di_T=di.transpose()
                                        di3=di_T.rename(columns = {0: "inpatient-outpatient1"}) 
                                        DS4=di3.reset_index(drop=True)
                                        DS4
                                        #Total of Charges -PART -2
                                        ds=df1[df1['Unnamed: 1'].notnull()]
                                        ds.drop(["ROWS", "Unnamed: 1"], axis = 1, inplace = True) 
                                        ds=ds.reset_index(drop=True)
                                        ds

                                        #Total Charges
                                        ds1=df1[df1['Average Unit Count']=='Total of Charges:']
                                        ds1=ds1[['Charge']] 
                                        ds1=ds1.reset_index(drop=True)
                                        ds1.rename(columns = {'Charge': "Total Charges"},inplace = True)
                                        ds1

                                        DS2=pd.concat([ds,ds1], axis=1)
                                        DS2['LineItem_cnt']=DS2.shape[0]
                                        C=DS2.shape[0]

                                        #Payers
                                        ds= df1.iloc[C+2:,:]
                                        ds=ds[['Primary Service and Ancillary Services','Charge']]
                                        ds=ds[ds['Primary Service and Ancillary Services'].notnull()]
                                        #ds1=ds.dropna() 

                                        #ds['Primary Service and Ancillary Services'] = ds['Primary Service and Ancillary Services'].str.replace('Charge for', '')
                                        ds2=ds.transpose()
                                        ds2.columns = ds2.iloc[0] 

                                        #ds2=ds2.drop(["Primary Service and Ancillary Services"])
                                        ds2=ds2.iloc[1:]
                                        DS3=ds2.reset_index(drop=True)

                                        DS_F=pd.concat([DS1,DS2,DS3,DS4], axis=1)
                                        #DS_F = DS_F.dropna(axis='columns', how='all')
                                        DS_F['id']=k[1]


                                    except:
                                        pass

                                    combined_df.append(DS_F)
                                    #combined_df['ID']=k[1]
                                combined_df = pd.concat(combined_df)
                                combined_df.drop(["Rev Code","Charge"], axis = 1, inplace = True)
                                FINAL=combined_df[combined_df['CPT_Description'].notnull()]
                                FINAL['CPT_Description1'] = FINAL.CPT_Description.str.split('-',1)
                                FINAL[['CPT/HCPCS/ICD-10 Code','Cpt_description2']] = pd.DataFrame(FINAL.CPT_Description1.tolist(), index= FINAL.index)
                                FINAL.drop(["CPT_Description","CPT_Description1", "Primary Service and Ancillary Services","CPT / HCPCS / ICD-10 Code"], axis = 1, inplace = True)
                                FINAL=FINAL.rename(columns = {'Cpt_description2': "CPT_Description"}) 
                                FINAL=FINAL[FINAL['CPT/HCPCS/ICD-10 Code'].str.len().le(8)]
                                FINAL

                        else:
                            None
            except Exception as e:
                print(e)
                pass


        try:
            FINAL[['service type','inpatient-outpatient']] = FINAL["inpatient-outpatient1"].str.split(":", 1, expand=True)
            FINAL.drop(["inpatient-outpatient1","service type"], axis = 1, inplace = True)
        except:
            pass
        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})


        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:

                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():

            for r in DF6.itertuples(index=False):

                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])



        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    ##Combined_data.dtypes
    Sample_output = Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Longformat2_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[21]:


################### wideFormat106 ##################
def wideFormat106(CNames):
    global df7      
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                    
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                    
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL
                    
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass
    
       
    
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        
    
        for i in FINAL.columns.tolist():
    
            for k in CNames.itertuples(index=False):
    
                for r in DF6.itertuples(index=False):
    
                    if str(k[1]) == str(r[2]):
                        try:
    
                            for i in FINAL.columns.tolist():
    
                                if str(i) == str(r[0]):
    
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass
    
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)
    
        Search_List = list(DF8["Dropping columns"])
        #dropping columns
    
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
    
        FINAL.loc[:, FINAL.columns.notnull()]
        
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
    
       
    
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
    
        
    
        df4=df3
        df4=df4[df4['cost'].notnull()]
    
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    
    Combined_data
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat106_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[22]:


################## wideFormat107 #################
def wideFormat107(CNames):
    global df7      
    Combined_data = []
    combined_final = []
    non_prep_data = []
    def similar(x1, x2):
        return SequenceMatcher(None, x1, x2).ratio()
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                 
                    if len(set(sh.lower().split()) & set(f.lower().split())) != 0:
    #                     DF_FINAL=pd.read_excel(f,sheet_name= sh)
    #                     print(sh)
                        s.append(sh)
                    else: 
                        lis = []
                        for i in set(f.lower().split()):
                            for j in set(sh.lower().split()):
                                lis.append(similar(i,j))
                                for p in lis:
                                    if p >= 0.9:
    #                                     DF_FINAL=pd.read_excel(f,sheet_name= sh)
                                        s.append(sh)
                                        break
                                    else:
                                         pass
                DF_1 = pd.read_excel(f, s[0],header=None)
                DF_1[:1] = DF_1[:1].fillna(method='ffill', axis=1,limit=1)
                DF_1[:1] = DF_1[:1].fillna('C')
                DF_1[:2] = DF_1[:2].fillna('C')
                DF_1.columns = (DF_1.iloc[0] + '_' + DF_1.iloc[1])
                DF_2 = DF_1.iloc[2:].reset_index(drop=True)
                DF_2['sheet_name'] = s[0]
                for i in DF_2.columns:
                    x = str(i).lower().strip()
                    DF_2 = DF_2.rename(columns= {i:x}) 
            
                for i in DF_2.columns.tolist():
            
                    for k in CNames.itertuples(index=False):
            
                        for r in DF6.itertuples(index=False):
            
                            if str(k[1]) == str(r[2]):
                                try:
            
                                    for i in DF_2.columns.tolist():
            
                                        if str(i) == str(r[0]):
            
                                            DF_2.rename(columns={i:r[1]}, inplace=True)
                                except:
                                    pass
            
                for i in DF_2.columns.tolist():
                    for r in DF6.itertuples(index=False):
                        if str(r[2]) == "nan" :
            
                            if str(i) == str(r[0]):
                                DF_2.rename(columns={i:r[1]}, inplace=True)
            
                #Second sheet
                DF_3 = pd.read_excel(f, s[1],header=None)
                DF_3[:1] = DF_3[:1].fillna(method='ffill', axis=1,limit=1)
                DF_3[:1] = DF_3[:1].fillna('C')
                DF_3[:2] = DF_3[:2].fillna('C')
                DF_3.columns = (DF_3.iloc[0] + '_' + DF_3.iloc[1])
                DF_4 = DF_3.iloc[2:].reset_index(drop=True)
                DF_4['sheet_name'] = s[1]
                for i in DF_4.columns:
                    x = str(i).lower().strip()
                    DF_4=DF_4.rename(columns= {i:x}) 
            
                for i in DF_4.columns.tolist():
            
                    for k in CNames.itertuples(index=False):
            
                        for r in DF6.itertuples(index=False):
            
                            if str(k[1]) == str(r[2]):
                                try:
            
                                    for i in DF_4.columns.tolist():
            
                                        if str(i) == str(r[0]):
            
                                            DF_4.rename(columns={i:r[1]}, inplace=True)
                                except:
                                    pass
            
                for i in DF_4.columns.tolist():
                    for r in DF6.itertuples(index=False):
                        if str(r[2]) == "nan" :
            
                            if str(i) == str(r[0]):
                                DF_4.rename(columns={i:r[1]}, inplace=True)
                #Concatenating two sheets    
                frames=[DF_2,DF_4]
                result=pd.concat(frames)
                # try:
                #     result = result.drop("c_cdm code - description",axis = 1)
                # except:
                    # None
                result['Description_1'] = result['Cde n dscrpn'].str.split('-',1)
                
                result.dropna(subset = ["Description_1"], inplace=True)
            
            
                #result[['CPT/DRG', 'description']] = result['Description_1'].str.split('_\s+', n=1, expand=True)
                result[['CPT/DRG','description']] = pd.DataFrame(result['Description_1'].tolist(), index= result.index)
                #result.drop(["Description_1","Cde n dscrpn"], axis = 1, inplace = True)
                Search_List = list(DF8["Dropping columns"])
                #dropping columns
                result1= result.drop(columns=[col for col in result if col in Search_List])
                result1 = result1.drop(['Description_1'],axis=1)
                DF_FINAL=result1
                #DF_FINAL
            
                #Selecting -6th column from every file
                DF_10=DF_FINAL.iloc[:, [s]]    
                m=0
                c=0
                
                #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1
                        
                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 3" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL
                        
                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None 
            except Exception as e:
                print(e)
                pass
     
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
    
        
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
    
            
        df4=df3
        df4=df4[df4['cost'].notnull()]
    
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
            
    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat107_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data  


# In[23]:


################## wideFormat108 ###############
def wideFormat108(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str, header = None)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[1])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:


                            DF_FINAL.iloc[:c, :c] = np.nan

                            DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                            DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                            idx = DF_FINAL.index.get_loc(c)
                            DF_11 = DF_FINAL.iloc[idx - c :]
                            j = c+1

                            req_rows = np.where(DF_11.index == j)[0][0]
                            start = max(0, req_rows - j )
                            end = max(1, req_rows)
                            DF_12 = DF_11.iloc[start:end]

                            DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                            DF_12 = DF_12.to_frame()
                            DF_13 = DF_12.T

                            DF_11.drop(DF_11.head(j).index, inplace = True)
                            DF15 = DF_13.append(DF_11)
                            DF16 = DF15.reset_index(drop = True)
                            DF16.columns = DF16.iloc[0]
                            FINAL = DF16[1:]
                            FINAL['id']=k[1]

                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            DF_FINAL.iloc[:c, :2] = np.nan

                            DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                            DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                            idx = DF_FINAL.index.get_loc(c)
                            DF_11 = DF_FINAL.iloc[idx - c :]
                            j = c+1

                            req_rows = np.where(DF_11.index == j)[0][0]
                            start = max(0, req_rows - j )
                            end = max(1, req_rows)
                            DF_12 = DF_11.iloc[start:end]

                            DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                            DF_12 = DF_12.to_frame()
                            DF_13 = DF_12.T

                            DF_11.drop(DF_11.head(j).index, inplace = True)
                            DF15 = DF_13.append(DF_11)
                            DF16 = DF15.reset_index(drop = True)
                            DF16.columns = DF16.iloc[0]
                            FINAL = DF16[1:]

                            FINAL['id']=k[1]

                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_FINAL.iloc[:c, :2] = np.nan

                        DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                        DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                        idx = DF_FINAL.index.get_loc(c)
                        DF_11 = DF_FINAL.iloc[idx - c :]
                        j = c+1

                        req_rows = np.where(DF_11.index == j)[0][0]
                        start = max(0, req_rows - j )
                        end = max(1, req_rows)
                        DF_12 = DF_11.iloc[start:end]

                        DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                        DF_12 = DF_12.to_frame()
                        DF_13 = DF_12.T

                        DF_11.drop(DF_11.head(j).index, inplace = True)
                        DF15 = DF_13.append(DF_11)
                        DF16 = DF15.reset_index(drop = True)
                        DF16.columns = DF16.iloc[0]
                        FINAL = DF16[1:]
                        FINAL['id']=k[1]




                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None

            except Exception as e:
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        #for r in DF6.itertuples(index=False):
            #FINAL.rename(columns={r[0]:r[1]}, inplace=True)
            #Search_List = list(DF8["Dropping columns"])
            #dropping columns
            #FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data = Combined_data.drop_duplicates()      
    Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat108_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[24]:


################# wideFormat11 ###############
def wideFormat11(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []


    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0


                    for index, row in DF_10.iterrows():


                        if (pd.notnull(row[0])):                       

                            m=m+1
                            print(m)
                            break
                        else:
                            c=c+1


                    if c==0:

                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                        else:

                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)



                    elif c>=1:

                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})

                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                                pass

                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)


                    else:
                        None


            except Exception as e:
                print(e)
                pass
        try:

            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^Unnamed')]
        except:
            pass

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        df2=FINAL
        df2 = df_column_uniquify(df2)
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')

        df4=df3
        df4['cost'] = np.where((df4['cost'] ==' ') , np.nan, df4['cost'])
        df5=df4[df4['cost'].notnull()]

        df5
        Combined_data.append(df5)
    Combined_data = pd.concat(Combined_data)

    Combined_data

    Sample_output= Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat11_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[25]:


################# wideFormat111 ##############
def wideFormat111(CNames):
    global df7
    
    combined_df = []
    Combined_data = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 1" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        DF_11=DF_FINAL.iloc[c:]
                        #This Case works when description is more than 1 row
                        #DF_11=DF_FINAL.iloc[C:]
                        list1=['CDM']
                        DF_11["ROW"] = DF_11.iloc[:,0].isin(list1)
                        n =0
                        for index, row in DF_11['ROW'].iteritems():
                            if row == True:
                                n += 1
                                DF_11['ROW'][index] = n
                            else:   
                                DF_11['ROW'][index] = np.nan
                        DF_11['ROW'].fillna(method='bfill', inplace=True, limit = 1)
                        DF_11['ROW'].fillna(method='ffill', inplace=True)
                        List_ids=[]
                        for i in DF_11['ROW']:
                            List_ids.append(i)

                            List_ids=list(set(List_ids))
                        list2 = pd.DataFrame()
                        for i in List_ids:
                            list2 = DF_11[DF_11['ROW']==i]
                            list2 = list2.drop(['ROW'],axis=1)
                            list2 = list2.dropna(how='all',axis=1)
                            list2.iloc[:2] = list2.iloc[:2].fillna(' ')
                            list2.columns = list2.iloc[0] + ' ' + list2.iloc[1]
                            list2 = list2.iloc[2:]
                            list2 = list2.reset_index(drop=True)
                            list2['id']=k[1]
                            FINAL = list2
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x}) 

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_df.append(FINAL)

                    else:
                        None 
            except Exception as e:
                print(e)
                pass

        FINAL = pd.concat(combined_df) 
        # FINAL.to_csv('Wideformat4(825).csv',index=False)

        FINAL['Charges1'] = FINAL['Charges']

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '    

        df2 = FINAL
        df2 = df_column_uniquify(df2)
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        column_list = ['cost1']
        for col in column_list:
            if col not in df3.columns:
                df3[col] = ''

        df3=df3[df3['cost'].notnull()]

        list1=['Average', 'Charge', 'Average charge', '0', 'Not offered']
        df3=df3[~(df3["cost"].isin(list1))]

        try:    
            df3["cost11"] = df3['cost'].str.contains('\d+%')
            df3["cost1"]= np.where((df3["cost11"].apply(lambda x:x==True)), df3['cost'].apply(lambda x: x.split("%", 1)[0]),df3["cost1"])
            df3 = df3.drop('cost11',axis = 1)



            df3[pd.to_numeric(df3.Charges1, errors='coerce').isnull()]

            df3['cost1'] = pd.to_numeric(df3['cost1'])

            df3["cost1"]= np.where(pd.notnull(df3["cost1"]), df3['cost1'].div(100).round(3),df3["cost1"])

            df3["Charges1"]= np.where((df3["Charges1"].str.contains(",")), df3["Charges1"].str.replace(',',''),df3["Charges1"])

            cols=[i for i in df3.columns if i in ['cost1', 'Charges1']]

            for col in cols:
                df3[col] = pd.to_numeric(df3[col], errors='coerce')

            df3["cost1"]= np.where(pd.notnull(df3["cost1"]), df3["Charges1"] * df3["cost1"],df3["cost1"])

            df3["cost1"].fillna(df3["cost"], inplace=True)
            df3 = df3.drop(['cost','Charges1'],axis=1)
            df4=df3.rename(columns = {'cost1': "cost"}) 
        except:
            None


        df4=df4[df4['name'].notnull()]

        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data

    Sample_output = Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat111_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[26]:


################### wideFormat117 #####################
def wideFormat117(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    def similar(x1, x2):
        return SequenceMatcher(None, x1, x2).ratio()
    for k in CNames.itertuples(index=False):
        f=k[0] 

        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f, sheet_name = None,dtype=str)
            for sh in sheet_to.keys():

                    if len(set(sh.lower().split()) & set(f.lower().split())) != 0:
                        DF_FINAL=pd.read_excel(f,sheet_name= sh)
                        print(sh)
                    else:
                        lis = []
                        for i in set(f.lower().split()):
                            for j in set(sh.lower().split()):
                                lis.append(similar(i,j))
                                for p in lis:
                                    if p >= 0.9:
                                        DF_FINAL=pd.read_excel(f,sheet_name= sh)
                                        print(DF_FINAL)
                                        break
                                    else:
                                        pass
            non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        count = 0
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows..3
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 1" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        #print(sh)
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        FINAL = FINAL.reset_index(drop=True)
                        # FINAL['sheet_name'] = sh
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})    


                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass

                        for i in FINAL.columns.tolist():


                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)

                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        FINAL = FINAL.reset_index(drop=True)
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})    


                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass

                        for i in FINAL.columns.tolist():


                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)



                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #print(sh)
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                    FINAL.dropna(how='all', axis=1, inplace=True)
                    FINAL = FINAL.reset_index(drop=True)

                    # for FINAL in FINAL.columns:
                    #     # FINAL['sheet_name'] = sh
                    #     FINAL['sheet_name'] = sh      # this adds `sheet_name` into the column `Week`
                    # combined_final = combined_final.append(FINAL)
                    for i in FINAL.columns:
                        x = str(i).lower().strip()
                        FINAL=FINAL.rename(columns= {i:x})    


                    for i in FINAL.columns.tolist():

                        for k in CNames.itertuples(index=False):

                            for r in DF6.itertuples(index=False):

                                if str(k[1]) == str(r[2]):
                                    try:

                                        for i in FINAL.columns.tolist():

                                            if str(i) == str(r[0]):

                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass

                    for i in FINAL.columns.tolist():


                        for r in DF6.itertuples(index=False):



                            if str(r[2]) == "nan" :

                                if str(i) == str(r[0]):
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                    combined_final.append(FINAL)

                    count += 1
                else:
                    count += 1
                    None

            except Exception as e:
                print(e)
                pass

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data

    Combined_data = Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Sample_output = Combined_data       

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat117_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[27]:


################# wideFormat118 ##################
def wideFormat118(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str,header=None)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[5])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            FINAL = DF_FINAL
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            DF_FINAL.iloc[:c, :c] = np.nan

                            DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                            DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                            idx = DF_FINAL.index.get_loc(c)
                            DF_11 = DF_FINAL.iloc[idx - c :]
                            j = c+1

                            req_rows = np.where(DF_11.index == j)[0][0]
                            start = max(0, req_rows - j )
                            end = max(1, req_rows)
                            DF_12 = DF_11.iloc[start:end]

                            DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                            DF_12 = DF_12.to_frame()
                            DF_13 = DF_12.T

                            DF_11.drop(DF_11.head(j).index, inplace = True)
                            DF15 = DF_13.append(DF_11)
                            DF16 = DF15.reset_index(drop = True)
                            DF16.columns = DF16.iloc[0]
                            FINAL = DF16[1:]
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_FINAL.iloc[:c, :c] = np.nan

                        DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                        DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                        idx = DF_FINAL.index.get_loc(c)
                        DF_11 = DF_FINAL.iloc[idx - c :]
                        j = c+1

                        req_rows = np.where(DF_11.index == j)[0][0]
                        start = max(0, req_rows - j )
                        end = max(1, req_rows)
                        DF_12 = DF_11.iloc[start:end]

                        DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                        DF_12 = DF_12.to_frame()
                        DF_13 = DF_12.T

                        DF_11.drop(DF_11.head(j).index, inplace = True)
                        DF15 = DF_13.append(DF_11)
                        DF16 = DF15.reset_index(drop = True)
                        DF16.columns = DF16.iloc[0]
                        FINAL = DF16[1:]
                        FINAL['id']=k[1]



                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None

            except Exception as e:
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass






        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat118_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[28]:


#################### wideFormat128 ####################
def wideFormat128(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f, header = None)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1',  header = None)


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,  header = None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str,  header = None)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[1])): 
                        m=m+1
                        break
                    else:
                        c=c+1


                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            DF_FINAL.iloc[:c, :c] = np.nan

                            DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                            DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                            idx = DF_FINAL.index.get_loc(c)
                            DF_11 = DF_FINAL.iloc[idx - c :]
                            j = c+1

                            req_rows = np.where(DF_11.index == j)[0][0]
                            start = max(0, req_rows - j )
                            end = max(1, req_rows)
                            DF_12 = DF_11.iloc[start:end]

                            DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                            DF_12 = DF_12.to_frame()
                            DF_13 = DF_12.T

                            DF_11.drop(DF_11.head(j).index, inplace = True)
                            DF15 = DF_13.append(DF_11)
                            DF16 = DF15.reset_index(drop = True)
                            DF16.columns = DF16.iloc[0]
                            FINAL = DF16[1:]
                            FINAL['id']=k[1]
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})




                            for k in CNames.itertuples(index=False):
                                for r in DF6.itertuples(index=False):
                                    if str(k[1]) == str(r[2]):
                                        try:
                                            for i in FINAL.columns.tolist():
                                                if str(i) == str(r[0]):
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                            for i in FINAL.columns.tolist():

                                for r in DF6.itertuples(index=False):



                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_FINAL.iloc[:c, :c] = np.nan

                        DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                        DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                        idx = DF_FINAL.index.get_loc(c)
                        DF_11 = DF_FINAL.iloc[idx - c :]
                        j = c+1

                        req_rows = np.where(DF_11.index == j)[0][0]
                        start = max(0, req_rows - j )
                        end = max(1, req_rows)
                        DF_12 = DF_11.iloc[start:end]

                        DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                        DF_12 = DF_12.to_frame()
                        DF_13 = DF_12.T

                        DF_11.drop(DF_11.head(j).index, inplace = True)
                        DF15 = DF_13.append(DF_11)
                        DF16 = DF15.reset_index(drop = True)
                        DF16.columns = DF16.iloc[0]
                        FINAL = DF16[1:]



                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})




                        for k in CNames.itertuples(index=False):
                            for r in DF6.itertuples(index=False):
                                if str(k[1]) == str(r[2]):
                                    try:
                                        for i in FINAL.columns.tolist():
                                            if str(i) == str(r[0]):
                                                FINAL.rename(columns={i:r[1]}, inplace=True)
                                    except:
                                        pass
                        for i in FINAL.columns.tolist():

                            for r in DF6.itertuples(index=False):



                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None

            except Exception as e:
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        try:
            FINAL = FINAL.loc[:, FINAL.columns.notnull()]
        except:
            pass

        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass






        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' ' 





        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat128_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[29]:


################### wideFormat12 #####################
def wideFormat12(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1





                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 3" in DF_10:

                        # DF_FINAL.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in DF_FINAL.columns.values]).ffill().values.flatten()
                        DF_11=DF_FINAL.iloc[c+1:]
                        DF_11[:c]=DF_11[:c].fillna(method='ffill', axis=1,limit=1)
                        DF_11[:c-1] = DF_11[:c-1].fillna(' ')

                        DF_FINAL.columns = (DF_FINAL.iloc[c+1] + ' ' + DF_FINAL.iloc[c+2])
                        #DF_FINAL.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in DF_FINAL.columns.values]).ffill().values.flatten()
                        DF_FINAL = DF_FINAL.iloc[c+3:]
                        # DF_FINAL=DF_FINAL.dropna()
                        #row values as column names
                        #Dropping the row
                        FINAL = DF_FINAL.reset_index(drop=True)
                        FINAL['id']=k[1]
                        FINAL

                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL



                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c-1:]
                    DF_11[:c-1]=DF_11[:c-1].fillna(method='ffill', axis=1)
                    DF_11[:c-1] = DF_11[:c-1].fillna(' ')
                    DF_FINAL.columns = (DF_FINAL.iloc[c-1] + ' ' + DF_FINAL.iloc[c])
                    DF_FINAL = DF_FINAL.iloc[c+2:]
                    #row values as column names
                    #Dropping the row
                    FINAL = DF_FINAL.reset_index(drop=True)
                    FINAL['id']=k[1]
                    FINAL
                else:
                    None 

            except Exception as e:
                print(e)
                pass

        for i in FINAL.columns:
                x = str(i).lower().strip()
                FINAL=FINAL.rename(columns= {i:x})


        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(i) == str(r[0]):
                    FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])
    #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL
        df2 = df_column_uniquify(df2)

        try:
            df2 = df2[df2["billing_code"].str.contains("2020 CPT/HCPCS")==False]
            df2=df2[df2['billing_code'].notnull()]
        except:
            pass
        df2["lineitem_cnt"] = df2.groupby(["billing_code"])["billing_code"].transform('count')
        df2["lineitem_cnt"]=df2["lineitem_cnt"]-1
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        df3 = df3.replace(r'^\s*$', np.NaN, regex=True)
        df3=df3[df3['name'].notnull()]#df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None

        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]


        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])

    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)

    # Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains('ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
    # Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])

    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat12_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[30]:


################# wideFormat131 #################
def wideFormat131(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]] ###### it is  6
                    m=0
                    c=0



                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row.iloc[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                    print(c)
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            combined_final.append(FINAL)
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            combined_final.append(FINAL)


                    elif c>=1:
                        # From c-1 idex the rows are considered 
                        # c-1 row is ffilled and c-1 and c rows are combined to form the column names of DF_11 dataframe
                        # Dropping 'c and c-1' rows
                        # sheets are appended to combined final
                        DF_FINAL = DF_FINAL.dropna(axis=1, how='all')
                        DF_11=DF_FINAL.iloc[c-1:]
                        DF_FINAL.iloc[c-1] = DF_FINAL.iloc[c-1].fillna(method='ffill')
                        DF_11.columns = DF_FINAL.iloc[c-1].astype('str')+'_'+DF_FINAL.iloc[c].astype('str')
                        DF_11 = DF_11.iloc[2:]
                        DF_11 = DF_11.dropna(axis=1, how='all')
                        #Dropping the row
                        FINAL=DF_11
                        FINAL['id']=k[1]
                        combined_final.append(FINAL)

                    else:
                        pass
            except Exception as e:
                print(e)
                pass
        FINAL = pd.concat(combined_final)

        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        #FINAL.drop(['hospital data_chargedept'], axis = 1, inplace = True)

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :


                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)





        Search_List = list(DF8["Dropping columns"])

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        df2=FINAL
        df2 = df_column_uniquify(df2)

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        #Required format - variable list

        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')   
        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['cpt_hcpcs_drg_aprdrg_icdx10'].notnull()]
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data

    Sample_output = Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat131_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[31]:


################## wideFormat132 ##################
def wideFormat132(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:

                    dummy = ""
                    try:
                        count = 0
                        for i in range(len(DF_FINAL)):
                            if DF_FINAL["Hospital Name"][i] == "Payer":
                                #print(sh)
                                count += 1
                                dummy = pd.DataFrame(DF_FINAL.iloc[count+1,:2])

                    except:
                        # dummy = ""
                        None
                    print(dummy)



                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]                                  
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            try:
                                FINAL['sheet_name'] = sh
                            except:
                                pass


                            FINAL.dropna(how='all', axis=1, inplace=True)
                            FINAL = FINAL.reset_index(drop=True)

                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                            # FINAL=FINAL.rename(columns = {'drug mnemonic': "CPT/HCPCS"}) 

                            for i in FINAL.columns:
                               x = str(i).lower().strip()
                               FINAL=FINAL.rename(columns= {i:x}) 

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            try:
                                FINAL['sheet_name'] = sh
                            except:
                                pass


                            FINAL.dropna(how='all', axis=1, inplace=True)
                            FINAL = FINAL.reset_index(drop=True)
                            try:
                                DF_FINAL[" ".join(dummy.values[0])] = " ".join(dummy.values[1])
                            except:
                                None
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})



                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :

                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)



                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL['sheet_name'] = sh
                        try:
                            FINAL['Payer'] = " ".join(dummy.values[1])
                        except:
                            None
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        FINAL = FINAL.reset_index(drop=True)
                        for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})



                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass

                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :

                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None 
            except Exception as e:
                print(e)
                pass



        FINAL = pd.concat(combined_final) 
        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])   

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        try:
            FINAL['payer_name']= FINAL['payer_name'].fillna('') ##filling empty spaces with na values
            FINAL["payer_name"]=FINAL["payer_name"].astype(str)
        except:
            pass
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        try:
            df4['name'] = df4[['payer_name', 'name']].apply(lambda x: ' '.join(x), axis=1)
            df4 = df4.drop('payer_name',axis =1)
            df4['name'] = df4['name'].str.strip()

        except:
            pass
        #df4['name'] = df4['name'].str.strip()
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data2

    Sample_output= Combined_data

    Sample_output = Sample_output.drop_duplicates()

    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('IP ')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('OP')) , 'Outpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('Inpatient ')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('Outpatient ')) , 'Outpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('IP/OP')) , 'Inpatient/Outpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'outpatient', Sample_output['inpatient-outpatient'])
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat132_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[32]:


################# wideFormat15 ######################
def wideFormat15(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]



        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 

            except Exception as e:
                print(e)
                pass

        #Checking the file type- csv



        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        # FINAL.drop(["CPT/HCPCS","DESCRIPTION"], axis = 1, inplace = True)
        #FINAL
        Search_List = list(DF8["Dropping columns"])
            #dropping columns

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])


        # for r in DF6.itertuples(index=False):
        #     FINAL.rename(columns={r[0]:r[1]}, inplace=True)
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        FINAL["LineItem_cnt"] = FINAL.groupby("cpt_hcpcs_drg_aprdrg_icdx10")["cpt_hcpcs_drg_aprdrg_icdx10"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1
        #FINAL['inpatient-outpatient'] = (FINAL['inpatient-outpatient'].str.strip().replace('',np.nan).groupby(FINAL['CODE LOOKUP']).transform(lambda x: x.bfill().ffill()))

        FINAL['inpatient-outpatient'] = (FINAL.groupby('cpt_hcpcs_drg_aprdrg_icdx10')['inpatient-outpatient'].transform(lambda x: x[x != ''].iat[0]))

        FINAL['gross charge'] = pd.to_numeric(FINAL['gross charge'], errors='coerce')
        FINAL['self_pay'] = pd.to_numeric(FINAL['self_pay'], errors='coerce')

        #FINAL=FINAL.groupby(['CPT/HCPCS','LineItem_cnt','description','inpatient-outpatient','id']).aggregate(['sum']).reset_index()
        FINAL=FINAL.groupby(['cpt_hcpcs_drg_aprdrg_icdx10','LineItem_cnt','description','inpatient-outpatient','hospital_Id']).aggregate(['sum']).reset_index()


        FINAL.columns = FINAL.columns.get_level_values(0)
        #FINAL



        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat15_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[33]:


######################## wideFormat15_1 #####################
def wideFormat15_1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]



        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 


            except Exception as e:
                print(e)
                pass


           # "CPT/HCPCS"
            # FINAL.drop(["#","Ancillary codes (CPT/Revenue)","Category/Location",], axis = 1, inplace = True)
            # FINAL



            # for r in DF6.itertuples(index=False):
            #     FINAL.rename(columns={r[0]:r[1]}, inplace=True)




            for i in FINAL.columns:
                x = str(i).lower().strip()
                FINAL=FINAL.rename(columns= {i:x})

            for i in FINAL.columns.tolist():

                for k in CNames.itertuples(index=False):

                    for r in DF6.itertuples(index=False):

                        if str(k[1]) == str(r[2]):
                            try:

                                for i in FINAL.columns.tolist():

                                    if str(i) == str(r[0]):

                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                            except:
                                    pass

            for i in FINAL.columns.tolist():
                for r in DF6.itertuples(index=False):
                    if str(r[2]) == "nan" :
                        if str(i) == str(r[0]):
                            FINAL.rename(columns={i:r[1]}, inplace=True)

            Search_List = list(DF8["Dropping columns"])
                #dropping columns

            FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
            #Checking the  Inpatient/Outpatient column exists or not


            column_list = ['inpatient_outpatient']
            for col in column_list:
                if col not in FINAL.columns:
                    FINAL[col] = ' '

            FINAL['description'].fillna(method='ffill', inplace=True)
            FINAL['cpt_drg'].fillna(method='ffill', inplace=True)
            FINAL['cms_requried'].fillna(method='ffill', inplace=True)

            all_columns = list(FINAL)
            #FINAL[all_columns] = FINAL[all_columns].replace('\$|,', '', regex=True)
            cols=[i for i in FINAL.columns if i not in ["cpt_drg","description",'inpatient_outpatient','hospital_Id','cms_requried']]

            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')

            FINAL["LineItem_cnt"] = FINAL.groupby(["description","cpt_drg"])["cpt_drg"].transform('count')
            FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1


            #FINAL['Gross Charge'] = pd.to_numeric(FINAL['Gross Charge'], errors='coerce')

            FINAL=FINAL.groupby(['cpt_drg','LineItem_cnt','description','inpatient_outpatient','hospital_Id','cms_requried']).aggregate(['sum']).reset_index()
            FINAL.columns = FINAL.columns.get_level_values(0)
            FINAL



            #Search_List = list(DF3["Keeping columns"])
            #Keeping columns
            #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
            #Removing the empty rows from'cpt code'
            #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

            #df2=FINAL[FINAL['cpt code'].notnull()] 

            df2=FINAL
            df2 = df_column_uniquify(df2)
            #Required format - variable list
            Col_list = list(DF5["Columns"])
            df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
            #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

            #df3['payer'] = None
            #df3['insurance Type'] = None
            #df3['Benefit Type']=None

            #Name_List = list(DF1["Payers"])

            #Name_List1 = list(DF2["Insurance Type"])

            #Name_List2 = list(DF3["Benefit Type"])

            #Finding Payers from the list
            #for i in Name_List:
            #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
            #Finding Insurancetype from the list    
            #for j in Name_List1:
            #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

            #Finding Benefit Type from the list  
            #for k in Name_List2:
            #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

            df4=df3
            df4=df4[df4['cost'].notnull()]
            #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
            #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
            df4
            Combined_data.append(df4)
        Combined_data = pd.concat(Combined_data)

        Combined_data
        output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat15_1_{}.csv"
        Combined_data.to_csv(output_path.format((k[1])), index=False)
        return Combined_data


# In[34]:


############################## wideFormat15_2 ############################
def wideFormat15_2(CNames):      
    global df7
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    
                    DF_FINAL=pd.read_excel(f,sheet_name=sh,header=None,skiprows=3)
                    DF_FINAL[:2] = DF_FINAL[:2].fillna(method='ffill', axis=1)
                    DF_FINAL[:2] = DF_FINAL[:2].fillna(' ')
                    DF_FINAL.columns = (DF_FINAL.iloc[0] + ' ' + DF_FINAL.iloc[1]+' '+DF_FINAL.iloc[2])
                    DF_FINAL = DF_FINAL.iloc[3:]
                    FINAL = DF_FINAL.reset_index(drop=True)
                    FINAL['id']=k[1]
    
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                    
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                            
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL
                            
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            
            except Exception as e:
                print(e)
                pass
    
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})    
        
       
        
        for i in FINAL.columns.tolist():
        
            for k in CNames.itertuples(index=False):
        
                for r in DF6.itertuples(index=False):
        
                    if str(k[1]) == str(r[2]):
                        try:
        
                            for i in FINAL.columns.tolist():
        
                                if str(i) == str(r[0]):
        
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass
        
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)
                       
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
             
    
    
        FINAL['CPT/HCPCS/DRG'].fillna(method='ffill', inplace=True)
        FINAL['Shoppable Service Category'].fillna(method='ffill', inplace=True)
    
        if CNames['id'].isin(['2902']).any():
            try:
                FINAL = FINAL.replace(('May be billed separately'), ' ', regex=True)
                FINAL['inpatient-outpatient'] = FINAL['inpatient-outpatient'].replace(' ', np.nan, regex=True)
                FINAL['inpatient-outpatient'].fillna(method='ffill', inplace=True)
            except:
                   None
        FINAL['inpatient-outpatient'].fillna(method='ffill', inplace=True)
    
        all_columns = list(FINAL)
        #FINAL[all_columns] = FINAL[all_columns].replace('\$|,', '', regex=True)
        cols=[i for i in FINAL.columns if i not in ["CPT/HCPCS/DRG","Primary Service and Ancillary Service",'Hospital_Id','Shoppable Service Category', 'inpatient-outpatient']]
        
        for col in cols:
            FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
        
        FINAL = FINAL.drop_duplicates(keep='first', inplace=False)
    
        
        FINAL["LineItem_cnt"] = FINAL.groupby(["CPT/HCPCS/DRG","inpatient-outpatient"])["CPT/HCPCS/DRG"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1
        
        if CNames['id'].isin(['2902']).any():
            try:
                FINAL.drop(['Primary Service and Ancillary Service'],axis=1,inplace=True)
            except:
                None
                    
    
        #FINAL['Gross Charge'] = pd.to_numeric(FINAL['Gross Charge'], errors='coerce')
    
        FINAL=FINAL.groupby(['CPT/HCPCS/DRG','LineItem_cnt','Shoppable Service Category','inpatient-outpatient','Hospital_Id']).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)
        FINAL
        
    
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        
            
        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat15_2{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[35]:


############################# wideFormat21 ######################
def wideFormat21(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1
                if c==0:

                      #This Case works when description is in one row
                    if "Unnamed: 5" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None 

            except Exception as e:
                print(e)
                pass






        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)



        FINAL['cpt'] = np.where((FINAL['description_1'].notnull()) & (FINAL['description'].notnull()) &(FINAL['cpt_hcpcs'].notnull()), FINAL['cpt_hcpcs'], "")
        FINAL= FINAL.replace(r'^\s*$', np.NaN, regex=True)
        FINAL['description_1'].fillna(method='ffill', inplace=True)
        FINAL['cpt'].fillna(method='ffill', inplace=True)
        FINAL['LineItem_cnt'] = None 
        FINAL['LineItem_cnt'] = FINAL.groupby(['description_1','cpt'])["cpt"].transform("count")
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1

        all_columns = list(FINAL)
        FINAL[all_columns] = FINAL[all_columns].replace(r'^\s*$', '', regex=True)


        cols=[i for i in FINAL.columns if i not in ["description_1",'cpt','inpatient-outpatient','hospital_Id']]
        for col in cols:
            FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')
        # FINAL.drop(["Description",'Code'], axis = 1, inplace = True)
        FINAL=FINAL.groupby(['description_1','LineItem_cnt','cpt','hospital_Id']).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)




        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])


        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '



        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    # output_path = "D:" + os.path.sep + "Zigna AI Corp" + os.path.sep + "Zigna AI Corp - Hospital Application_2022-03-09" + os.path.sep + "Automation_task"  + os.path.sep + "outputs" + os.path.sep + "Wideformat21_{}.csv"
    # Combined_data.to_csv(output_path.format((k[1])), index=False)
    # return Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat21_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[36]:


######################### wideFormat23 ###########################
def wideFormat23(CNames):
    global df7      
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_FINAL1= pd.concat(pd.read_excel(f,sheet_name=None))
                    DF_FINAL1.reset_index(level=0, inplace=True)
                    DF_FINAL2 = DF_FINAL1.loc[:, ~DF_FINAL1.columns.str.contains('^Unnamed')]
                    DF_FINAL=DF_FINAL2[~(DF_FINAL2["level_0"]=='Discounted Cash Price')]
                    DF_FINAL.drop(["level_0"], axis = 1, inplace = True)
                    DF_FINAL
                    
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL
                
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass
        for i in FINAL.columns:
                x = str(i).lower().strip()
                FINAL=FINAL.rename(columns= {i:x})
    
        
    
        FINAL.loc[:, FINAL.columns.notnull()]
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(i) == str(r[0]):
                    FINAL.rename(columns={i:r[1]}, inplace=True)
        
        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
    
       
    
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
    
       
        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    
    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    Combined_data.dtypes
    
    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains('outpatient')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains('inpatient')) , 'Inpatient', Combined_data['inpatient-outpatient'])
    
    #Dropping the columns
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat23_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[37]:


############################## wideFormat35 #######################
def wideFormat35(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1

                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 2" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                   #This Case works when description is more than 1 row
                    #Dropping 'c' rows

                    DF_11=DF_FINAL.iloc[c-1:]
                    DF_11[:c-1]=DF_11[:c-1].fillna(method='ffill', axis=1)
                    DF_11[:c-1] = DF_11[:c-1].fillna(' ')
                    DF_FINAL.columns = (DF_FINAL.iloc[c-1] + ' ' + DF_FINAL.iloc[c])
                    DF_FINAL = DF_FINAL.iloc[c+2:]
                    #row values as column names
                    #Dropping the row
                    FINAL = DF_FINAL.reset_index(drop=True)
                    FINAL['id']=k[1]
                else:
                    None 

        #Checking the file type- csv
            except Exception as e:
                print(e)
                pass
        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        # if CNames['id'].isin(['3502']).any():
        #         try:
        #             FINAL.drop(['  COUNT','(see NOTES tab) CMS 70'],axis=1,inplace=True)
        #         except:
        #             None

           #FINAL
        Search_List = list(DF8["Dropping columns"])
            #dropping columns

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])



        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '        

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data = Combined_data.drop_duplicates()

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat35_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[38]:


############################### wideFormat41 #######################
def wideFormat41(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    def similar(x1, x2):
        return SequenceMatcher(None, x1, x2).ratio()
    for k in CNames.itertuples(index=False):
        f=k[0] 

        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f, sheet_name = None,dtype=str)
            for sh in sheet_to.keys():
                #print(sh)
                a = sh.strip()
                a = a.replace('.','')
                #print(sh)
                if CNames.FILE_NAMES.str.contains(a).any():
                    #sh = sh.replace('.','')
                    print(sh)
                    DF_FINAL = pd.read_excel(f,sheet_name=sh)
            non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows..3
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 0" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]

                    else:
                        None 

            except Exception as e:
                print(e)
                pass
        FINAL = FINAL.loc[:, FINAL.columns.notnull()]    
        FINAL['sheet_name'] = sh
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})    


        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                            pass

        for i in FINAL.columns.tolist():


            for r in DF6.itertuples(index=False):



                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)



        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Sample_output = Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat41_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    Sample_output


# In[39]:


#################### wideFormat43 ###########################
def wideFormat43(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0


                #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1

                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 5" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None 
        #Checking the file type- csv

            except Exception as e:
                print(e)
                pass

        FINAL1=FINAL.loc[:, FINAL.columns.notnull()]
        FINAL1.dropna()
        # FINAL1=FINAL1[FINAL1['description'].notnull()]

        for i in FINAL1.columns:

            x = str(i).lower().strip()
            FINAL1=FINAL1.rename(columns= {i:x})

        for i in FINAL1.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL1.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL1.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL1.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL1.rename(columns={i:r[1]}, inplace=True)

         # FINAL.drop(["CPT/HCPCS","DESCRIPTION"], axis = 1, inplace = True)
         #FINAL
        Search_List = list(DF8["Dropping columns"])
            #dropping columns

        FINAL1= FINAL1.drop(columns=[col for col in FINAL1 if col in Search_List])
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL1.columns:
                FINAL1[col] = ' '

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL1
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    try:


        Combined_data = pd.concat(Combined_data)
    except:
        pass

    Combined_data

    Sample_output = Combined_data
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip rate')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('op rate')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' ip')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains(' op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat43_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    Sample_output


# In[40]:


############### wideFormat61 ############################
def wideFormat61(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1

                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 0" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None 

            except Exception as e:
                print(e)
                pass
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})
        FINAL=FINAL.T.drop_duplicates().T 


        try:    
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains('^unnamed')]
        except:
            None

        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains(' rate type')]
        except:
            None
        try:
            FINAL['gross charge1'] = FINAL['gross charges']
        except:
            None
        try:
            FINAL['charge1'] = FINAL['charge']
        except:
            None
        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)


        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
        df2=FINAL  
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        df3=df3[df3['cost'].notnull()]
        #df3['cost1'] = df3['cost'].str.contains('% of medicare opps',case = False)
        column_list = ['cost1']
        for col in column_list:
            if col not in df3.columns:
                df3[col] = ' '


        df3["cost11"] = df3['cost'].str.contains('\d+%')
        df3["cost1"]= np.where((df3["cost11"].apply(lambda x:x==True)), df3['cost'].apply(lambda x: x.split("%", 1)[0]),df3["cost1"])
        df3 = df3.drop('cost11',axis = 1)

        df3['cost1'] = df3['cost1'].str.extract('(\d+)')
        df3['cost1'] = pd.to_numeric(df3['cost1'])
        try:
            df3["gross charge1"]= np.where((df3["gross charge1"].str.contains(",")), df3["gross charge1"].str.replace(',',''),df3["charge1"])
        except:
            None
        try:
            df3["gross charge1"]= np.where((df3["gross charge1"].str.contains("$")), df3["gross charge1"].str.replace(',',''),df3["charge1"])
        except:
            None        


        try:
            df3['gross charge1'] = pd.to_numeric(df3['gross charge1'])
        except:
            None
        try: 
            df3["charge1"]= np.where((df3["charge1"].str.contains("$")), df3["charge1"].str.replace(',',''),df3["charge1"])
        except:
            None

        try: 
            df3["charge1"]= np.where((df3["charge1"].str.contains(",")), df3["charge1"].str.replace(',',''),df3["charge1"])
        except:
            None
        try:    
            df3['charge1'] = pd.to_numeric(df3['charge1'])
        except:
            None
        df3['cost1'] = df3['cost1'].div(100).round(3)

        df3['cost1'] = pd.to_numeric(df3['cost1'])

        try:
            df3["cost1"] = df3["gross charge1"] * df3["cost1"]
        except:
            None
        try:
            df3["cost1"] = df3["gross charges"] * df3["cost1"]
        except:
            None

        try:     
            df3["cost1"] = df3["charge1"] * df3["cost1"]
        except:
            None


        df3["cost1"].fillna(df3["cost"], inplace=True)

        try:
            df3 = df3.drop(['cost'],axis=1)
        except:
            None


        try :
            df3 = df3.drop(["gross charge1"], axis = 1)
        except:
            pass

        df4=df3.rename(columns = {'cost1': "cost"}) 


        df4=df4[df4['name'].notnull()]

        df4

        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data
    Sample_output = Combined_data


    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat61_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[41]:


##################### wideFormat76 #######################
def wideFormat76(CNames):
    global df7
    
        
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                    
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                            
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL
                            
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
                        
            except Exception as e:
                print(e)
                pass
    
            
       
        
        try:
            FINAL = FINAL.drop(["Location", "Per"], axis = 1)
        except:
            pass
        
        FINAL["LineItem_cnt"] = FINAL.groupby(["CPT","ProcedureDescription"])["CPT"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1
    
    
        
        for i in FINAL.columns:
           x = str(i).lower().strip()
           FINAL=FINAL.rename(columns= {i:x}) 
    
        for i in FINAL.columns.tolist():
     
            for k in CNames.itertuples(index=False):
     
                for r in DF6.itertuples(index=False):
     
                    if str(k[1]) == str(r[2]):
                        try:
     
                            for i in FINAL.columns.tolist():
     
                                if str(i) == str(r[0]):
     
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                            pass
     
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
    
                   if str(i) == str(r[0]):
                       FINAL.rename(columns={i:r[1]}, inplace=True)
                       
        Search_List = list(DF8["Dropping columns"])
            #dropping columns
    
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
          
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
                
    
        
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        df3=df3[df3['name'].notnull()] 
        
        
        df4=df3
        df4=df4[df4['cost'].notnull()]
        
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat76_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[42]:


############## wideFormat77 ####################
def wideFormat77(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                    #print(sh)
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            #print(sh)
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', inplace=True)
                            FINAL = FINAL.reset_index(drop=True)

                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', inplace=True)
                            FINAL = FINAL.reset_index(drop=True)

                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)



                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        DF_11[:c]=DF_11[:c].fillna(method='ffill', axis=1)
                        DF_11[:c] = DF_11[:c].fillna(' ')

                        if sh == "Magee Physicians Group":
                            try:
                                DF_FINAL.columns = (DF_FINAL.iloc[c] +' '+ DF_FINAL.iloc[c+1] +' '+ DF_FINAL.iloc[c+2])
                                DF_FINAL = DF_FINAL.iloc[c+3:]  
                            except:
                                None
                        if sh == "Magee Rehabilitation Hospital":
                            try:
                                DF_FINAL.columns = (DF_FINAL.iloc[c] +' '+ DF_FINAL.iloc[c+1])
                                DF_FINAL = DF_FINAL.iloc[c+2:]
                            except:
                                None
                    #Dropping the row
                        FINAL = DF_FINAL.reset_index(drop=True)
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', inplace=True)
                        FINAL = FINAL.reset_index(drop=True)
                        # FINAL.columns.str.lower().strip()
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})
                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(i) == str(r[0]):
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)



                    else:
                        None
            except Exception as e:
                print(e)
                pass



        FINAL = pd.concat(combined_final) 

        Search_List = list(DF8["Dropping columns"])
             #dropping columns

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])



        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]

        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat77_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[43]:


################### wideFormat79 ###################
def wideFormat79(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_FINAL=pd.read_excel(f,sheet_name=sh,header=None)
                    DF_FINAL[:1] = DF_FINAL[:1].fillna(method='ffill', axis=1)
                    DF_FINAL[:1] = DF_FINAL[:1].fillna('')
                    DF_FINAL[:2] = DF_FINAL[:2].fillna('')
                    DF_FINAL.columns = (DF_FINAL.iloc[0] + ' ' +DF_FINAL.iloc[1])
                    DF_FINAL = DF_FINAL.iloc[2:]      


                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 

            except Exception as e:
                print(e)
                pass


        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(i) == str(r[0]):
                    FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        FINAL.loc[:, FINAL.columns.notnull()]

        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '



        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat79_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[44]:


########################## wideFormat7_1 #######################
def wideFormat7_1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass



        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(i) == str(r[0]):
                    FINAL.rename(columns={i:r[1]}, inplace=True)
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '



        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    # Combined_data["id"]=Combined_data.id.astype('int64')
    # Combined_data.dtypes
    Sample_output = Combined_data



    try:
        Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['Primary Service and Ancillary Service'].str.contains('Outpatient')) , 'Outpatient', Sample_output['inpatient-outpatient'])
        Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['Primary Service and Ancillary Service'].str.contains('Inpatient')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    except:
        pass
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat7_1_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[45]:


################## wideFormat91 #################
def wideFormat91(CNames):
    global df7     
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    try:
                        #df=pd.read_csv(f)
                        DF_FINAL= pd.concat(pd.read_csv(f,sheet_name=None,dtype=str))
                        DF_FINAL.reset_index(level=0, inplace=True)
                        #DF6=pd.read_excel ('D:\\zigna Analytics\\Wide format 4\\Payers list v.xlsx',sheet_name='Rnme')
                        for i in DF_FINAL.columns:
                            x = str(i).lower().strip()
                            DF_FINAL=DF_FINAL.rename(columns= {i:x})
                        for i in DF_FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                                    if str(i) == str(r[0]):
                                        DF_FINAL.rename(columns={i:r[1]}, inplace=True)
                    #df["description"] = df["description"].str.upper()
                        def sjoin(x): return ';'.join(x[x.notnull()].astype(str))
            
                        DF_FINAL = DF_FINAL.groupby(level=0, axis=1).apply(lambda x: x.apply(sjoin, axis=1))
                        DF_FINAL.drop(["inpatient-outpatient"], axis = 1, inplace = True)
                    except:
                        #df=pd.read_csv(f,encoding='latin1')
                        DF_FINAL= pd.concat(pd.read_csv(f,sheet_name=None,encoding='latin1',dtype=str))
                        DF_FINAL.reset_index(level=0, inplace=True)
                        #DF6=pd.read_excel ('D:\\zigna Analytics\\Wide format 4\\Payers list v.xlsx',sheet_name='Rnme')
                        for i in DF_FINAL.columns:
                            x = str(i).lower().strip()
                            DF_FINAL=DF_FINAL.rename(columns= {i:x})
                        for i in DF_FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                                    if str(i) == str(r[0]):
                                        DF_FINAL.rename(columns={i:r[1]}, inplace=True)
                    #df["description"] = df["description"].str.upper()
                        def sjoin(x): return ';'.join(x[x.notnull()].astype(str))
            
                        DF_FINAL = DF_FINAL.groupby(level=0, axis=1).apply(lambda x: x.apply(sjoin, axis=1))
                        DF_FINAL.drop(["inpatient-outpatient"], axis = 1, inplace = True)
            
                    if 'Unnamed: 0' not in DF_FINAL.iloc[:, [0]]:
                        if 'Primary Service and Ancillary Services' not in DF_FINAL.iloc[:, [2]]:
                            
            
                            DF_13=DF_FINAL.iloc[:, [s]]
                            M=0
                            C=0   
            
                            for index, row in DF_13.iterrows():
                                if(pd.notnull(row[0])):
                                    M=M+1
                                    break
                                else:
                                    C=C+1
            
                            if C==0:
                                #This Case works when description is in one row
                                if "Unnamed: 5" in DF_13:
                                    DF_FINAL.columns=DF_FINAL.iloc[0]
                                    FINAL=DF_FINAL.drop([C])
                                    FINAL['Hospital_Id']=k[1]
                                    FINAL 
            
                                else:
                                    #This Case works when description is not found
                                    FINAL=DF_FINAL
                                    FINAL['Hospital_Id']=k[1]
                                    FINAL
                            elif C>=1:
                                #This Case works when description is more than 1 row
                                #Dropping 'c' rows
                                DF_14=DF_FINAL.iloc[C:]
                                #row values as column names
                                DF_14.columns=DF_14.iloc[0]
                                #Dropping the row
                                FINAL=DF_14.drop([C])
                                FINAL['Hospital_Id']=k[1] 
                                FINAL 
                            else:
                                None
            except Exception as e:
                print(e)
                pass
    
            
             
                    
    
    
        #for r in DF6.itertuples(index=False):
            #FINAL.rename(columns={r[0]:r[1]}, inplace=True)
        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
    
        
    
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
    
      
        df4=df3
        df4=df4[df4['cost'].notnull()]
    
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    
    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat91_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[46]:


################# wideFormat92 #################
def wideFormat92(CNames):
    global df7       
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
    
    
    
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
    
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
    
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                         
                            for i in FINAL.columns.tolist():
                         
                                for k in CNames.itertuples(index=False):
                         
                                    for r in DF6.itertuples(index=False):
                         
                                        if str(k[1]) == str(r[2]):
                                            try:
                         
                                                for i in FINAL.columns.tolist():
                         
                                                    if str(i) == str(r[0]):
                         
                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass
                         
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
    
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                         
                            for i in FINAL.columns.tolist():
                         
                                for k in CNames.itertuples(index=False):
                         
                                    for r in DF6.itertuples(index=False):
                         
                                        if str(k[1]) == str(r[2]):
                                            try:
                         
                                                for i in FINAL.columns.tolist():
                         
                                                    if str(i) == str(r[0]):
                         
                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass
                         
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
    
    
    
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})
                     
                        for i in FINAL.columns.tolist():
                     
                            for k in CNames.itertuples(index=False):
                     
                                for r in DF6.itertuples(index=False):
                     
                                    if str(k[1]) == str(r[2]):
                                        try:
                     
                                            for i in FINAL.columns.tolist():
                     
                                                if str(i) == str(r[0]):
                     
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                                pass
                     
                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None 
        
            except Exception as e:
                print(e)
                pass
    
      
        
        FINAL = pd.concat(combined_final) 
        
            #Search_List = list(DF8["Dropping columns"])
            #dropping columns
            #FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
        
        
        #df2=FINAL[FINAL['cpt code'].notnull()] 
        
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
     
            
        df4=df3
        df4=df4[df4['cost'].notnull()]
       
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
            
    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    Combined_data.dtypes
    
    
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat92_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[47]:


################ wideFormat93 ##################
def wideFormat93(CNames):
    global df7       
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
            
            #Selecting -6th column from every file
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL = FINAL.reset_index(drop=True)
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            try:
                                FINAL.drop(['Severity'],axis=1,inplace=True)
                            except:
                                pass
                            #for r in DF6.itertuples(index=False):
                            #    FINAL.rename(columns={r[0]:r[1]}, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})
                            for i in FINAL.columns.tolist():
                         
                                for k in CNames.itertuples(index=False):
                         
                                    for r in DF6.itertuples(index=False):
                         
                                        if str(k[1]) == str(r[2]):
                                            try:
                         
                                                for i in FINAL.columns.tolist():
                         
                                                    if str(i) == str(r[0]):
                         
                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass
                         
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                         
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL = FINAL.reset_index(drop=True)
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            try:
                                FINAL.drop(['Severity'],axis=1,inplace=True)
                            except:
                                pass
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x}) 
                        
                            for i in FINAL.columns.tolist():
                         
                                for k in CNames.itertuples(index=False):
                         
                                    for r in DF6.itertuples(index=False):
                         
                                        if str(k[1]) == str(r[2]):
                                            try:
                         
                                                for i in FINAL.columns.tolist():
                         
                                                    if str(i) == str(r[0]):
                         
                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                pass
                         
                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                         
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)
                 
                
                
                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL = FINAL.reset_index(drop=True)
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        try:
                            FINAL.drop(['Severity'],axis=1,inplace=True)
                        except:
                            pass
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x}) 
                    
                        for i in FINAL.columns.tolist():
                     
                            for k in CNames.itertuples(index=False):
                     
                                for r in DF6.itertuples(index=False):
                     
                                    if str(k[1]) == str(r[2]):
                                        try:
                     
                                            for i in FINAL.columns.tolist():
                     
                                                if str(i) == str(r[0]):
                     
                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                            pass
                     
                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                     
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)
                    else:
                        None 
                             
            except Exception as e:
                print(e)
                pass
    
    
        FINAL = pd.concat(combined_final)
        for i in FINAL.columns:
           x = str(i).lower().strip()
           FINAL=FINAL.rename(columns= {i:x}) 
    
        for i in FINAL.columns.tolist():
     
            for k in CNames.itertuples(index=False):
     
                for r in DF6.itertuples(index=False):
     
                    if str(k[1]) == str(r[2]):
                        try:
     
                            for i in FINAL.columns.tolist():
     
                                if str(i) == str(r[0]):
     
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                            pass
     
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
     
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)
         
        Search_List = list(DF8["Dropping columns"])
             #dropping columns
    
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        
    
      
    
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
    
        
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        
            
        df4=df3
        df4=df4[df4['cost'].notnull()]
       
        
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
            
    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat93_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data
    
    


# In[48]:


################### wideFormat97 #####################
def wideFormat97(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    def similar(x1, x2):
        return SequenceMatcher(None, x1, x2).ratio()
    for k in CNames.itertuples(index=False):
        f=k[0] 

        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f, sheet_name = None,dtype=str)
            for sh in sheet_to.keys():

                    if len(set(sh.lower().split()) & set(f.lower().split())) != 0:
                        DF_FINAL=pd.read_excel(f,sheet_name= sh)
                        print(sh)
                    else:
                        lis = []
                        for i in set(f.lower().split()):
                            for j in set(sh.lower().split()):
                                lis.append(similar(i,j))
                                for p in lis:
                                    if p >= 0.9:
                                        DF_FINAL=pd.read_excel(f,sheet_name= sh)
                                        print(DF_FINAL)
                                        break
                                    else:
                                        pass
            non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows..3
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 0" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]

                    else:
                        None 

            except Exception as e:
                print(e)
                pass
        FINAL = FINAL.loc[:, FINAL.columns.notnull()]    
        # FINAL['sheet_name'] = sh
        try:
            FINAL['Shoppable Service'].fillna(method='ffill', inplace=True)
        except:
            pass
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})    


        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                            pass

        for i in FINAL.columns.tolist():


            for r in DF6.itertuples(index=False):



                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)



        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Sample_output = Combined_data

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat97_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[49]:


###################### wideFormat98 ###################
def wideFormat98(CNames):
    global df7      
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')
                
            non_prep_data.append(DF_FINAL)
        
        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)
         
            for sh in sheet_to.keys():
               
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)
                
        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]
                
        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_FINAL[:1] = DF_FINAL[:1].fillna(method='ffill', axis=1)
                    DF_FINAL[:2] = DF_FINAL[:2].fillna(' ')
                    DF_FINAL.columns = (DF_FINAL.iloc[0] + ' ' + DF_FINAL.iloc[1])
                    DF_FINAL = DF_FINAL.iloc[2:]
    
                    FINAL = DF_FINAL.reset_index(drop=True)
    
                    FINAL['id']=k[1]  
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0
                    
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1
                            
                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 1" in DF_10:
                            DF_FINAL=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL
                            
                    elif c>=1:
                            #This Case works when description is more than 1 row
                            #Dropping 'c' rows
                            DF_11=DF_FINAL.iloc[c:]
                            #row values as column names
                            DF_11.columns=DF_11.iloc[0]
                            #Dropping the row
                            FINAL=DF_11.drop([c])
                            FINAL['id']=k[1]
                    else:
                        None 
                    
            except Exception as e:
                print(e)
                pass
    
    
        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x}) 
    
        for i in FINAL.columns.tolist():
    
            for k in CNames.itertuples(index=False):
    
                for r in DF6.itertuples(index=False):
    
                    if str(k[1]) == str(r[2]):
                        try:
    
                            for i in FINAL.columns.tolist():
    
                                if str(i) == str(r[0]):
    
                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                            pass
    
        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
    
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)   
        
        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])
        
        # for i in FINAL.columns.tolist():
        #     for r in DF6.itertuples(index=False):
        #         if str(i) == str(r[0]):
        #             FINAL.rename(columns={i:r[1]}, inplace=True)
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '
        
    
        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
            
        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
            
    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat98_{}.csv"
    Combined_data.to_csv(output_path.format((k[1])), index=False)
    return Combined_data


# In[50]:


#################### wideFormat13_1 ####################
def wideFormat13_1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:
                    DF_10=DF_FINAL.iloc[:, [s]]    
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        None
            except Exception as e:
                print(e)
                pass
        try:
            FINAL = pd.concat(combined_final)
        except:
            pass


        FINAL = FINAL.loc[:, FINAL.columns.notnull()]


        try:
            for col in FINAL.columns:
                if col =='unnamed: 0':
                    FINAL=FINAL.drop(["unnamed: 0"],axis=1)
                else:
                       None
        except:
            pass

        for i in FINAL.columns:
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})




        for k in CNames.itertuples(index=False):
            for r in DF6.itertuples(index=False):
                if str(k[1]) == str(r[2]):
                    try:
                        for i in FINAL.columns.tolist():
                            if str(i) == str(r[0]):
                                FINAL.rename(columns={i:r[1]}, inplace=True)
                    except:
                        pass
        for i in FINAL.columns.tolist():

            for r in DF6.itertuples(index=False):



                if str(r[2]) == "nan" :

                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        # Combining the payer, benfit and name columns. then we are dropping the unwanted  columns.

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '   


        new = FINAL['description'].str.split(" ",n=2,expand=True)
        # new = FINAL['description'].str.split(" ",n=2,expand=True)

        FINAL['Cpt Code'] = new[0]
        FINAL['HCPCS Code'] = new[1]

        FINAL['HCPCS Code'] = FINAL['HCPCS Code'].astype(str)

        FINAL["Cpt Code"] = np.where(FINAL['Cpt Code'].str.endswith('-') , FINAL['Cpt Code'].replace('-','', regex=True),FINAL['Cpt Code'])
        FINAL['Cpt Code'] = FINAL['Cpt Code'].str.strip()

        column_list = ['Cpt Code1','HCPCS Code1']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '  


        FINAL['Cpt Code1'] = pd.to_numeric(FINAL['Cpt Code'],errors="coerce")
        FINAL['HCPCS Code1'] = pd.to_numeric(FINAL['HCPCS Code'],errors="coerce")




        for index, row in FINAL.iterrows():
            if re.findall(r'^[A-Z]{1}[0-9]{4}',FINAL['Cpt Code'][index]):
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code'][index]
            else:
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code1'][index] 
        for index, row in FINAL.iterrows():
            if re.findall(r'^[A-Z]{1}[0-9]{4}',FINAL['HCPCS Code'][index]):
                FINAL['HCPCS Code1'][index] = FINAL['HCPCS Code'][index]
            else:
                FINAL['HCPCS Code1'][index] = FINAL['HCPCS Code1'][index]         
        for index, row in FINAL.iterrows():
            if re.findall(r'^[0-9]{5}',FINAL['Cpt Code'][index]):
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code'].str[:5][index]
            else:
                FINAL['Cpt Code1'][index] = FINAL['Cpt Code1'][index]


        FINAL['Cpt Code1'] = FINAL['Cpt Code1'].astype(str)
        FINAL['HCPCS Code1'] = FINAL['HCPCS Code1'].astype(str)
        FINAL['Cpt Code1']=FINAL['Cpt Code1'].replace('\.0', '', regex=True)
        FINAL['HCPCS Code1'] = FINAL['HCPCS Code1'].replace('\.0', '', regex=True)
        FINAL.drop(["Cpt Code"], axis = 1, inplace = True)
        FINAL.drop(["HCPCS Code"], axis = 1, inplace = True)
        FINAL=FINAL.rename(columns = {'Cpt Code1': "CPT"})
        FINAL=FINAL.rename(columns = {'HCPCS Code1': "HCPCS"})

        # FINAL['CPT'] = FINAL['CPT'].astype(str)
        # FINAL['CPT'].fillna(" ", inplace=True)
        FINAL['CPT'] = FINAL['CPT'].replace(r'nan',"")
        FINAL['HCPCS'] = FINAL['HCPCS'].replace(r'nan',"")



        # FINAL['HCPCS'].fillna(" ", inplace=True)
        FINAL['cpt_hcpcs'] = FINAL['CPT'] + FINAL['HCPCS']
        FINAL.drop(["CPT"], axis = 1, inplace = True)
        FINAL.drop(["HCPCS"], axis = 1, inplace = True)
        # FINAL["CPT/HCPCS"].replace(" ",np.nan,inplace=True)
        FINAL=FINAL[FINAL['cpt_hcpcs'].notnull()]

        FINAL['cpt_hcpcs'] = FINAL['cpt_hcpcs'].astype(str)


        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')



        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data
    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient/outpatient/drug')) , 'IP/OP', Sample_output['inpatient-outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip & op')) , 'IP & OP', Sample_output['inpatient_outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('ip/op')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient_outpatient'])
    # Sample_output['inpatient_outpatient'] = np.where((Sample_output['inpatient_outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient_outpatient'])
    # #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('ip')) , 'Inpatient', Sampl


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat13_1_{}.csv"
    Sample_output.to_csv(output_path.format((k[1])), index=False)
    return Sample_output


# In[51]:


###################### wideFormat101 ##################
def wideFormat101(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []


    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():           
                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str,header=None)
                DF_FINAL[:1] = DF_FINAL[:1].fillna('')
                DF_FINAL[:2] = DF_FINAL[:2].fillna(method='ffill', axis=1)
                DF_FINAL[:2] = DF_FINAL[:2].fillna('')
                DF_FINAL.columns = (DF_FINAL.iloc[0] + ' ' +DF_FINAL.iloc[1])
                DF_FINAL = DF_FINAL.iloc[2:]

                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 

            except Exception as e:
                print(e)
                pass

        for i in FINAL.columns:    
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        FINAL['description'] = np.where((FINAL['CPT/DRG'].notnull())&(FINAL['description'].isnull()),"NA",FINAL['description'])

        FINAL['CPT/DRG'].fillna(method='ffill', inplace=True)
        #FINAL=FINAL[FINAL['DETAIL '].notnull()]
        FINAL["LineItem_cnt"] = FINAL.groupby("CPT/DRG")["CPT/DRG"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1

        FINAL=FINAL[(FINAL["LineItem_cnt"] ==0) | ((FINAL["LineItem_cnt"] >=1))] #& (FINAL["DETAIL "].str.contains("",case=False))) ]

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        FINAL = FINAL[pd.notnull(FINAL['Shoppable Service Category'])]

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    

    Combined_data = Combined_data.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat101_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[52]:


#################### wideFormat109 #######################
def wifeFormat109(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        DF_11[:c+1]=DF_11[:c+1].fillna(method='ffill', axis=1)
                        DF_11[:c+1] = DF_11[:c+1].fillna(' ')
                        DF_FINAL.columns = (DF_FINAL.iloc[c] + ' ' + DF_FINAL.iloc[c+1] + ' ' + DF_FINAL.iloc[c+2] )
                        DF_FINAL = DF_FINAL.iloc[c+3:]
                        DF_FINAL = DF_FINAL.dropna(how='all',axis=0)
                        FINAL = DF_FINAL.reset_index(drop=True)

                        #Dropping the row
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass


        FINAL['CPT/HCPCS CODE    '] = FINAL['CPT/HCPCS CODE    '].replace(np.nan, 0)

    #    FINAL[FINAL["CPT/HCPCS"].str.contains(" SERVICES")==False]
        FINAL['bool series'] =  pd.notnull(FINAL["SHOPPABLE SERVICE    "])
        FINAL['code1'] = FINAL.loc[FINAL['bool series'] == True, 'CPT/HCPCS CODE    ']
    #    FINAL['Primary Service and Ancillary Service1'] = FINAL.loc[FINAL['bool series'] == True, 'Primary Service and Ancillary Service']
    #    FINAL=FINAL[~(FINAL["description"].str.contains(' rate type'))]
        FINAL['SHOPPABLE SERVICE    '] = FINAL['SHOPPABLE SERVICE    '].fillna(method='ffill', axis=0)
        FINAL['code1'] = FINAL['code1'].fillna(method='ffill', axis=0)    


        for i in FINAL.columns:    
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        try:
            FINAL = FINAL.loc[:, ~FINAL.columns.str.contains(' rate type')]
        except:
            None

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '



        all_columns = list(FINAL)
        FINAL[all_columns] = FINAL[all_columns].replace('\$|,', '', regex=True)
        cols=[i for i in FINAL.columns if i not in ["CPT/HCPCS","Hospital_Id","Primary Service and Ancillary Service","description", "inpatient-outpatient","bool series", "code1"]]

        for col in cols:
            FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')         


        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        FINAL["LineItem_cnt"] = FINAL.groupby(["description","code1"])["code1"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1


        FINAL=FINAL.groupby(['description','Hospital_Id', 'inpatient-outpatient', 'code1', 'LineItem_cnt']).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)
        FINAL



        FINAL=FINAL.rename(columns = {'code1': "CPT/HCPCS"}) 
        FINAL['CPT/HCPCS'] = FINAL['CPT/HCPCS'].replace(0,np.nan)

        df2=FINAL  
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        df3=df3[df3['name'].notnull()] 

        df4=df3
        df4["cost"] = df4["cost"].replace('',np.nan)
        df4["cost"] = df4["cost"].replace(0,np.nan)


        df4=df4[df4['cost'].notnull()]

        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat109_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[53]:


################### wideFormat10 ###################
def wideFormat10(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 

            except Exception as e:
                print(e)
                pass        


            DATA1=FINAL[FINAL['Shoppable_Cd'].notnull()]
            DATA1['HCPC/Cpt_Cd'] = np.where(DATA1['Shoppable_Cd'] != 'Total', DATA1['Shoppable_Cd'], DATA1['HCPC/Cpt_Cd'])
            DATA1['HCPC/Cpt_Cd'].fillna(method='ffill', inplace=True)
            DATA1['Rev_Cd'].fillna(method='ffill', inplace=True)
            DATA1['Rev_Cd_Descr'].fillna(method='ffill', inplace=True)
            DATA1['Quantity/Units'].fillna(method='ffill', inplace=True)
            DATA2= DATA1[DATA1['Shoppable_Cd']=='Total']
            DATA2
            FINAL['Shoppable_Cd'].fillna(method='ffill', inplace=True)
            FINAL['Descr'].fillna(method='ffill', inplace=True)
            DATA3=FINAL.groupby(["Shoppable_Cd", "Descr"]).size().reset_index(name="LineItem_cnt")
            DATA3
            DATA4= DATA3[DATA3['Shoppable_Cd']!='Total']
            DATA4
            new_df = pd.merge(DATA2, DATA4,  how='left', left_on=['HCPC/Cpt_Cd','Descr'], right_on = ['Shoppable_Cd','Descr'])


            for i in new_df.columns:
                x = str(i).lower().strip()
                new_df=new_df.rename(columns= {i:x})
                FINAL1 = new_df

            for i in FINAL1.columns.tolist():

                for k in CNames.itertuples(index=False):

                    for r in DF6.itertuples(index=False):

                        if str(k[1]) == str(r[2]):
                            try:

                                for i in FINAL1.columns.tolist():

                                    if str(i) == str(r[0]):

                                        FINAL1.rename(columns={i:r[1]}, inplace=True)
                            except:
                                    pass

            for i in FINAL1.columns.tolist():
                for r in DF6.itertuples(index=False):
                    if str(r[2]) == "nan" :
                        if str(i) == str(r[0]):
                            FINAL1.rename(columns={i:r[1]}, inplace=True)

            Search_List = list(DF8["Dropping columns"])
        #dropping columns
            FINAL1= FINAL1.drop(columns=[col for col in FINAL1 if col in Search_List])

            column_list = ['inpatient-outpatient']
            for col in column_list:
                if col not in FINAL1.columns:
                    FINAL1[col] = ' '

            df2=FINAL1
            df2 = df_column_uniquify(df2)

            Col_list = list(DF5["Columns"])
            df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')

            df4=df3
            df4=df4[df4['cost'].notnull()]

            df4
            Combined_data.append(df4)

    Combined_data = pd.concat(Combined_data)        

    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass


    # Drop if any duplicate column comes in the data
    Sample_output = Combined_data.drop_duplicates()




    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat10_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[54]:


######################### wideFormat116 #######################
def wideFormat116(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []


    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]

                    else:
                        None 
            except Exception as e:   
                print(e)
                pass


        for i in FINAL.columns:

           x = str(i).lower().strip()
           FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

    #    FINAL['descripition1'] = np.where((FINAL['CPT/HCPCS'].notnull()) & (FINAL['description'].notnull()) , FINAL['description'],"")
    #    lst=['Procedure/Charge Number','rev_cd','description']
    #    if CNames['id'].isin(['1215']).any():
    #        FINAL.drop(columns=[col for col in FINAL if col  in lst],axis=1,inplace=True)
    #        
    #    else:
    #        pass
    #    if CNames['id'].isin(['1215']).any():
    #          
    #        FINAL=FINAL.rename(columns = {'descripition1': "description"})
    #    else:
    #        pass

        FINAL = FINAL.replace(r'^\s*$', np.NaN, regex=True)
        cols=['CPT/HCPCS','description']
        FINAL[cols]=FINAL[cols].ffill()


        FINAL['LineItem_cnt'] = None 
        FINAL['LineItem_cnt'] = FINAL.groupby(['CPT/HCPCS','description']).transform(np.size)
        FINAL['LineItem_cnt']=FINAL['LineItem_cnt']-1





        FINAL.fillna(value = 0,inplace = True)

        col_list1=['CPT/HCPCS','LineItem_cnt','description']
        FINAL1=FINAL.groupby([col for col in FINAL if col  in col_list1]).aggregate(['sum']).reset_index()
        FINAL.columns = FINAL.columns.get_level_values(0)
        FINAL['inpatient-outpatient'] = np.where((FINAL['inpatient-outpatient'] == 0)  , ' ', FINAL['inpatient-outpatient'])



        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')


        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data = Combined_data.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat116_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[55]:


########################## wideFormat16 ####################
def wideFormat16(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0
                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass

        for i in FINAL.columns:          
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        FINAL1=FINAL[FINAL['Primary Service and Ancillary Service'].notnull()]
        FINAL1['description'].fillna(method='ffill', inplace=True)
        FINAL1['Shoppable Service Category'].fillna(method='ffill', inplace=True)

        FINAL1['cpt code']=FINAL1['description'].str.extract('(\d+)')
        #FINAL1['description1'] = FINAL1['description'].str.replace('\d+','')

        FINAL1.loc[FINAL1['cpt code']== '0297', 'cpt code'] = "G0297"
    #    Name_List1 = list((DF9["cpt codes"]).astype(str))
    #    FINAL1['description1'] = FINAL1['description'].str.replace('|'.join(Name_List1),'',case=False)
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 304','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 301','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 302','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 321','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 513','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 560','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 540','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR DRG 640','')
    #    FINAL1['description1']=FINAL1['description1'].str.replace('MS DRG /APR 480','')
    #    FINAL1['description1']=FINAL1['description1'].str.strip()

        FINAL1["LineItem_cnt"] = FINAL1.groupby("description")["description"].transform('count')
        FINAL1["LineItem_cnt"]=FINAL1["LineItem_cnt"]-1
        FINAL1
        FINAL2=FINAL1[(FINAL1["LineItem_cnt"] ==0) | ((FINAL1["LineItem_cnt"] >=1) & (FINAL1["Primary Service and Ancillary Service"].str.contains("Total",case=False))) ]


        FINAL2.drop(["description"], axis = 1, inplace = True)
        FINAL3=FINAL2.rename(columns={'description1': 'description'})


        #Checking the  Inpatient/Outpatient column exists or not


        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL3
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    # Combined_data["id"


    Combined_data = Combined_data.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat16_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[56]:


##################### wideFormat44 ###################
def wideFormat44(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []


    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_FINAL.iloc[:c,:c] = np.nan

                        DF_FINAL[:c] = DF_FINAL[:c].fillna(method='ffill', axis=1)
                        DF_FINAL = DF_FINAL.replace(np.nan,'',regex=True)
                        idx = DF_FINAL.index.get_loc(c)
                        DF_11 = DF_FINAL.iloc[idx - c :]
                        j = c+1

                        req_rows = np.where(DF_11.index == j)[0][0]
                        start = max(0, req_rows - j )
                        end = max(1, req_rows)
                        DF_12 = DF_11.iloc[start:end]

                        DF_12= DF_12.apply(lambda c: ' '.join(c), axis=0)
                        DF_12 = DF_12.to_frame()
                        DF_13 = DF_12.T

                        DF_11.drop(DF_11.head(j).index, inplace = True)
                        DF15 = DF_13.append(DF_11)
                        DF16 = DF15.reset_index(drop = True)
                        DF16.columns = DF16.iloc[0]
                        FINAL = DF16[1:]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass

        for i in FINAL.columns:    
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data = Combined_data.drop_duplicates()

    try: 
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat44_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[57]:


#################### wideFormat4_4 ###############
def wideFormat4_4(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                    else:
                        None 
            except Exception as e:
                print(e)
                pass

        lst=['Service Code']
        FINAL= FINAL[~FINAL['Service Code'].isin(lst)]   

        P=[] 
        # for the values in the cost column, where ever $ symbol is present it is replaced with an empty string.
        for d in FINAL['Service Fee/ Gross Charge']:
            yu = d.replace(r'$', '')
            yu1= yu.replace(r'/hour', '')
            yu2 = yu1.replace(r'/daily charge', '')
            P.append(yu2) # Now the new

        for index,all in enumerate(P):
            if "-" in all:
                ind = index
                l = all.split("-")  # Where we find a "-" there the values get split 
                b =list(map(float,l)) 
                d =np.mean(b) 
                      # Mean of two values that are seperated is calculated
                P[ind] = d          # The mean value is replaced to their respective index

        FINAL['Service Fee/ Gross Charge']=P

        for i in FINAL.columns:    
            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

        Search_List = list(DF8["Dropping columns"])
            #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])

        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        lst=['Service Code']
        df1= FINAL[~FINAL['Procedure/Charge Number'].isin(lst)] 

        P=[] 
        # for the values in the cost column, where ever $ symbol is present it is replaced with an empty string.
        for d in df1['Gross Charge']:
            yu = d.replace(r'$', '')
            yu1= yu.replace(r'/hour', '')
            yu2 = yu1.replace(r'/daily charge', '')
            P.append(yu2) # Now the new

        for index,all in enumerate(P):
            if "-" in all:
                ind = index
                l = all.split("-")  # Where we find a "-" there the values get split 
                b =list(map(float,l)) 
                d =np.mean(b) 
                      # Mean of two values that are seperated is calculated
                P[ind] = d          # The mean value is replaced to their respective index
        df1=FINAL
        df1 = df_column_uniquify(df1)
        df1['GC_1']=df1['Gross Charge']

        Col_list = list(DF5["Columns"])
        df2=df1.melt(id_vars=[col for col in df1 if col in Col_list],var_name="name",value_name='cost')

        df2['cost1']=None
        df2['cost1']=df2['cost'].str.extract('(\d+(\.\d+)?%)')
        df2['cost1']=df2['cost1'].str.strip('%')
        df2
        df2['cost1']=df2['cost1'].astype(float)
        df2['cost1'] = df2['cost1'].div(100)
        df2['GC_1']=df2['GC_1'].astype(float)
        df2['cost1'] = pd.np.where(df2['cost1'].isnull(), df2['cost'],df2['cost1'])

        if k[1]=='5291':
            try:
                df2.drop(['cost','GC_1'],axis=1,inplace=True)

            except:
                Search_List = list(DF8["Dropping columns"])

                df2.drop(columns=[col for col in df2 if col  in Search_List])

        for r in DF6.itertuples(index=False):
            df2.rename(columns={r[0]:r[1]}, inplace=True)
        df4=df2[df2['cost'].notnull()]
        df4["cost"]=df4.cost.astype('str')


        L=[] 
        # for the values in the cost column, where ever $ symbol is present it is replaced with an empty string.
        for n in df4['cost']:
            yu3 = n.replace(r'$', '')
            yu4= yu3.replace(r'/hour', '')
    #     yu2 = yu1.replace(r'/daily charge', '')

            L.append(yu4) # Now the new

        for index,all in enumerate(L):
            if "-" in all:
                ind = index
                m = all.split("-")  # Where we find a "-" there the values get split 
                n =list(map(float,m)) 
                o =np.mean(n)
                      # Mean of two values that are seperated is calculated
                L[ind] = o         # The mean value is replaced to their respective index
        df4['cost']=L



        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data = Combined_data.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat4_4_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[58]:


#################### wideFormat85 ##############
def wideFormat85(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0


                    for index, row in DF_10.iterrows():


                        if (pd.notnull(row[0])):                       

                            m=m+1
                            print(m)
                            break
                        else:
                            c=c+1


                    if c==0:

                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)

                        else:

                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            for i in FINAL.columns:
                                x = str(i).lower().strip()
                                FINAL=FINAL.rename(columns= {i:x})

                            for i in FINAL.columns.tolist():

                                for k in CNames.itertuples(index=False):

                                    for r in DF6.itertuples(index=False):

                                        if str(k[1]) == str(r[2]):
                                            try:

                                                for i in FINAL.columns.tolist():

                                                    if str(i) == str(r[0]):

                                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                                            except:
                                                    pass

                            for i in FINAL.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(r[2]) == "nan" :
                                        if str(i) == str(r[0]):
                                            FINAL.rename(columns={i:r[1]}, inplace=True)
                            combined_final.append(FINAL)



                    elif c>=1:

                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        for i in FINAL.columns:
                            x = str(i).lower().strip()
                            FINAL=FINAL.rename(columns= {i:x})

                        for i in FINAL.columns.tolist():

                            for k in CNames.itertuples(index=False):

                                for r in DF6.itertuples(index=False):

                                    if str(k[1]) == str(r[2]):
                                        try:

                                            for i in FINAL.columns.tolist():

                                                if str(i) == str(r[0]):

                                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                                        except:
                                                pass

                        for i in FINAL.columns.tolist():
                            for r in DF6.itertuples(index=False):
                                if str(r[2]) == "nan" :
                                    if str(i) == str(r[0]):
                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                        combined_final.append(FINAL)


                    else:
                        None


            except Exception as e:
                print(e)
                pass

                    #if CNames['ID'].isin(['992331','1898']).any():
            #FINAL['Cpt code'].isnull().sum()  
            FINAL = pd.concat(combined_final) 
            FINAL['description'].fillna(method='ffill', inplace=True)
            FINAL["LineItem_cnt"] = FINAL.groupby(["description"])["description"].transform('count')
            FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1
            FINAL = FINAL.drop_duplicates(subset=['description'],keep='first')
            column_list = ['inpatient-outpatient']
            for col in column_list:
                if col not in FINAL.columns:
                    FINAL[col] = ' '

            df2=FINAL
            df2 = df_column_uniquify(df2)
            #Required format - variable list
            Col_list = list(DF5["Columns"])
            df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
            #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

            #df3['payer'] = None
            #df3['insurance Type'] = None
            #df3['Benefit Type']=None

            #Name_List = list(DF1["Payers"])

            #Name_List1 = list(DF2["Insurance Type"])


            #Name_List2 = list(DF3["Benefit Type"])

            #Finding Payers from the list
            #for i in Name_List:
            #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
            #Finding Insurancetype from the list    
            #for j in Name_List1:
            #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

            #Finding Benefit Type from the list  
            #for k in Name_List2:
            #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

            df4=df3
            df4=df4[df4['cost'].notnull()]

            #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
            #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
            df4
            Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)
    Combined_data
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes

    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])
    Sample_output = Combined_data

    Combined_data = Sample_output.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat85_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[59]:


################## wideFormat86 #################
def wideFormat86(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0
                                    #Getting the count of  empty rows
                    for index, row in DF_10.iterrows():
                        if(pd.notnull(row[0])): 
                            m=m+1
                            break
                        else:
                            c=c+1

                    if c==0:
                    #This Case works when description is in one row
                        if "Unnamed: 5" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL
                        else:
                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL

                    elif c>=1:
                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows

                        DF_11=DF_FINAL.iloc[c:]
                        #DF_111 = DF_11
                        #print(DF_11)

                        list1=['CMS-Specified Shoppable Service, Evaluation & Management Services','Shoppable LEVEL OF CARE Service Description','Shoppable DRG Service Description','CMS-Specified Shoppable Service, Laboratory & Pathology Services','CMS-Specified Shoppable Service, Radiology Services','CMS-Specified Shoppable Service, Medicine and Surgery Services']
                        DF_11["ROW"] = DF_11.iloc[:,0].isin(list1)
                        DF_11= DF_11.reset_index(drop=True)
                        n =0
                        for index, row in DF_11['ROW'].iteritems():
                            if row == True:
                                n += 1
                                DF_11['ROW'][index] = n
                            else:
                                DF_11['ROW'][index] = np.nan
                                DF_11['ROW'].fillna(method='ffill', inplace=True)
                        List_ids=[]
                        for i in DF_11['ROW']:
                            List_ids.append(i)
                            List_ids=list(set(List_ids))
                        list2 = []
                        for i in List_ids:
                            DF = DF_11[DF_11['ROW']==i]
                            DF = DF.drop(['ROW'],axis=1)
                            DF.columns = DF.iloc[0]
                            DF = DF.iloc[1:]
                            DF = DF.dropna(how='all',axis=1)
                            DF = DF.dropna(how='all',axis=0)
                            DF = DF.reset_index(drop=True)
                            DF['id']=k[1]

                            for i in DF.columns:
                                x = str(i).lower().strip()
                                DF=DF.rename(columns= {i:x})
                            #print(DF_FINAL1.columns)
                            for i in DF.columns.tolist():
                                for r in DF6.itertuples(index=False):
                                    if str(i) == str(r[0]):
                                        DF.rename(columns={i:r[1]}, inplace=True)
                            list2.append(DF)
                        # combined_df.append(list2)
                        FINAL = pd.concat(list2)
                    else:
                        None


            except Exception as e:    
                print(e)
                pass

        try:
            FINAL = pd.concat(combined_final)
        except:
            pass

        Search_List = list(DF8["Dropping columns"])
        #dropping columns
        FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])


        #Checking the  Inpatient/Outpatient column exists or not
        column_list = ['inpatient-outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])
        #try:
        #    if 'cpt code'=='cpt code':
        #        df2=FINAL[FINAL['cpt code'].notnull()] 
        #    elif 'hcpcs code'=='hcpcs code':
        #        df2=FINAL[FINAL['hcpcs code'].notnull()]
        #except:
        #    None

        df2=FINAL  
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
        #df3=df3[df3['name'].notnull()] 

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k  

        df4=df3
        df4=df4[df4['cost'].notnull()]
        df4=df4[df4['name'].notnull()]
        #df4=df4[df4['Cpt code'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data = Combined_data.drop_duplicates()
    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Sample_output = Combined_data
    #Joining both hospital data with shoppable data by ID
    #Sample_output=pd.merge(left=DHC_Hosp_data1, right=Combined_data,how='inner',left_on=['Hospital_Id'],right_on=['id'])

    #Dropping the columns
    #Sample_output.drop(["id"], axis = 1, inplace = True)
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains(' ip')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains(' op')) , 'Outpatient', Sample_output['inpatient-outpatient'])
    #Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains(' ip')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains(' op')) , 'Outpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('inpatient')) , 'Inpatient', Sample_output['inpatient-outpatient'])
    Sample_output['inpatient-outpatient'] = np.where((Sample_output['inpatient-outpatient'] ==' ') & (Sample_output['name'].str.contains('outpatient')) , 'Outpatient', Sample_output['inpatient-outpatient'])

    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat86_{}.csv"
    Sample_output.to_csv(output_path.format(k[1]), index=False)
    return Sample_output


# In[60]:


#################### wideFormat99 #################
def wideFormat99(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []

    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')

            non_prep_data.append(DF_FINAL)

        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str,header=None)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)
                non_prep_data.append(DF_FINAL)

        for j in df7.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]

        for DF_FINAL in non_prep_data:
            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0


                    for index, row in DF_10.iterrows():


                        if (pd.notnull(row[0])):                       

                            m=m+1
                            print(m)
                            break
                        else:
                            c=c+1


                    if c==0:

                    #This Case works when description is in one row
                        if "Unnamed: 2" in DF_10:
                            DF_FINAL.columns=DF_FINAL.iloc[0]
                            FINAL=DF_FINAL.drop([c])
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            FINAL

                        else:

                            #This Case works when description is not found
                            FINAL=DF_FINAL
                            FINAL['id']=k[1]
                            FINAL.dropna(how='all', axis=1, inplace=True)
                            FINAL

                    elif c>=1:

                        #This Case works when description is more than 1 row
                        #Dropping 'c' rows
                        DF_11=DF_FINAL.iloc[c:]
                        #row values as column names
                        DF_11.columns=DF_11.iloc[0]
                        #Dropping the row
                        FINAL=DF_11.drop([c])
                        FINAL['id']=k[1]
                        FINAL.dropna(how='all', axis=1, inplace=True)
                        FINAL


                    else:
                        None


            except Exception as e:
                print(e)
                pass
            FINAL["Code Type1"] = FINAL['Code Type'].str.contains('Charge')
            column_list = ['Code1']
            for col in column_list:
                if col not in FINAL.columns:
                    FINAL[col] = ' '
            FINAL["Code1"]= np.where((FINAL["Code Type1"].apply(lambda x:x==False)), FINAL['Code'].apply(lambda x: x.split(" ", 1)[0]),FINAL["Code1"])
            FINAL["description"]= np.where((FINAL["Code Type1"].apply(lambda x:x==False)), FINAL['Description'], 'nan')

            FINAL['description'] = FINAL['description'].replace('nan', np.nan, regex=True)
            FINAL['Code1'] = FINAL['Code1'].replace(' ', np.nan, regex=True)

            FINAL['description'].fillna(method='ffill', inplace=True,axis=0)
            FINAL['Code1'].fillna(method='ffill', inplace=True,axis=0)

            FINAL.drop(['Code Type','Description','Code','Notes/Disclaimer','Code Type1'],axis=1,inplace = True)

            all_columns = list(FINAL)
            #FINAL[all_columns] = FINAL[all_columns].replace('\$|,', '', regex=True)
            cols=[i for i in FINAL.columns if i not in ['id','Code1', 'description']]

            for col in cols:
                FINAL[col] = pd.to_numeric(FINAL[col], errors='coerce')

            #FINAL = FINAL.drop_duplicates(keep='first', inplace=False)


            FINAL["LineItem_cnt"] = FINAL.groupby(["Code1","description"])["Code1"].transform('count')
            FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1

            FINAL=FINAL.groupby(['id','Code1', 'description','LineItem_cnt']).aggregate(['sum']).reset_index()
            FINAL.columns = FINAL.columns.get_level_values(0)
            FINAL   



            for i in FINAL.columns:
                x = str(i).lower().strip()
                FINAL=FINAL.rename(columns= {i:x})

            for i in FINAL.columns.tolist():

                for k in CNames.itertuples(index=False):

                    for r in DF6.itertuples(index=False):

                        if str(k[1]) == str(r[2]):
                            try:

                                for i in FINAL.columns.tolist():

                                    if str(i) == str(r[0]):

                                        FINAL.rename(columns={i:r[1]}, inplace=True)
                            except:
                                    pass

            for i in FINAL.columns.tolist():
                for r in DF6.itertuples(index=False):
                    if str(r[2]) == "nan" :
                        if str(i) == str(r[0]):
                            FINAL.rename(columns={i:r[1]}, inplace=True)

            Search_List = list(DF8["Dropping columns"])
            #dropping columns
            FINAL= FINAL.drop(columns=[col for col in FINAL if col  in Search_List])

            FINAL=FINAL.rename(columns = {'code1': "CPT/HCPCS/DRG"}) 

           #Checking the  Inpatient/Outpatient column exists or not
            column_list = ['inpatient-outpatient']
            for col in column_list:
                if col not in FINAL.columns:
                    FINAL[col] = ' '

        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])
        #try:
        #    if 'cpt code'=='cpt code':
        #        df2=FINAL[FINAL['cpt code'].notnull()] 
        #    elif 'hcpcs code'=='hcpcs code':
        #        df2=FINAL[FINAL['hcpcs code'].notnull()]
        #except:
        #    None

            df2=FINAL
            df2 = df_column_uniquify(df2)
           #Required format - variable list
            Col_list = list(DF5["Columns"])
            df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
           #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')
            df3=df3[df3['name'].notnull()] 

           #df3['payer'] = None
           #df3['insurance Type'] = None
           #df3['Benefit Type']=None

           #Name_List = list(DF1["Payers"])

            #Name_List1 = list(DF2["Insurance Type"])

            #Name_List2 = list(DF3["Benefit Type"])

            #Finding Payers from the list
            #for i in Name_List:
            #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
            #Finding Insurancetype from the list    
            #for j in Name_List1:
            #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

            #Finding Benefit Type from the list  
            #for k in Name_List2:
            #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k  

            df4=df3
            df4["cost"] = df4["cost"].replace('',np.nan)

            df4=df4[df4['cost'].notnull()]
            #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
            #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
            df4
            Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    #Combined_data["id"]=Combined_data.id.astype('int64')
    #Combined_data.dtypes
    Sample_output = Combined_data

    Combined_data = Sample_output.drop_duplicates()
    try:

        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' ip')) , 'Inpatient', Combined_data['inpatient-outpatient'])
        Combined_data['inpatient-outpatient'] = np.where((Combined_data['inpatient-outpatient'] ==' ') & (Combined_data['name'].str.contains(' op')) , 'Outpatient', Combined_data['inpatient-outpatient'])
    except:
        pass


    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat99_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data


# In[61]:


################# wideFormat18_1 #################
def wideFormat18_1(CNames):
    global df7
    
    Combined_data = []
    combined_final = []
    non_prep_data = []
    #Getting the details as list from files
    for k in CNames.itertuples(index=False):
        f=k[0] 
        #Checking the file type- xlsx   
        if k[2]== 'csv':
            try:
                DF_FINAL=pd.read_csv(f)
            except:
                DF_FINAL=pd.read_csv(f,encoding='latin1')


        elif k[2]== 'xlsx':
            sheet_to = pd.read_excel(f,sheet_name=None,dtype=str)

            for sh in sheet_to.keys():

                DF_FINAL = pd.read_excel(f,sheet_name=sh,dtype=str)
                print(sh)


                non_prep_data.append(DF_FINAL)

        for j in df2.itertuples(index=False):
            if str(k[1]) == str(j[0]):
                s = j[1]



        for DF_FINAL in non_prep_data:


            try:
                if DF_FINAL.iloc[:, [s]].empty == False:


                    DF_10 = DF_FINAL.iloc[:,[s]]
                    m=0
                    c=0

            #Getting the count of  empty rows
                for index, row in DF_10.iterrows():
                    if(pd.notnull(row[0])): 
                        m=m+1
                        break
                    else:
                        c=c+1

                if c==0:
                #This Case works when description is in one row
                    if "Unnamed: 2" in DF_10:
                        DF_FINAL.columns=DF_FINAL.iloc[0]
                        FINAL=DF_FINAL.drop([c])
                        FINAL['id']=k[1]
                        FINAL
                    else:
                        #This Case works when description is not found
                        FINAL=DF_FINAL
                        FINAL['id']=k[1]
                        FINAL

                elif c>=1:
                    #This Case works when description is more than 1 row
                    #Dropping 'c' rows
                    DF_11=DF_FINAL.iloc[c:]
                    #row values as column names
                    DF_11.columns=DF_11.iloc[0]
                    #Dropping the row
                    FINAL=DF_11.drop([c])
                    FINAL['id']=k[1]
                else:
                    None 

        #Checking the file type- csv
            except Exception as e:
                print(e)
                pass
        for i in FINAL.columns:

            x = str(i).lower().strip()
            FINAL=FINAL.rename(columns= {i:x})

        for i in FINAL.columns.tolist():

            for k in CNames.itertuples(index=False):

                for r in DF6.itertuples(index=False):

                    if str(k[1]) == str(r[2]):
                        try:

                            for i in FINAL.columns.tolist():

                                if str(i) == str(r[0]):

                                    FINAL.rename(columns={i:r[1]}, inplace=True)
                        except:
                                pass

        for i in FINAL.columns.tolist():
            for r in DF6.itertuples(index=False):
                if str(r[2]) == "nan" :
                    if str(i) == str(r[0]):
                        FINAL.rename(columns={i:r[1]}, inplace=True)

         # FINAL.drop(["CPT/HCPCS","DESCRIPTION"], axis = 1, inplace = True)
         #FINAL
        Search_List = list(DF8["Dropping columns"])
            #dropping columns

        FINAL= FINAL.drop(columns=[col for col in FINAL if col in Search_List])
        column_list = ['inpatient_outpatient']
        for col in column_list:
            if col not in FINAL.columns:
                FINAL[col] = ' '

        FINAL['billing_code'].fillna(method='ffill', inplace=True)
        try:

            FINAL=FINAL[FINAL['procedure_chargenumber'].notnull()]
        except:
            pass
        FINAL["LineItem_cnt"] = FINAL.groupby("billing_code")["billing_code"].transform('count')
        FINAL["LineItem_cnt"]=FINAL["LineItem_cnt"]-1

        try:

            FINAL=FINAL[(FINAL["LineItem_cnt"] ==0) | ((FINAL["LineItem_cnt"] >=1) & (FINAL["procedure_chargenumber"].str.contains("CLAIM",case=False))) ]
            FINAL         
            #FINAL=FINAL[(FINAL["Primary Service and Ancillary Service"].str.contains("CLAIM",case=False))]
            #FINAL         
            FINAL.drop(["procedure_chargenumber"], axis = 1, inplace = True)        
        except:
            pass
        #Search_List = list(DF3["Keeping columns"])
        #Keeping columns
        #df1= FINAL.drop(columns=[col for col in FINAL if col not in Search_List])
        #Removing the empty rows from'cpt code'
        #df2 = df1.dropna(axis=0, subset=['Cpt Code'])

        #df2=FINAL[FINAL['cpt code'].notnull()] 

        df2=FINAL
        df2 = df_column_uniquify(df2)
        #Required format - variable list
        Col_list = list(DF5["Columns"])
        df3=df2.melt(id_vars=[col for col in df2 if col in Col_list],var_name="name",value_name='cost')
        #df3=df1.melt(id_vars=['ID','Description','Cpt Code','Rev_Cd','Units'],var_name="Name",value_name='Cost')

        #df3['payer'] = None
        #df3['insurance Type'] = None
        #df3['Benefit Type']=None

        #Name_List = list(DF1["Payers"])

        #Name_List1 = list(DF2["Insurance Type"])

        #Name_List2 = list(DF3["Benefit Type"])

        #Finding Payers from the list
        #for i in Name_List:
        #    df3['payer'][(df3['name'].str.contains(i,case=False,na=False))] = i
        #Finding Insurancetype from the list    
        #for j in Name_List1:
        #    df3['insurance Type'][(df3['name'].str.contains(j,case=False,na=False))] = j

        #Finding Benefit Type from the list  
        #for k in Name_List2:
        #    df3['Benefit Type'][(df3['name'].str.contains(k,case=False,na=False))] = k 

        df4=df3
        df4=df4[df4['cost'].notnull()]
        #Dropping_columns=['Not offered','Not Contracted','N/A','Not a covered service','Not Offered','Not contracted','Not a covered service','N/A']
        #df4 = df4[~df4['Cost'].isin(Dropping_columns)]
        df4
        Combined_data.append(df4)
    Combined_data = pd.concat(Combined_data)

    Combined_data
    output_path = r"C:\Users\User\Zigna AI Corp\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\outputs\Wideformat18_1_{}.csv"
    Combined_data.to_csv(output_path.format(k[1]), index=False)
    return Combined_data

