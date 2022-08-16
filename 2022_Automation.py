# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 16:57:21 2022

@author: user
"""

# Import all the dependencies

import getpass
import sys
import os
import shutil
import re
import warnings
import glob
import pandas as pd
import numpy as np
import traceback

import Format_Functions_2022

import time
import pytz
# from Format_functions_487_main import*
# from Functions_remaining import*
from Format_Functions_2022  import*
#from Format_functions_0911 import *      # Importing all the functions from the Format_functions file
#from Format_functions_main import *      # Importing all the functions from the Format_functions file
from inspect import getmembers, isfunction
#pip install pywin32
#import multiprocessing as mp
#import win32com.client as client
warnings.filterwarnings("ignore")






Format_Functions_2022


# Version of the packages

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)


# STEP-1 : Listing out all the functions used in the Format_functions file 

functions_list = getmembers(Format_Functions_2022, isfunction)

res = [list(ele) for ele in functions_list]    # convert list of tuples to list of list

# To extract first and last element of each sublist in a list of lists
def Extract(lst):
    return [item[0] for item in lst]
res_1 = Extract(res)           # res_1 list of functions present in the Functions_format file

res_1

# STEP-2 : Read the Hospital base file 
Hosp_data = pd.read_excel(r"D:\Zigna AI Corp\Zigna AI Corp - Hospital Application_2022-03-09\Automation_task\2022 Automation\Hospitals with shoppables Links Base file Automation.xlsx", sheet_name="Raw file")


Hosp_data_1 = Hosp_data[["Hospital_Id" ,"Format_type","iloc"]]             # Considering only the required columns
Hosp_data_1 = Hosp_data_1.rename(columns = {'Hospital_Id': 'ID'})   # Changing the column name
Hosp_data_1['ID']= Hosp_data_1['ID'].astype(str) 
#Converting ID type to string from int
Hosp_data_1


#sample files

####################################################################################

# 487 sample files IDs 

rawfileID = pd.read_excel(r"C:\Users\zigna\Zigna AI Pvt Ltd\Zigna AI Corp - RightPx\Hospital Application_2022-03-09\Automation_task\4_6files.xlsx",dtype=str)

rawfileID




#extract the raw file folder
raw_file_path = "C:" + os.path.sep + "Users"+ os.path.sep + "zigna" + os.path.sep + "Zigna AI Pvt Ltd"+ os.path.sep + "Zigna AI Corp - RightPx"+ os.path.sep +"Hospital Application_2022-03-09" + os.path.sep +"Structured files" + os.path.sep + "487 files list" + os.path.sep + "487 files"


os.chdir(raw_file_path)
path = os.getcwd()
files = os.listdir(path)




#List of Files 
mylinks=[]
for i in files:
    i
    mylinks.append(i)
mylinks  
cols=['FILE_NAMES']
#Creating the dataframe
CNames=pd.DataFrame(mylinks,columns=cols)
CNames

# #extracting ids and type of file from CNames dataframe
CNames[['ID']]=CNames.FILE_NAMES.str.extract('(\d+)')
CNames['Type'] = CNames.FILE_NAMES.str.split('.',expand= True)[1]
# for i in CNames.FILE_NAMES:
#     i
#     CNames[['Type']] = i.split('.')[-1]
#     print(CNames.columns)
# CNames
CNames



CNames1 = CNames.merge(rawfileID, how="inner", left_on = "ID", right_on="Hospital_Id")
#CNames1.drop(['Format_Type'],axis=1, inplace=True)
raw_file_count = len(CNames1)                       # Number of files in the raw file folder
raw_file_count






CNames1



# STEP-4 : Merging CNames and Hosp_data_1 using left join based on Hospital ID

Final = CNames1.merge(Hosp_data_1, on='ID', how='left')  # Left join on bases of ID

#Final = Final.drop ('Format_type_y', axis =1)

Format_type_NA = [Final[Final['Format_type'].isna()]]   # Files that are not categorized from the raw file folder are taken into a list
Final = Final[Final['Format_type'].notna()]             # Whereever Format type is Nan the rows are dropped and the files details are listed out 
Final["Error"] = np.nan                                 # Creating an empty column "Error" to store the error occured
Final["Structured_time(seconds)"] = np.nan              # Creating an empty column "Structured_time(seconds)" to store the execution time for each file
Final.drop(['Hospital_Id','Hospital_Name'],axis=1, inplace=True)
files_categorized = len(Final)                          # categorized files count is taken from the raw files folder
files_categorized


Final

Final
Final["Format_type"].unique()



# STEP-5 : If the format type matches with the list of functions then that function works

# It iterates through each row in the dataframe and gives the output

not_struc_files = []       # struc_failed
struc_files = []            # struc_passed
not_matched_files = []      # Creating an empty list for storing the files that are not structured or throwing error

#error



for i in range(len(Final)):
    y = Final["Format_type"].iloc[i]                  # Iterates through each and every row 
#     if y in res_1:                                    # If that function presents in list of functions
#         s = Final["Format_type"].iloc[i]              # Iterating through each row in Format type
#         x = globals()[s]                              # Converting the format type to function
        
        

i

x

final = pd.DataFrame(Final.iloc[i:i+1,0:6])       # The entire column is passed into that particular function row by row
final

    final = pd.DataFrame(Final.iloc[i:i+1,0:6])       # The entire column is passed into that particular function row by row
        # 
        #x(final)
        try:                                          # Used try and except and running the function
            start = time.time()                       # start time
            x(final)
            end = time.time()                         # end time
            code_exec = end-start
            final["Structured_time(seconds)"] = round(code_exec,2)
            struc_files.append(final)                                      # executes the function  
        except Exception as e:                                       # If it throws any error 
            final["Error"] = e
            not_struc_files.append(final)                     # All the files that are throwing errors are apppended into empty list created above
            pass
    else:
        final = pd.DataFrame(Final.iloc[i:i+1,0:6])
        not_matched_files.append(final)

try:
    not_struc_files = pd.concat(not_struc_files)                  # Finally we will concatenate all the files names and their IDs in a dataframe   
    not_struc_files.insert(4,"Status","Structuring Failed")
except:
    not_struc_files = pd.DataFrame()

try:
    struc_files = pd.concat(struc_files)
    struc_files.insert(4,"Status","Structuring Passed")
except:
    struc_files = pd.DataFrame()

try:
    not_matched_files = pd.concat(not_matched_files)
    not_matched_files.insert(4,"Status","Function does not exist")  
except:
    not_matched_files = pd.DataFrame()

not_categorized_files = pd.concat(Format_type_NA)   
files_not_categorized = len(not_categorized_files)             # not categorized files count is taken from raw files folder               
not_categorized_files.insert(4,"Status","File not categorized")

# number_of_rows = len(struc_files) find length of index.
# print(struc_files)

       

i

not_categorized_files = pd.concat(Format_type_NA)   
files_not_categorized = len(not_categorized_files)             # not categorized files count is taken from raw files folder               
not_categorized_files.insert(4,"Status","File not categorized")

report_list = [not_struc_files,struc_files,not_matched_files]  # All the report dataframes of each case are taken into a list
report = pd.concat(report_list)                                # Got a final report by concatenating all of them
report["Indian_datestamp"] = pd.datetime.now().strftime("%d-%m-%Y")
usa_timezone = pytz.timezone('America/New_York')
usa_time = pd.datetime.now(usa_timezone)
report["US_datestamp"] = usa_time.strftime("%d-%m-%Y")

passed_file_count = len(report[report['Status'] == 'Structuring Passed'])
failed_file_count = len(report[report['Status'] == 'Structuring Failed'])
not_matched_file_count = len(report[report['Status'] == 'Function does not exist'])

#report["Structured_time(seconds)"].sum() # Time taken to structure the files present in CNames

body_text = "Hi mam!\nReport for structuring automation: \nTotal number of raw_files in the folder are {}\nNumber of files that are categorized in the raw files folder are {}\nNumber of files that are not categorized in the raw files folder are {}\nNumber of structured files are {}\nNumber of not structured files are {}\nNumber of files that are not matched with the existing formats are {}\nThe attachment has the clear report of all the files."

body_final_text = body_text.format(raw_file_count,files_categorized,files_not_categorized,passed_file_count,failed_file_count,not_matched_file_count)
body_text.format(raw_file_count,files_categorized,files_not_categorized,passed_file_count,failed_file_count,not_matched_file_count)

struc_report_path = "C:" + os.path.sep + "Users"+ os.path.sep + "zigna" + os.path.sep + "Zigna AI Pvt Ltd"+ os.path.sep + "Zigna AI Corp - RightPx"+ os.path.sep +"Hospital Application_2022-03-09" + os.path.sep +"Automation_task" + os.path.sep + "Struc_reports" + os.path.sep + "structuring_report_42.csv" 
#report.to_csv(struc_report_path.format(pd.datetime.now().strftime("%Y%m%d%H%M%S")), index = False)
try:
    check = pd.read_csv(struc_report_path) # It checks for the report file in the particular path
    report.to_csv(struc_report_path, mode='a', header=False, index = False) # If already a report file exists the current report will be appended to the same sheet
except:
    report.to_csv(struc_report_path,index= False) # If there is no particular file it creates one file

# Move all the Raw files from raw_file folder into another folder once the status of the file is "Structuring Passed" 
file_name_list = report["FILE_NAMES"].where(report["Status"]=="Structuring Passed").dropna()
file_name_list = file_name_list.tolist()

#List all files in path
for filename in os.listdir(raw_file_path):  
    #If file is present in list
    if filename in file_name_list:  
        # assigned a common path in sharepoint to move the "Structured raw files"
        try:
            new_path = "C:" + os.path.sep + "Users"+ os.path.sep + zigna + os.path.sep + "Zigna Analytics Private Limited"+ os.path.sep + "Zigna Analytics Private Limited Team Site - Hospital Application"+ os.path.sep +"Automation_task" + os.path.sep + "moved_sample20struc_files"
            full_file_path = os.path.join(path, filename)  # Old path
            new_file_path = os.path.join(new_path,filename) # New path
            #os.remove(full_file_path)
            os.rename(full_file_path,new_file_path)          # To rename the old path with new path
            #shutil.move(full_file_path,new_file_path)      # To move the file from old path to new path
        except:
            pass

########## Create a folder with present datestamp to move all the outputs into it #############

# create directory to shift all the outputs into that folder with that date timestamp
directory = "output_{}".format(pd.datetime.now().strftime("%m%d%Y"))
    
# Parent Directories path is outputs 
parent_dir = "C:" + os.path.sep + "Users"+ os.path.sep + "zigna" + os.path.sep + "Zigna AI Pvt Ltd"+ os.path.sep + "Zigna AI Corp - RightPx"+ os.path.sep +"Hospital Application_2022-03-09" + os.path.sep +"Automation_task" + os.path.sep + "outputs"
    
# Path is combined
path = os.path.join(parent_dir, directory) 
os.makedirs(path)




# All_outputs_path = "C:" + os.path.sep + "Users"+ os.path.sep + "zigna" + os.path.sep + "Zigna AI Pvt Ltd"+ os.path.sep + "Zigna AI Corp - RightPx"+ os.path.sep +"Hospital Application_2022-03-09" + os.path.sep +"Automation_task" + os.path.sep + "outputs"
# os.chdir(All_outputs_path)

# # Define the source and destination path
# source = All_outputs_path
# destination = path

# extension = 'csv'
# all_filenames = [i for i in glob.glob(f"*{extension}")]

# for file in all_filenames:
#     file_name = os.path.join(source, file)
#     shutil.move(file_name, destination)


# outputs_path = path
# os.chdir(outputs_path)

# extension = 'csv'
# all_filenames = [i for i in glob.glob(f"*{extension}")]

# def x(f):
#     try:
#         try:
#             z=pd.read_csv(f)
#         except:
#             z=pd.read_csv(f,encoding='utf-8')
#     except:
#         z=pd.read_csv(f,encoding='latin1')
#     return z

# combined_csv = pd.concat([ x(i) for i in all_filenames ])
# output_path = path + os.path.sep + "files_combined_{}.csv" 
# combined_csv.to_csv(output_path.format(pd.datetime.now().strftime("%Y%m%d%H%M%S")), index=False)

