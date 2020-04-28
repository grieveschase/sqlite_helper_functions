import sqlite3
from sqlite_helper import *
import glob
import os
import pandas as pd
import datetime

db_file = "sem_data.db"
db_table = "sem_data"
db_col_names = [
'Site',
'EP',
'Top',
'Main_Slope',
'Bottom',
'Algorithm',
'Algo_Type',
'ID_X',
'ID_Y',
'Location_X',
'Location_Y',
'Target_Name',
'Recipe',
'Lot',
'Slot_ID',
'Date',
'LC_X',
'LC_Y',
'LocationX_field',
'LocationY_field',
'Meas_Params_Name',
'Tool_ID',
'PR_Vector_X',
'PR_Vector_Y',
'PR_Success',
'Recipe_Process',
'Recipe_Product',
'Recipe_Layer',
'Field_Order',
'LC_Order']
db_col_types = [
'TEXT',
'TEXT',
'REAL',
'REAL',
'REAL',
'TEXT',
'TEXT',
'INTEGER',
'INTEGER',
'REAL',
'REAL',
'TEXT',
'TEXT',
'TEXT',
'INTEGER',
'TIMESTAMP',
'INTEGER',
'INTEGER',
'REAL',
'REAL',
'TEXT',
'TEXT',
'REAL',
'REAL',
'REAL',
'TEXT',
'TEXT',
'TEXT',
'INTEGER',
'INTEGER']
db_col_unique = [
'Site',
'EP',
'Top',
'Main_Slope',
'Bottom',
'Algorithm',
'Algo_Type',
'ID_X',
'ID_Y',
'Location_X',
'Location_Y',
'Target_Name',
'Recipe',
'Lot',
'Slot_ID',
'LC_X',
'LC_Y',
'LocationX_field',
'LocationY_field',
'Meas_Params_Name',
'Tool_ID',
'PR_Vector_X',
'PR_Vector_Y',
'PR_Success',
'Recipe_Process',
'Recipe_Product',
'Recipe_Layer',
'Field_Order',
'LC_Order']
db_init(db_file, db_table, db_col_names, db_col_types,db_col_unique)

file_path = "E://ufiles//F4PHOTO//Opal_Data//this_month//"
#file_path ="C://Users//CCAG//Desktop//test_cst//"
cst_files = glob.glob(file_path +"*.cst*")
print("Total Files Found: " + str(len(cst_files)))
count = 0

for cst_file in cst_files:
    mtime = os.path.getmtime(cst_file)
    file_date = datetime.datetime.fromtimestamp(mtime)
    time_delta = datetime.datetime.now() - file_date
    count += 1
    if count%100 == 0:
        print("Total completed %s of %s"%(str(count),str(len(cst_files))))
    if time_delta.days < 10:
        cst_data = []
        with open(cst_file,'r') as o_file:
            for line in o_file:
                if line.split():
                    cst_data.append(line.split())
        try:
            pd_df = pd.DataFrame(cst_data[1:], columns = cst_data[0])
            db_head = ['Site','EP','Top','Main_Slope','Bottom','Algorithm','Algo_Type','F.ID_X','F.ID_Y','Location_X','Location_Y','Target_Name','Recipe','Lot','Slot_ID','Date','LC_X','LC_Y','LocationX_(field)','LocationY_(field)','Meas_Params_Name','Tool_ID','PR_Vector_X','PR_Vector_Y','PR_Success','Recipe_Process','Recipe_Product','Recipe_Layer','Field_Order','LC_Order']
            pd_db = pd_df[db_head]
            db_head_real = ['Top','Main_Slope','Bottom','Location_X','Location_Y','LocationX_(field)','LocationY_(field)','PR_Vector_X','PR_Vector_Y','PR_Success']
            db_head_int = ['F.ID_X','F.ID_Y','Slot_ID','LC_X','LC_Y','Field_Order','LC_Order']
            db_head_date = ['Date']

            for tic in range(pd_db.shape[0]):
                temp_list = pd_db.iloc[tic]
                for head in db_head_real:
                    try:
                        temp_list[head] = float(temp_list[head])
                    except:
                        _=True
                for head in db_head_int:
                    try:
                        temp_list[head] = int(temp_list[head])
                    except:
                        _=True

                temp_list['Date'] = datetime.datetime.strptime(temp_list['Date'],"%m/%d/%Y-%H:%M:%S")

            for tic in range(pd_db.shape[0]):
                temp_list = list(pd_db.iloc[tic])
                insert_data_db(db_file, db_table, db_col_names, temp_list, print_output = False)
        except:
            print("Ducked up CST File")
            print(cst_file)
            _=True
print('Done')
