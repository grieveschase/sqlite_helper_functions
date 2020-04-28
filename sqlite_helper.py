
import sqlite3
'''
Chase Grieves March 2020
sqlite3 database helper functions, allows for quick setup and population of sqlite database.
fxn db_init() Inputs:
db_file - file name/path of sqlite data file.
db_table - table name.
db_col_names - list of Column names.
db_col_types - list of the data type for each column. Follows ordered list db_col_names
*note:     assert len(db_col_names) == len(db_col_types)
db_col_unique - sublist of db_col_names of which columns to have a unique constraint.

fxn insert_data_db() Inputs:
db_file - file name/path of sqlite data file.
db_table - table name.
db_col_names - list of Column names.
data - ordered list of data to insert into db, following order of db_col_names.

'''
def db_init(db_file, db_table, db_col_names, db_col_types,db_col_unique=None):
    #setup init string that creates database
    assert len(db_col_names) == len(db_col_types)
    if db_col_unique:
        col_define ="( "+ " NOT NULL, ".join(x + " " + y for  x,y in zip(db_col_names, db_col_types)) + " NOT NULL, "
        unique_define = ",".join(db_col_unique)
        sql_init = "CREATE TABLE IF NOT EXISTS "+db_table+" "+col_define+"unique("+unique_define+"));"
    else:
        col_define ="( "+ " NOT NULL, ".join(x + " " + y for  x,y in zip(db_col_names, db_col_types)) + " NOT NULL "
        sql_init = "CREATE TABLE IF NOT EXISTS "+db_table+" "+col_define+");"
    try:
        conn = sqlite3.connect(db_file, detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        cur.execute(sql_init)
        conn.commit()
        cur.close()
        if db_col_unique:
            print("database %s created with table: %s"%(db_file, db_table))
            print("database contains unique column restraints")
        else:
            print("database %s created with table: %s"%(db_file, db_table))
            print("database does NOT contain unique column restraints")
    except sqlite3.Error as error:
        print("SQLite error: ", error)
        print(sql_init)
    finally:
        if (conn):
            conn.close()

def insert_data_db(db_file, db_table, db_col_names, data, print_output):
    assert len(db_col_names) == len(data)
    insert_start_col = "'"+ "','".join(db_col_names)+"'"
    insert_value_placeholders = ",".join(['?']*len(db_col_names))
    sql_insert = "INSERT INTO '"+db_table+"'("+insert_start_col+") VALUES("+insert_value_placeholders+");"
    try:
        conn = sqlite3.connect(db_file, detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        cur.execute(sql_insert, data)
        conn.commit()
        cur.close()
        if print_output:
            print("Dat data added")
    except sqlite3.Error as error:
        print("SQLite error: ", error)
    finally:
        if (conn):
            conn.close()
if __name__ == "__main__":


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

    db_file = "test.db"
    db_table = "test_table"


    data = ["yeet"] * len(db_col_names)

    insert_data_db(db_file, db_table, db_col_names, data)










#
