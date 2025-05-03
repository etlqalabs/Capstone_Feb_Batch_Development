import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle


'''
oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
'''

def read_files_and_write_to_stage(file_path,file_type,table_name,db_engine):
    if file_type =='csv':
        df = pd.read_csv(file_path)
    elif file_type == 'json':
        df = pd.read_json(file_path)
    elif file_type == 'xml':
        df = pd.read_xml(file_path,xpath='.//item')
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")
    df.to_sql(table_name, db_engine, if_exists='replace', index=False)

