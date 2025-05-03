# Home work to implement all the other function to have util function.


import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.ETLconfigs import *

from Utilities.ReadFiles import *

oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

class DataExtraction:

    def extract_sales_data(self,file_path,file_type,table_name,db_engine):
        print("Sales data extraction started...")
        read_files_and_write_to_stage(file_path,file_type,table_name,db_engine)
        print("Sales data extraction completed...")


    def extract_product_data(self):
        print("Product data extraction started...")
        df = pd.read_csv("Source_Systems/product_data.csv")
        df.to_sql("staging_product", mysql_engine, if_exists='replace', index=False)
        print("Product data extraction completed...")

    def extract_supplier_data(self):
        print("Supplier data extraction started...")
        df = pd.read_json("Source_Systems/supplier_data.json")
        df.to_sql("staging_supplier", mysql_engine, if_exists='replace', index=False)
        print("Supplier data extraction completed...")

    def extract_inventory_data(self):
        print("Inventory data extraction started...")
        df = pd.read_xml("Source_Systems/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventory", mysql_engine, if_exists='replace', index=False)
        print("Inventory data extraction completed...")

    def extract_stores_data(self):
        print("Stores data extraction started...")
        query =  """select * from stores"""
        df = pd.read_sql(query,oracle_engine)
        df.to_sql("staging_stores", mysql_engine, if_exists='replace', index=False)
        print("Stores data extraction completed...")

if __name__ == "__main__":
    extRef = DataExtraction()
    extRef.extract_sales_data("Source_Systems/sales_data.csv","csv","staging_sales",mysql_engine)
    extRef.extract_product_data()
    extRef.extract_supplier_data()
    extRef.extract_inventory_data()
    extRef.extract_stores_data()
