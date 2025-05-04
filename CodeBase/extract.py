# Home work to implement all the other function to have util function.
# Read about levels of logging ( 8 levels of logging )
# Replace all the print with logger
# complete the logging mechanism for all the other functions
# Parametrize the linux download function with remote and local file path
# implement all the load function in load.py file ( 3 remaining function to be implemented )


import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.ETLconfigs import *

from Utilities.Utils import *
import logging

# Logging confiruration
logging.basicConfig(
    filename = "Logs/ETLLogs.log",
    filemode = "w", #  a  for append the log file and w for overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)



oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

class DataExtraction:

    def extract_sales_data(self,file_path,file_type,table_name,db_engine):
        logger.info("Sales data extraction started...")
        try:
            CommomUtilities.read_files_and_write_to_stage(file_path,file_type,table_name,db_engine)
            logger.info("Sales data extraction completed")
        except Exception as e:
            logger.error("Error while sales data extraction..",e,exc_info=True)


    def extract_product_data(self):
        logger.info("Product data extraction started...")
        try:
            df = pd.read_csv("Source_Systems/product_data.csv")
            df.to_sql("staging_product", mysql_engine, if_exists='replace', index=False)
            logger.info("product data extraction completed")
        except Exception as e:
            logger.error("Error while product extraction..", e, exc_info=True)

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
    # pre-reqisite ( extract sales_data.csv file from Linux server )
    logger.info("file getting extracted ...")
    pre_req_ref = CommomUtilities()
    # download the linix file to local/project
    pre_req_ref.Sales_Data_From_Linux_Server()
    logger.info("file getting completted ...")

    extRef = DataExtraction()
    extRef.extract_sales_data("Source_Systems/sales_data_Linux_remote.csv","csv","staging_sales",mysql_engine)
    extRef.extract_product_data()
    extRef.extract_supplier_data()
    extRef.extract_inventory_data()
    extRef.extract_stores_data()
