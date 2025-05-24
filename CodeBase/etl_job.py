from CodeBase.extract import DataExtraction
from CodeBase.load import DataLoading
from CodeBase.transform import DataTransformation
import pandas as pd
from sqlalchemy import create_engine, text
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



#oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")


oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

if __name__ == "__main__":
    DE = DataExtraction()
    DE.extract_sales_data("Source_Systems/sales_data_Linux_remote.csv", "csv", "staging_sales", mysql_engine)
    DE.extract_product_data()
    DE.extract_supplier_data()
    DE.extract_inventory_data()
    DE.extract_stores_data()

    DT = DataTransformation()
    DT.transform_filter_sales_data()
    DT.transform_router_sales_data_High_region()
    DT.transform_router_sales_data_Low_region()
    DT.transform_aggregator_sales_data()
    DT.transform_Joiner_sales_product_stores()
    DT.transform_aggregator_inventory_level()
    DT.transform_Joiner_sales_product_stores()

    DL = DataLoading()
    DL.load_fact_sales_table()
    DL.load_fact_inventory_table()
    DL.load_monthly_sales_summary_table()
    DL.load_inventory_level_by_store_table()
