# Home work to implement all the other function to have util function.
# Read about levels of logging ( 8 levels of logging )
# Replace all the print with logger
# complete the logging mechanism for all the other functions
# Parametrize the linux download function with remote and local file path
# implement all the other load fucntion

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

class DataLoading:

    def load_fact_sales_table(self):
        logger.info("Loading for fact_sales started...")
        query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                        select sales_id,product_id,store_id,quantity,sales_amount,sale_date from sales_with_details""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_sales table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_sales table loading completed...")
        except Exception as e:
            logger.error(f"Error while loading fact_sales table {e}",exc_info=True)

    def load_fact_inventory_table(self):
        logger.info("Loading for Fact_inventory started...")
        query = text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated) 
                        select product_id,store_id,quantity_on_hand,last_updated from staging_inventory""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_inventory table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_inventory table loading completed...")
        except Exception as e:
            logger.error(f"Error while loading Fact_inventory table {e}",exc_info=True)

    def load_monthly_sales_summary_table(self):
        logger.info("Loading for monthly_sales_summary started...")
        query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales) 
                        select product_id,month,year,total_sales from monthly_sales_summary_source""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("monthly_sales_summary table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("monthly_sales_summary table loading completed...")
        except Exception as e:
            logger.error(f"Error while loading monthly_sales_summary table {e}",exc_info=True)

    def load_inventory_level_by_store_table(self):
        logger.info("Loading for inventory_level_by_store started...")
        query = text("""insert into inventory_levels_by_store(store_id,total_inventory) 
                        select store_id,total_inventory from aggegated_inventory_level;""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("inventory_level_by_store table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("inventory_level_by_store table loading completed...")
        except Exception as e:
            logger.error(f"Error while loading inventory_level_by_store table {e}",exc_info=True)




if __name__ == "__main__":
    loadRef = DataLoading()
    loadRef.load_fact_sales_table()
    loadRef.load_fact_inventory_table()
    loadRef.load_monthly_sales_summary_table()
    loadRef.load_inventory_level_by_store_table()

