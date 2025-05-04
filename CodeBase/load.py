# Home work to implement all the other function to have util function.
# Read about levels of logging ( 8 levels of logging )
# Replace all the print with logger
# complete the logging mechanism for all the other functions
# Parametrize the linux download function with remote and local file path


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





if __name__ == "__main__":
    loadRef = DataLoading()
    loadRef.load_fact_sales_table()

# implement all the other load fucntion