import pandas as pd
import paramiko
from sqlalchemy import create_engine
import cx_Oracle
import logging

# Logging confiruration
from Configuration.ETLconfigs import *

logging.basicConfig(
    filename = "Logs/ETLLogs.log",
    filemode = "a", #  a  for append the log file and w for overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


'''
oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
'''

class CommomUtilities:

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


    def Sales_Data_From_Linux_Server(self):
        """Download sales data from a remote Linux server via SSH/SFTP."""
        logger.info("Extraction of linux file started ...")
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname, username=username, password=password)
            sftp = ssh_client.open_sftp()
            sftp.get(remote_file_path, local_file_path)
            sftp.close()
            ssh_client.close()
            logger.info("Extraction of linux file completed ...")
        except Exception as e:
            logger.error(f"Test failed due to SSH/SFTP error: {e}", exc_info=True)
