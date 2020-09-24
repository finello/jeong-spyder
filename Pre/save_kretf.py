import FinanceDataReader as fdr
import pandas as pd
import pymysql
from sqlalchemy import create_engine
# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from datetime import datetime
import numpy as np


 
nowDate = datetime.now().strftime('%Y-%m-%d')
start_date = '2010-01-01'
end_date = '2020-09-16'


kretf_name = fdr.EtfListing('KR').Name.values
kretf_symbol = fdr.EtfListing('KR').Symbol.values

temp_df = []
idx = 0
for kr_symbol in kretf_symbol :
    temp = fdr.DataReader(symbol=kr_symbol,start=start_date,end=end_date)
    
    temp['name'] = kretf_name[idx]
    temp_df.append(temp)
    idx +=1
    
etf_df = pd.concat(temp_df)



# DB 연결

project_str = 'mysql+mysqldb://admin:1234@13.125.20.104:57836/Indices'


# 프로젝트 DB 저장
project_db= create_engine(project_str).connect()

etf_df.to_sql('kr', if_exists='replace',con=project_db)

project_db.close()

