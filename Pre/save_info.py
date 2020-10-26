import pandas as pd
import pymysql
from sqlalchemy import create_engine




info_df = pd.read_csv('C:/Users/15U780/Documents/GitHub/jeong1/Pre/final_etf_info.csv',encoding='CP949')


pymysql.install_as_MySQLdb()
project_str = 'mysql+mysqldb://admin:1234@13.124.54.4:59791/Indices'
project_db= create_engine(project_str).connect()


info_df.to_sql('clusteringkr_info', if_exists='replace',con=project_db)

project_db.close()