import pandas as pd
import pymysql
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt
import time
from sklearn.cluster import AffinityPropagation
from datetime import datetime, timedelta

# 주요 변수
period = 60 # 최근 차트 길이
min_etf = 3  # 그룹군 안에 포함된 ETF 최소개수


### DB 연결

# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
project_str = 'mysql+mysqldb://admin:1234@13.124.54.4:59791/Indices'
#today = datetime.now()
today = datetime(2020,10,7)
start = today-timedelta(days=period)

project_db= create_engine(project_str).connect()

query = '''
        select * from Indices.kr 
        where Date(Date) >= '{}' 
        '''.format(start)

etf_df=pd.read_sql( query,con=project_db)
project_db.close()

etf_df['Date'] = etf_df['Date'].dt.date
df= pd.pivot_table(etf_df, values='Change', index=['name'],
                    columns=['Date'], aggfunc='mean')
df = df.dropna()
# 일별 수익률 계산

X = np.array(df.values)
clustering = AffinityPropagation().fit(X)
label = clustering.labels_
# clustering.predict()
# clustering.cluster_centers_
df['label'] = label
df_g=df.groupby(['label']).size().reset_index(name='count')
df_g = df_g.set_index('label')
df_g = df_g.sort_values('count', ascending = False)
df_g = df_g[df_g['count']>min_etf]

selected_group = df_g.index.to_list()

df_selected = df[df['label'].isin(selected_group)]
df_selected = df_selected.filter(['name', 'label'])

# 최근 거래량으로 그룹군 중에서 1개씩 선택
etf_df['Trading Value'] = etf_df['Close'] * etf_df['Volume']
recent_day = etf_df['Date'].max()
recent_df = etf_df.query('Date==@recent_day')
recent_df = recent_df.filter(['name', 'Trading Value'])

# 
full_df = pd.merge(df, recent_df, how='left', on='name')
final_df = full_df.loc[full_df.groupby('label')['Trading Value'].idxmax(), ['name', 'label', 'Trading Value']]
