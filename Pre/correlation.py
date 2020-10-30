import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# 참고 링크
# https://julian-west.github.io/blog/visualising-asset-price-correlations/

# correlation matrix 기반으로 출력

# 주요 변수
period = 90 # 최근 차트 길이
min_etf = 3  # 그룹군 안에 포함된 ETF 최소개수
#today = datetime.now()
today = datetime(2020,10,7)
corr_threshold = 0.5

### DB 연결

# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
project_str = 'mysql+mysqldb://admin:1234@13.124.54.4:59791/Indices'

start = today-timedelta(days=period)

project_db= create_engine(project_str).connect()

#선정 종목 가져오기
query = '''
        select * from Indices.clusteringkr         
        '''
selected_df=pd.read_sql(query,con=project_db)


selected_etf = selected_df.name
selected_etf = '"' + selected_etf + '"'
selected_list = ','.join(selected_etf)

# 선정 종목 price 가져오기
query = '''
        select * from Indices.kr 
        where Date(Date) >= '{}'
        and name in ({})
        '''.format(start, selected_list)

etf_df=pd.read_sql( query,con=project_db)
project_db.close()

etf_df['Date'] = etf_df['Date'].dt.date
etf_df = etf_df.rename(columns={'name':'etf'})
df= pd.pivot_table(etf_df, values='Change', index=['Date'],
                    columns=['etf'], aggfunc='mean')
df = df.dropna()

# Corr Matrix
corr_df = df.corr()

input_etf = 'KODEX 200'
corr = corr_df.loc[:,corr_df.columns==input_etf]
high_corr = corr.sort_values(input_etf, ascending=False)[1:6]
round(high_corr,3)
print(high_corr.loc['KODEX 레버리지','KODEX 200'])
low_corr = corr.sort_values(input_etf)[:5]

# NetworkX Plot
# edges = corr_df.stack()
# edges = pd.DataFrame(edges)
# edges['etf1'] = edges.index.get_level_values(0)
# edges['etf2'] = edges.index.get_level_values(1)
# edges.index = range(len(edges))
# edges = edges.rename(columns = {0:'correlation'})
# edges = edges.filter(['etf1', 'etf2', 'correlation'])

# #remove self correlations
# edges = edges.query('etf1!=etf2')

# #create undirected graph with weights corresponding to the correlation magnitude
# G = nx.from_pandas_edgelist(edges, 'etf1', 'etf2', edge_attr=['correlation'])

# # correlation 낮은 거 지우기
# # list to store edges to remove
# remove = []
# # loop through edges in Gx and find correlations which are below the threshold
# for etf1, etf2 in G.edges():
#     corr = G[etf1][etf2]['correlation']
#     #add to remove node list if abs(corr) < threshold
#     if abs(corr) < corr_threshold:
#         remove.append((etf1, etf2))

# # remove edges contained in the remove list
# G.remove_edges_from(remove)

# nx.draw(G, with_labels = True,node_size = 20, edge_color = 'c',
#         pos=nx.random_layout(G))
# plt.show()# -*- coding: utf-8 -*-

# high_corr.index.tolist()
# high_corr.values.flatten().tolist()
