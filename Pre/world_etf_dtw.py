import pandas as pd
import pymysql
from sqlalchemy import create_engine
import numpy as np
# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
# import MySQLdb
from dtaidistance import dtw
import matplotlib.pyplot as plt
# import matplotlib
import time
# DB 연결
project_str = 'mysql+mysqldb://admin:1234@3.34.43.57:51646/Indices'
indice = '^GSPC'
start = '2010-01-01'
end = '2020-09-04'
period = 60

project_db= create_engine(project_str).connect()

start_time = time.time()
# 데이터 읽기
query = '''
        select Date, `Adj Close` from Indices.world 
        where Date(Date) >= '{}' and ticker = '{}'
        '''.format(start, indice)

etf_df=pd.read_sql( query,con=project_db)
# DB종료
project_db.close()

etf_df = etf_df.sort_values('Date', ascending=False)

# period만큼 재구성
train_df = pd.DataFrame()
idx = 0
step = 1
while True:
    split = etf_df[idx:idx+period]    
    split = split.reset_index()    
    if idx > len(etf_df) or len(split)<period: break
    train_df = train_df.assign(idx = split['Adj Close'] / split.tail(1)['Adj Close'].values  -1)
    train_df = train_df.rename(columns = {'idx':idx})
    idx += step
    
train_df = train_df.T#전치
train_np = train_df.to_numpy()

ds = dtw.distance_matrix_fast(train_np, block=((0, 1), (1, len(train_np))), compact=True)
ds_array = np.array(ds)

ds_array = np.delete(ds_array,range(60),axis=0) #target 주변 삭제
temp_value = []
temp_index = []
store = {}
scale = period


#get 10
for i in range(10):

    temp_value.append(min(ds_array))
    temp_index.append(ds.index(temp_value[i]))
    temp_1 = train_df.loc[temp_index[i] +1]#ds에는 target없기 때문+1
    store['min_df{}'.format(i)] = temp_1[::-1].reset_index(drop=True)
    
    remove_idx = np.where(ds_array==temp_value[i])
    remove_idx = int(remove_idx[0])
    low_scope = int(remove_idx-scale/2)
    high_scope = int(remove_idx+scale/2)
    if low_scope<0:
         ds_array[0:high_scope] = None
         
         
         # = np.delete(
         #                 ds_array,
         #                 range(0,high_scope),
         #                 axis=0
         #                )
    elif high_scope>len(ds_array):
         ds_array[low_scope:len(ds_array)] = None
         
         
         # = np.delete(
         #                 ds_array,
         #                 range(low_scope,len(ds_array)),
         #                 axis=0
         #                )
    else:
         ds_array[low_scope:high_scope] = None
         
         
         # = np.delete(
         #                 ds_array,
         #                 range(low_scope,high_scope),
         #                 axis=0
         #                )



#target_df.plot
print('time:',time.time()-start_time)
target_df = train_df.loc[0]
plt.plot(target_df)
plt.xlabel('Date')
plt.ylabel('rtn')
plt.title('target_df')
plt.show()
#min_df.plot
for i in range(10):
    plt.plot(store['min_df{}'.format(i)])
    plt.xlabel('Date')
    plt.ylabel('rtn')
    plt.title('min_df'+str(i))
    plt.show()
#get rtn after 5-days
rtn_store={}
for i in range(10):
    
    buyprice = etf_df.loc[len(etf_df)-1 - store['min_df{}'.format(i)].name, 'Adj Close']
    
    five_days = etf_df.loc[len(etf_df)-1 - store['min_df{}'.format(i)].name +5, 'Adj Close']
    rtn_store['min_df{}'.format(i)] = five_days/buyprice -1
#rtn.plot(bar)

plt.bar(x=rtn_store.keys(),height=rtn_store.values())
plt.show()


# DB저장
# ds_df = pd.DataFrame()




# test_str = 'mysql+mysqldb://admin:1234@3.34.43.57:51646/Indices'
# test_db= create_engine(test_str).connect()
# .to_sql('dtw', if_exists='replace',con=project_db)

#streamlit

