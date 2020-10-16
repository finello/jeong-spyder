from sqlalchemy import create_engine
import pymysql 
import pandas as pd
import numpy as np
from dtaidistance import dtw



pymysql.install_as_MySQLdb()
engine = create_engine("mysql+mysqldb://admin:1234@13.124.54.4:59791/Indices")
conn = engine.connect()
query = '''
        select * from Indices.kr
       
        '''
etf_df=pd.read_sql(query ,con=conn)

etf_df = etf_df.set_index("Date") #Date로 index채움

# 1 3 5 7 9 12 14 16 18 20 23 25 27 29 31
#ticker중복처리
etf_names = list(etf_df.name.unique())

chart_data = pd.DataFrame()
ppp =0

for select_etf in etf_names:
    # select_etf = 'KODEX 200'
    print(select_etf)
    select_etf = [select_etf]
    select_df = etf_df[etf_df.name.isin(select_etf)] #isin - list로 input
    
    history_etf = select_etf
    #cal
    his_df = etf_df[etf_df.name.isin(history_etf)].reset_index().loc[:,['Date','Close']]
    his_df = his_df.sort_values('Date', ascending=False)
    
    
    # period만큼 재구성
    train_df = pd.DataFrame() #rtn df
    train_date_df = pd.DataFrame()#date of rtn df
    idx = 0
    step = 1
    period = 60
    while True:
        split = his_df[idx:idx+period]    
        split = split.reset_index()    
        if idx > len(his_df) or len(split)<period: break
        train_df = train_df.assign(idx = split['Close'] / split.tail(1)['Close'].values  -1)
        train_date_df = train_date_df.assign(idx = split['Date'])
        train_df = train_df.rename(columns = {'idx':idx})
        train_date_df = train_date_df.rename(columns = {'idx':idx})
        idx += step

    train_df = train_df.T#전치
    train_date_df = train_date_df.T
    train_np = train_df.to_numpy()
    
    ds = dtw.distance_matrix_fast(train_np, block=((0, 1), (1, len(train_np))), compact=True)
    ds_array = np.array(ds)
    
    ds_array = np.delete(ds_array,range(60),axis=0) #target 주변 삭제
    temp_value = []
    temp_index = []
    scale = period

    rtn_data = []
    num_of_hisdata = 5
    rtn_after_date = 5
    #get his data
    for i in range(num_of_hisdata):
        #dtw적용 value,index저장
        # i = 0
        temp_value.append(min(ds_array))
        temp_index.append(ds.index(temp_value[i]))
        #chart_data에 추가
        temp_data = train_df.loc[temp_index[i] +1]#ds에는 target없기 때문+1
        temp_date = train_date_df.loc[temp_index[i] +1]
             #chartdata
        chart_data['{0}_{1}'.format(select_etf[0],i)] = temp_data[::-1].reset_index(drop=True)
        chart_data['{0}_{1}_date'.format(select_etf[0],i)] = temp_date[::-1].reset_index(drop=True)
            #bar data
        buyprice = his_df[his_df['Date'] == chart_data['{0}_{1}_date'.format(select_etf[0],i)].tail(1).values[0]].Close.values[0]
        after_date_price=his_df.loc[his_df[his_df['Date'] == chart_data['{0}_{1}_date'.format(select_etf[0],i)].tail(1).values[0]].index.values[0] + rtn_after_date].Close
        rtn_data.append(after_date_price / buyprice -1)

        #ds_array에서 주변값 정리
        remove_idx = np.where(ds_array==temp_value[i])
        remove_idx = int(remove_idx[0])
        low_scope = int(remove_idx-scale/2)
        high_scope = int(remove_idx+scale/2)
        
        if low_scope<0:
             ds_array[0:high_scope] = None
    
        elif high_scope>len(ds_array):
             ds_array[low_scope:len(ds_array)] = None
             
    
        else:
             ds_array[low_scope:high_scope] = None
    while len(rtn_data) < len(chart_data):
        rtn_data.append(np.nan)
    chart_data['{0}_after'.format(select_etf[0],i)] = rtn_data
    ppp+=1
    if ppp >1:
        break
# chart_data['KODEX 200_0_date']
chart_data = chart_data.T

chart_data = chart_data.reset_index().rename(columns = {'index':'name'})

temp_name = chart_data.name.values.tolist()
chart_data = chart_data.drop(columns ='name')

#Timestamp to datetime
check =0
date_idx =1


for i in range(10):# for i in range(5xlen(etf_names)):

    chart_data.loc[date_idx] = pd.to_datetime(chart_data.loc[date_idx]).apply(lambda x: x.date())
    check +=1
    
    if check % 5 ==0:
        date_idx +=3
        continue
    date_idx+=2

# columns변경
for i in range(60):
    chart_data = chart_data.rename(columns = {i:'x{}'.format(i)})
chart_data['name'] = temp_name
chart_data.to_sql('krchartdata', if_exists='replace',con=conn)

# chart_data.loc[1] = pd.to_datetime(chart_data.loc[1])
# chart_data.loc[5] = pd.to_datetime(chart_data.loc[5]).apply(lambda x: x.date())
conn.close()

selected = 'KODEX 200'
i = 1
chart_data[chart_data.name.isin(['{0}_{1}'.format(selected,i)])].drop(columns ='name').values.flatten().tolist()
