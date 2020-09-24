import pandas as pd
import pymysql 
from sqlalchemy import create_engine
import streamlit as st
import numpy as np
from dtaidistance import dtw
#import MySQLdb
# from bokeh.plotting import figure


#DB
@st.cache
def get_df():
    #MySQL Connector using pymysql
    pymysql.install_as_MySQLdb()
    
    engine = create_engine("mysql+mysqldb://admin:1234@3.34.43.57:51646/Indices")
    conn = engine.connect()

    query = '''
            select * from Indices.world
    
            '''
    df=pd.read_sql(query ,con=conn)
    conn.close()
    return df
etf_df = get_df()
etf_df = etf_df.set_index("Date")
#df
#ticker중복처리
tickers = etf_df.ticker.unique()
tickers = list(tickers)
#streamlit

#select symbol
select_symbol = st.multiselect('Select symbol' , tickers , ['^GSPC'])


select_df = etf_df[etf_df.ticker.isin(select_symbol)]
select_df.drop(columns='index')
#display df
st.write("*DataFrame*",select_df)

#chart
#column조정
_columns = etf_df.drop(columns=['ticker','index']).columns

#select column
select_column = st.selectbox('Select column',options=_columns)

st.write("*{0}*  ~ {1}".format(select_df.index.min(),select_df.index.max()))
#chart생성
chart_data = pd.DataFrame()

for i in select_symbol:
    temp_df=etf_df[etf_df.ticker.isin([i])]
    chart_data[i] = temp_df[select_column]
    
#display chart
st.line_chart(data=chart_data)


#select history ticker
history_ticker = st.selectbox('Select ticker(show history)',options=tickers)

#cal
his_df = etf_df[etf_df.ticker.isin(['^GSPC'])].reset_index().loc[:,['Date','Adj Close']]
is_true= his_df['Date'] > pd.to_datetime('2010-01-01')
his_df = his_df[is_true].reset_index().drop(columns='index')
his_df = his_df.sort_values('Date', ascending=False)


# period만큼 재구성
train_df = pd.DataFrame()
idx = 0
step = 1
period = 60
while True:
    split = his_df[idx:idx+period]    
    split = split.reset_index()    
    if idx > len(his_df) or len(split)<period: break
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
rtn_store = {}
scale = period
bar_df = pd.DataFrame()

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

    elif high_scope>len(ds_array):
         ds_array[low_scope:len(ds_array)] = None
         

    else:
         ds_array[low_scope:high_scope] = None
         


for i in range(10):

    start_date = his_df.loc[len(his_df)-1 - store['min_df{}'.format(i)].name,'Date']
    end_date = his_df.loc[len(his_df)-1 - store['min_df{}'.format(i)].name + 60,'Date']
    st.write('*{0}  ~ {1}*'.format(start_date,end_date ))
    st.line_chart(store['min_df{}'.format(i)])
    
    
    buyprice = his_df.loc[len(his_df)-1 - store['min_df{}'.format(i)].name +60, 'Adj Close']
    
    five_days = his_df.loc[len(his_df)-1 - store['min_df{}'.format(i)].name +65, 'Adj Close']
    rtn_store['min_df{}'.format(i)] =pd.Series( five_days/buyprice -1,index=[i])

    bar_df = pd.concat([bar_df,rtn_store['min_df{}'.format(i)]],axis=0)
st.bar_chart(bar_df)
























