import streamlit as st
import numpy as np
import pandas as pd
import altair as alt




data = pd.DataFrame(
    np.array(range(12)).reshape(4,3),
    columns=[1, 'b', 'c'])




line_chart = alt.Chart(data).mark_line().encode(x=1,y='c')
st.write('test')