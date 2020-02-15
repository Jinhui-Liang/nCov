
# Import
import pandas as pd
import pickle
import numpy as np
import datetime as dt
from datetime import datetime
import matplotlib.pyplot as plt

# Reading the Data
with open('ccdc.txt','rb') as f:
    ccdc = pickle.load(f)
dxy = pd.read_csv('DXYArea.csv')
# Data Wrangling, clean up the date
dxy['updateTime'] = pd.to_datetime(dxy['updateTime'])
date = pd.DataFrame({'year': dxy['updateTime'].dt.year,
                     'month': dxy['updateTime'].dt.month,
                     'day': dxy['updateTime'].dt.day})
dxy['date'] = pd.to_datetime(date, unit='D')
dxy = dxy.drop(['updateTime'], axis=1)

# Wrangling, get a province-level data
dxy_pv = dxy[['provinceName','province_confirmedCount','province_curedCount','province_deadCount','date']]
dxy_pv = dxy_pv.drop_duplicates()
period = dxy_pv['date'].drop_duplicates()
period = sorted(period)
lit = []
li = []
for t in period:
    d = t.date()
    temp = dxy_pv[dxy_pv['date'] == t]
    temp = temp.drop(columns='date')
    temp = temp.sort_values('province_confirmedCount').drop_duplicates('provinceName', keep='last')
    temp = temp.set_index('provinceName')
    temp = temp.stack()
    lit.append(d)
    li.append(temp)
dxy_pv = pd.concat(li, axis=1,)
dxy_pv.columns = lit

# Dealing with NaN
dxy_pv.iloc[:,0].fillna(0, inplace=True)
for l in range(1,dxy_pv.shape[1]):
    index = dxy_pv.iloc[:,l].index[dxy_pv.iloc[:,l].apply(np.isnan)]
    col1 = dxy_pv.columns[l]
    col2 = dxy_pv.columns[l-1]
    for i in index:
        dxy_pv.loc[i,col1] = dxy_pv.loc[i,col2]
dxy_pv[dxy_pv<0] = 0

# Derive the new cases daily
ddxy_pv = dxy_pv
for l in range(1,dxy_pv.shape[1]):
    ddxy_pv.iloc[:,l] = dxy_pv.iloc[:,l]-dxy_pv.iloc[:,l-1]
ddxy_pv[ddxy_pv<0] = 0
