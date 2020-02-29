#!/usr/bin/env python
# coding: utf-8

# In[37]:


# import
import pandas as pd
import json
import pickle
import os
import re
from datetime import datetime

rx_dict ={'date': re.compile(r'yq_(\d\d\d\d\d\d\d\d).json')}
dateli = []
ccdcli = [] 
ccdc = []
new_index = ['suspected_n','suspected_t','confirmed_n','confirmed_t','dead_n','dead_t']
for filename in os.listdir('/home/jinhui/Github/nCov/ccdc'):
    date = re.findall(rx_dict['date'],filename)
    dateli.append(date[0])
    with open('/home/jinhui/Github/nCov/ccdc/%s'%filename, encoding='utf-8') as f:
        ccdcli.append(json.load(f))

for i in range(len(dateli)):
    date = datetime.strptime(dateli[i],'%Y%m%d')
    date = date.date()
    cdc = ccdcli[i]
    li = []
    lin = []
    for j in range(0, 34):
        temp = cdc['features'][j]['properties']
        tempname = cdc['features'][j]['properties']['name']
        dftemp = pd.Series(temp, index=['新增疑似','累计疑似','新增确诊','累计确诊','新增死亡','累计死亡'])
        dftemp = dftemp.reset_index(drop=True)
        dftemp.index = new_index
        li.append(dftemp)
        lin.append(tempname)
    dfd = pd.DataFrame(li,index=lin)
    dfds = dfd.stack()
    dfds = dfds.to_frame(name=date)
    ccdc.append(dfds)
ccdc = pd.concat(ccdc, axis=1)
ccdc.index = pd.MultiIndex.from_tuples(ccdc.index.values, names=['provinces', 'cases'])

# seperate the new cases and total cases
list_t = [ccdc.xs('confirmed_t',axis=0, level=1,drop_level=False),ccdc.xs('suspected_t',axis=0, level=1,drop_level=False),ccdc.xs('dead_t',axis=0, level=1,drop_level=False)]
list_n = [ccdc.xs('confirmed_n',axis=0, level=1,drop_level=False),ccdc.xs('suspected_n',axis=0, level=1,drop_level=False),ccdc.xs('dead_n',axis=0, level=1,drop_level=False)]
ccdc_n = pd.concat(list_n, axis=0)
ccdc_t = pd.concat(list_t, axis=0)

## Fixing the 01-15
for i in range(0, 102):
    ccdc_t.iloc[i,5] = ccdc_t.iloc[i,6]-ccdc_n.iloc[i,6]
    ccdc_n.iloc[i,5] = ccdc_t.iloc[i,5]-ccdc_t.iloc[i,4]

# Save
with open('ccdc_n.txt', 'wb') as f:
    pickle.dump(ccdc_n, f)
with open('ccdc_t.txt', 'wb') as f:
    pickle.dump(ccdc_t, f)
    

