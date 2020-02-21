# Import
import pandas as pd
import re
import numpy as np
import datetime as dt
from datetime import datetime
from datetime import timedelta
import linearmodels
import pickle
from linearmodels.panel import PooledOLS
import statsmodels.api as sm

# Load
with open('im.txt','rb') as f:
    im = pickle.load(f)

with open('dxy_c_t.txt','rb') as f:
    dxy_c_t = pickle.load(f)

with open('isc.txt','rb') as f:
    isc = pickle.load(f)

with open('osc.txt','rb') as f:
    osc = pickle.load(f)

with open('dxy_c_n.txt','rb') as f:
    dxy_c_n = pickle.load(f)

with open('ccdc_t.txt','rb') as f:
    ccdc_t = pickle.load(f)

# construct the threat meter
dxy_c_t_c = dxy_c_t.xs('confirmed_c_t',axis=0,level=2)
dxy_c_t_c = dxy_c_t_c.reset_index(level=0,drop=True)

day1 = dxy_c_t_c.columns[0]
day1l = day1 + timedelta(days=5)
dayendl = dxy_c_t_c.columns[-1]
dayend = dayendl - timedelta(days=5)

li = []
tempc = list(im.keys())
for c in tempc:
    temp1 = dxy_c_t_c.loc[:,day1:dayend]
    temp2 = im[c].loc[:,day1l:dayendl]
    li.append(pd.DataFrame(temp1.values*temp2.values, columns=temp1.columns, index=temp2.index))

tm = dict(zip(tempc,li))

tmm = pd.DataFrame(index=tempc,columns=pd.date_range(start=day1,end=dayend).date)
isc = isc.astype(float)
for c in tempc:
    temp1 = tm[c]
    temp2 = isc.loc[c,day1:dayend]
    temp3 = np.array([temp2.values,]*len(tempc))
    res = np.multiply(temp1.values,temp3)
    tmm.loc[c] = np.sum(res,axis=0)

# prepare for panel data
period = pd.date_range(start=day1+timedelta(days=1), end=dayend)
period = period.date
yt = dxy_c_t_c.loc[:,day1+timedelta(days=1):dayend]
ytlag1 = dxy_c_t_c.loc[:,day1:dayend-timedelta(days=1)]
xt = tmm.loc[:,day1+timedelta(days=1):dayend]
xt = xt.fillna(0)

li = []
for p in period:
    temp = pd.DataFrame(yt.loc[:,p])
    temp['date'] = p
    temp = temp.set_index('date', append=True)
    temp = temp.rename({p:'y'},axis=1)
    li.append(temp)
yt = pd.concat(li,axis=0)

li = []
for p in period:
    temp = pd.DataFrame(ytlag1.loc[:,p-timedelta(days=1)])
    temp['date'] = p
    temp = temp.set_index('date', append=True)
    temp = temp.rename({p-timedelta(days=1):'ylag1'},axis=1)
    li.append(temp)
ytlag1 = pd.concat(li,axis=0)

li = []
for p in period:
    temp = pd.DataFrame(xt.loc[:,p])
    temp['date'] = p
    temp = temp.set_index('date', append=True)
    temp = temp.rename({p:'x'},axis=1)
    li.append(temp)
xt = pd.concat(li,axis=0)

panel = pd.concat([yt,ytlag1,xt],axis=1)

#regression
exog_vars = ['ylag1','x']
exog = sm.add_constant(panel[exog_vars])
mod = PooledOLS(panel.y, exog)
pooled_res = mod.fit()
print(pooled_res)


