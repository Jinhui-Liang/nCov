# Import
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
import pickle
from _collections import defaultdict

# Load
with open('mi.txt','rb') as f:
    din = pickle.load(f)

with open('dxy_c_t.txt','rb') as f:
    dxy_c_t = pickle.load(f)

# Get the list of cities
tempc = dxy_c_t.reset_index()
tempc = tempc['city']
tempc = tempc.drop_duplicates()
tempc = list(tempc)

day1 = dt.date(2020, 1, 1)
dayend = list(din.keys())[-1]
dayend = datetime.strptime(dayend, "%Y%m%d")
dayend = dayend.date()
period = pd.date_range(start=day1, end=dayend)
period = period.date
day1s = datetime.strftime(day1, "%Y%m%d")
dayends = datetime.strftime(dayend, "%Y%m%d")

mi = dict.fromkeys(period)  #dictionary to store the whole stuff

for dayn in period:
    temp = din[datetime.strftime(dayn, "%Y%m%d")]  #each day dictionary
    ldin = list(temp.keys())
    lic = list()
    mid = {}
    for city1 in tempc:
        value = 0
        mis = pd.DataFrame(index=tempc)  #influx matrix for each city
        mis['ratio'] = 0
        res = [s for s in ldin if city1 in s]  #check if the city in mi has influx data
        if len(res)>0:
            tempdata = temp[res[0]]  #the influx data for city1
            for c in mis.index:
                res1 = [s for s in tempdata['city'] if c in s]
                if len(res1)>0:
                    value = tempdata['value'][tempdata['city']==res1[0]].iloc[0]
                    mis.loc[c,'ratio'] = float(value)
        else:
            mis = np.nan
        lic.append(mis)
    for key, value in zip(tempc, lic):
        mid[key] = value
    mid = dict(zip(tempc,lic))
    mi[dayn] = mid

# swapping the dictionary level
im = defaultdict(dict)
for d, dir in mi.items():
    for k, v in dir.items():
        im[k][d] = v

# fill in nan
for key, value in im.items():  #fill the empty last entry with second to last entry
    if not isinstance(im[key][period[-1]],pd.DataFrame):
        im[key][period[-1]] = im[key][period[-2]]

for key, value in im.items():
    for i in range(2,period.size):
        if not isinstance(im[key][period[-i]], pd.DataFrame):  #check for nan
            if isinstance(im[key][period[-i-1]], pd.DataFrame): #check for nan for previous one
                df_concat = pd.concat((im[key][period[-i-1]],im[key][period[-i+1]]))
                im[key][period[-i]] = pd.DataFrame(df_concat.mean())
            else:
                im[key][period[-i]] = im[key][period[-i+1]]

for key, value in im.items():  #fill the empty last entry with second to last entry
    if not isinstance(im[key][period[0]],pd.DataFrame):
        im[key][period[0]] = im[key][period[1]]

#organize the influx matrix
imx = pd.DataFrame(index=tempc, columns=period)
for p in tempc:
    temp = im[p]
    for d in period:
        imx[d] = temp[d]
        imx = imx.fillna(0)
        im[p] = imx


#process the influx scale data:
with open('inscale.txt','rb') as f:
    inscale = pickle.load(f)

isc = pd.DataFrame(index=tempc,columns=period)
lscale = list(inscale.keys())

for i in tempc:
    check = [s for s in lscale if i in s]
    if len(check)>0:
        tempdata = inscale[check[0]]
        i1 = tempdata.index[tempdata['date'] == day1s].tolist()[0]
        i2 = tempdata.index[tempdata['date'] == dayends].tolist()[0]
        s = tempdata[i1:i2+1]['value']
        for j in range(0,s.size):
            isc.loc[i,period[j]] = s.iloc[j]

#process the outflux scale data:
with open('outscale.txt','rb') as f:
    outscale = pickle.load(f)

osc = pd.DataFrame(index=tempc,columns=period)
lscale = list(outscale.keys())

for i in tempc:
    check = [s for s in lscale if i in s]
    if len(check)>0:
        tempdata = outscale[check[0]]
        i1 = tempdata.index[tempdata['date'] == day1s].tolist()[0]
        i2 = tempdata.index[tempdata['date'] == dayends].tolist()[0]
        s = tempdata[i1:i2+1]['value']
        for j in range(0,s.size):
            osc.loc[i,period[j]] = s.iloc[j]

#save
with open('im.txt', 'wb') as f:
    pickle.dump(im, f)

with open('isc.txt', 'wb') as f:
    pickle.dump(isc, f)

with open('osc.txt', 'wb') as f:
    pickle.dump(osc, f)