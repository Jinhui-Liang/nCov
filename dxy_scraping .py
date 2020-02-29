#!/usr/bin/env python
# coding: utf-8

# In[1]:



# Import
import pandas as pd
import pickle
import numpy as np
import copy

# Reading the Data
dxy = pd.read_csv('DXYArea.csv')
# Data Wrangling, clean up the date, renaming; _c means city level; _t means total; _n means addition
dxy['updateTime'] = pd.to_datetime(dxy['updateTime'])
date = pd.DataFrame({'year': dxy['updateTime'].dt.year,
                     'month': dxy['updateTime'].dt.month,
                     'day': dxy['updateTime'].dt.day})
dxy['date'] = pd.to_datetime(date, unit='D')
dxy = dxy.drop(['province_zipCode','city_zipCode','updateTime','province_suspectedCount','city_suspectedCount','provinceEnglishName','cityEnglishName'], axis=1)
dxy = dxy.rename(columns={'provinceName':'province','cityName':'city','province_confirmedCount':'confirmed_t','city_confirmedCount':'confirmed_c_t','province_curedCount':'cured_t','city_curedCount':'cured_c_t','province_deadCount':'dead_t','city_deadCount':'dead_c_t'})

# Wrangling, get a province-level data
dxy_p_t = dxy[['province','confirmed_t','cured_t','dead_t','date']]
dxy_p_t = dxy_p_t.sort_values('confirmed_t').drop_duplicates(keep='last')
period = dxy_p_t['date'].drop_duplicates()
period = sorted(period)
lit = []
li = []
for t in period:
    d = t.date()
    temp = dxy_p_t[dxy_p_t['date'] == t]
    temp = temp.drop(columns='date')
    temp = temp.sort_values('confirmed_t').drop_duplicates('province', keep='last')
    temp = temp.set_index('province')
    temp = temp.stack()
    lit.append(d)
    li.append(temp)
dxy_p_t = pd.concat(li, axis=1,)
dxy_p_t.columns = lit

# Dealing with NaN
dxy_p_t.iloc[:,0].fillna(0, inplace=True)
for l in range(1,dxy_p_t.shape[1]):
    index = dxy_p_t.iloc[:,l].index[dxy_p_t.iloc[:,l].apply(np.isnan)]
    col1 = dxy_p_t.columns[l]
    col2 = dxy_p_t.columns[l-1]
    for i in index:
        dxy_p_t.loc[i,col1] = dxy_p_t.loc[i,col2]
dxy_p_t[dxy_p_t<0] = 0
dxy_p_t.index = dxy_p_t.index.rename('case', level=1)


# Derive the new cases daily
dxy_p_n = pd.DataFrame(index=dxy_p_t.index,columns=dxy_p_t.columns)
dxy_p_n.iloc[:,0] = dxy_p_t.iloc[:,0]
for l in range(1,dxy_p_t.shape[1]):
    dxy_p_n.iloc[:,l] = dxy_p_t.iloc[:,l]-dxy_p_t.iloc[:,l-1]
dxy_p_n[dxy_p_n<0] = 0
dxy_p_n = dxy_p_n.reset_index()
dxy_p_n = dxy_p_n.rename(columns={'level_1':'case'})
dxy_p_n = dxy_p_n.replace(to_replace='confirmed_t',value='confirmed_n')
dxy_p_n = dxy_p_n.replace(to_replace='cured_t',value='cured_n')
dxy_p_n = dxy_p_n.replace(to_replace='dead_t',value='dead_n')
dxy_p_n = dxy_p_n.set_index(['province','case'])

#city level data
dxy_c_t = copy.deepcopy(dxy)
citylist = copy.deepcopy(dxy_c_t[{'province','city'}])
citylist = citylist.drop_duplicates()
dxy_c_t = dxy_c_t[dxy_c_t['province'] != '上海市']
dxy_c_t = dxy_c_t[dxy_c_t['province'] != '重庆市']
dxy_c_t = dxy_c_t[dxy_c_t['province'] != '天津市']
dxy_c_t = dxy_c_t[dxy_c_t['province'] != '北京市']
dxy_c_t = dxy_c_t[np.logical_not(dxy_c_t['city'].str.contains('明确'))]
dxy_c_t = dxy_c_t[np.logical_not(dxy_c_t['city'].str.contains('未知'))]
dxy_c_t = dxy_c_t[np.logical_not(dxy_c_t['city'].str.contains('外地'))]
dxy_c_t = dxy_c_t[np.logical_not(dxy_c_t['city'].str.contains('兵团'))]

for i in range(0,dxy_c_t.shape[0]): #matching and sub for partial string of city names
    str = dxy_c_t.iloc[i,1]
    b = dxy_c_t['city'].str.contains(str)
    dxy_c_t.loc[b,['city']] = str

dxy_c_t = dxy_c_t.sort_values('confirmed_c_t').drop_duplicates(keep='last')
period = dxy_c_t['date'].drop_duplicates()
period = sorted(period)
dxy_c_t = dxy_c_t[pd.notnull(dxy_c_t['province'])]

lit = []
li = []
for t in period:
    d = t.date()
    temp = dxy_c_t[dxy_c_t['date'] == t]
    temp = temp.drop(columns='date')
    temp = temp.sort_values('confirmed_c_t').drop_duplicates('city', keep='last')
    temp = temp.set_index(['province','city'])
    temp = temp.stack()
    lit.append(d)
    li.append(temp)
dxy_c_t = pd.concat(li, axis=1)
dxy_c_t.columns = lit
dxy_c_t.index = dxy_c_t.index.rename('case', level=2)

# Dealing with NaN
dxy_c_t.iloc[:,0].fillna(0, inplace=True)
for l in range(1,dxy_c_t.shape[1]):
    index = dxy_c_t.iloc[:,l].index[dxy_c_t.iloc[:,l].apply(np.isnan)]
    col1 = dxy_c_t.columns[l]
    col2 = dxy_c_t.columns[l-1]
    for i in index:
        dxy_c_t.loc[i,col1] = dxy_c_t.loc[i,col2]
dxy_c_t[dxy_c_t<0] = 0

# Derive the new cases daily
dxy_c_n = pd.DataFrame(index=dxy_c_t.index,columns=dxy_c_t.columns)
dxy_c_n.iloc[:,0] = dxy_c_t.iloc[:,0]
for l in range(1,dxy_c_t.shape[1]):
    dxy_c_n.iloc[:,l] = dxy_c_t.iloc[:,l]-dxy_c_t.iloc[:,l-1]
dxy_c_n[dxy_c_n<0] = 0
dxy_c_n = dxy_c_n.reset_index()
dxy_c_n = dxy_c_n.rename(columns={'level_2':'case'})
dxy_c_n = dxy_c_n.replace(to_replace='confirmed_t',value='confirmed_n')
dxy_c_n = dxy_c_n.replace(to_replace='cured_t',value='cured_n')
dxy_c_n = dxy_c_n.replace(to_replace='dead_t',value='dead_n')
dxy_c_n = dxy_c_n.replace(to_replace='confirmed_c_t',value='confirmed_c_n')
dxy_c_n = dxy_c_n.replace(to_replace='cured_c_t',value='cured_c_n')
dxy_c_n = dxy_c_n.replace(to_replace='dead_c_t',value='dead_c_n')
dxy_c_n = dxy_c_n.set_index(['province','city','case'])

# Making up for the 4 provincial cities
cities=['上海市','重庆市','北京市','天津市']
pc_n = dxy_p_n.loc[cities]
pc_n = pc_n.reset_index()
pc_n['city'] = pc_n['province']
pc_n['city'] = pc_n.city.replace({'市':''}, regex=True)
pc_n = pc_n.set_index(['province','city','case'])
dxy_c_n = dxy_c_n.append(pc_n)
pc_n = pc_n.reset_index()
pc_n = pc_n.replace(to_replace='confirmed_n',value='confirmed_c_n')
pc_n = pc_n.replace(to_replace='cured_n',value='cured_c_n')
pc_n = pc_n.replace(to_replace='dead_n',value='dead_c_n')
pc_n = pc_n.set_index(['province','city','case'])
dxy_c_n = dxy_c_n.append(pc_n)

pc_t = dxy_p_t.loc[cities]
pc_t = pc_t.reset_index()
pc_t['city'] = pc_t['province']
pc_t['city'] = pc_t.city.replace({'市':''}, regex=True)
pc_t = pc_t.set_index(['province','city','case'])
dxy_c_t = dxy_c_t.append(pc_t)
pc_t = pc_t.reset_index()
pc_t = pc_t.replace(to_replace='confirmed_t',value='confirmed_c_t')
pc_t = pc_t.replace(to_replace='cured_t',value='cured_c_t')
pc_t = pc_t.replace(to_replace='dead_t',value='dead_c_t')
pc_t = pc_t.set_index(['province','city','case'])
dxy_c_t = dxy_c_t.append(pc_t)

# Wrangling the list of city

tempc = dxy_c_t.reset_index()
tempc = tempc['city']
tempc = tempc.drop_duplicates()
citylist = citylist[citylist['province'] != '北京市']
city = pd.merge(tempc,citylist, on='city',how='inner')
city = city.append({'city':'上海','province':'上海市'},ignore_index=True)
city = city.append({'city':'重庆','province':'重庆市'},ignore_index=True)
city = city.append({'city':'北京','province':'北京市'},ignore_index=True)
city = city.append({'city':'天津','province':'天津市'},ignore_index=True)

# # save
with open('dxy_p_n.txt', 'wb') as f:
    pickle.dump(dxy_p_n, f)
with open('dxy_p_t.txt', 'wb') as f:
    pickle.dump(dxy_p_t, f)
with open('dxy_c_n.txt', 'wb') as f:
    pickle.dump(dxy_c_n, f)
with open('dxy_c_t.txt', 'wb') as f:
    pickle.dump(dxy_c_t, f)
with open('city.txt', 'wb') as f:
    pickle.dump(city, f)


# In[ ]:




