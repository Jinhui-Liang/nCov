# import
import pandas as pd
import json
import datetime as dt
import pickle
# Scraping the ccdc, where 01-15 is missing
day1 = dt.date(2020, 1, 10)
dayend = dt.date(2020, 2, 10)
period = pd.date_range(start=day1, end=dayend)
period = period.date
ccdc = []
new_index = ['suspected_n','suspected_t','confirmed_n','confirmed_t','dead_n','dead_t']
for j in period:
    j = j.strftime("%m%d")
    with open('ccdc/yq_2020%s.json' % j, encoding='utf-8') as f:
        cdc = json.load(f)
    li = []
    lin = []
    for i in range(0, 34):
        temp = cdc['features'][i]['properties']
        tempname = cdc['features'][i]['properties']['name']
        dftemp = pd.Series(temp, index=['新增疑似','累计疑似','新增确诊','累计确诊','新增死亡','累计死亡'])
        dftemp = dftemp.reset_index(drop=True)
        dftemp.index = new_index
        li.append(dftemp)
        lin.append(tempname)
    dfd = pd.DataFrame(li,index=lin)
    dfds = dfd.stack()
    ccdc.append(dfds)
ccdc = pd.concat(ccdc, axis=1)
ccdc.index = pd.MultiIndex.from_tuples(ccdc.index.values, names=['provinces', 'cases'])
ccdc.columns = period

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
ccdc = pd.concat([ccdc_n,ccdc_t],axis=0)
with open('ccdc.txt', 'wb') as f:
    pickle.dump(ccdc, f)
with open('ccdc_n.txt', 'wb') as f:
    pickle.dump(ccdc_n, f)
with open('ccdc_t.txt', 'wb') as f:
    pickle.dump(ccdc_t, f)
    
ccdc.to_csv(r'C:\Users\jinhu\Google Drive\machine learning\nCov\ccdcdata.csv', header=True)


