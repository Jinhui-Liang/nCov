#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Import
import pandas as pd
import requests
import re
import datetime as dt
from datetime import datetime
import pickle
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# loading the city code
citycode = pd.read_excel('Book1.xlsx')
citycode = citycode.T
citycode = citycode.reset_index()
citycode = citycode['index']
cityframe = pd.DataFrame(columns=['city', 'code'])
for i in range(0, 438):
    cityframe = cityframe.append({'city': citycode[2 * i], 'code': float(citycode[2 * i + 1])}, ignore_index=True)
cityframe['code'] = round(cityframe['code'])
cityframe['code'] = cityframe['code'].astype('int')
cityframe = cityframe.drop_duplicates(subset='code', keep='first')

#load
with open('mo.txt','rb') as f:
    dout = pickle.load(f)

with open('mi.txt','rb') as f:
    din = pickle.load(f)

# adding new data
lastupdate = list(dout.keys())
lastupdate = [dt.datetime.strptime(d,'%Y%m%d').date() for d in lastupdate]
sday = max(lastupdate)+dt.timedelta(days=1)
eday = dt.date.today()-dt.timedelta(days=1)
nperiod = pd.date_range(start=sday, end=eday)
nperiod = nperiod.strftime('%Y%m%d')

for d in nperiod:
    date = d
    rx_dict = {
        'city_name': re.compile(r'"city_name":"(?P<city_name>.*)","province_name"'),
        'value': re.compile(r'"value":(\d*[.]?\d*)')
    }
    moveout = {}
    movein = {}
    mic = list()
    miv = list()
    moc = list()
    mov = list()

    for i in range(0, cityframe.shape[0]):
        city = cityframe.iloc[i][0]
        code = cityframe.iloc[i][1]
        print(city,i,date)
        ourl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_out&date=%s' % (code, date)
        iurl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_in&date=%s' % (code, date)
        try:
            ocityrank = requests_retry_session().get(ourl)
            icityrank = requests_retry_session().get(iurl)
        except Exception as x:
            print('It failed :(', x.__class__.__name__)
        else:
            print('It worked')
        otemp = ocityrank.text.split('},')
        itemp = icityrank.text.split('},')
        if len(otemp) > 1:
            ocityr = pd.DataFrame(columns=['city', 'value'])
            for s in otemp:
                oa = re.findall(rx_dict['city_name'], s)
                ob = re.findall(rx_dict['value'], s)
                ocityr = ocityr.append({'city': oa[0].encode('utf-8').decode('unicode-escape'), 'value': ob[0]},
                                     ignore_index=True)
            moc.append(city)
            mov.append(ocityr)
        if len(itemp) > 1:
            icityr = pd.DataFrame(columns=['city', 'value'])
            for s in itemp:
                ia = re.findall(rx_dict['city_name'], s)
                ib = re.findall(rx_dict['value'], s)
                icityr = icityr.append({'city': ia[0].encode('utf-8').decode('unicode-escape'), 'value': ib[0]},
                                     ignore_index=True)
            mic.append(city)
            miv.append(icityr)
    for key, value in zip(moc, mov):
        moveout[key] = value
    moveout = dict(zip(moc, mov))
    dout[d] = moveout
    for key, value in zip(mic, miv):
        movein[key] = value
    movein = dict(zip(mic, miv))
    din[d] = movein              

    with open('mo.txt', 'wb') as f:
        pickle.dump(dout, f)

    with open('mi.txt', 'wb') as f:
        pickle.dump(din, f)

    

