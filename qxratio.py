# Import
import pandas as pd
import requests
import re
import datetime as dt
import pickle
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def requests_retry_session(
    retries=3,
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

# The dates, from 1-1 to 2-12
day1 = dt.date(2020, 1, 1)
dayend = dt.date(2020, 2, 10)
period = pd.date_range(start=day1, end=dayend)
period = period.strftime('%Y%m%d')

# Going through cities, move out data, during the period
dout = dict.fromkeys(period)
din = dict.fromkeys(period)
oli = list()
ili = list()
tries = 10
for d in period:
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
        ourl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_out&date=%s' % (code, date)
        iurl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_in&date=%s' % (code, date)
        icond = 0
        iloop = 1
        ocond = 0
        oloop = 1
        while ocond == 0 and oloop <= tries:
            print(city, i, date, "oloop=",oloop)
            try:
                ocityrank = requests_retry_session().get(ourl)
            except Exception as x:
                print('It failed :(', x.__class__.__name__)
            else:
                print('It worked')
            otemp = ocityrank.text.split('},')

            if len(otemp) > 1:
                ocityr = pd.DataFrame(columns=['city', 'value'])
                for s in otemp:
                    oa = re.findall(rx_dict['city_name'], s)
                    ob = re.findall(rx_dict['value'], s)
                    ocityr = ocityr.append({'city': oa[0].encode('utf-8').decode('unicode-escape'), 'value': ob[0]},
                                         ignore_index=True)
                moc.append(city)
                mov.append(ocityr)
                ocond = 1
            else:
                ocond = 0
                oloop = oloop+1
        if oloop >= tries:
            oli.append([code,date])

        while icond == 0 and iloop <= tries:
            print(city, i, date, "iloop=",iloop)
            try:
                icityrank = requests_retry_session().get(iurl)
            except Exception as x:
                print('It failed :(', x.__class__.__name__)
            else:
                print('It worked')
            itemp = icityrank.text.split('},')

            if len(itemp) > 1:
                icityr = pd.DataFrame(columns=['city', 'value'])
                for s in itemp:
                    ia = re.findall(rx_dict['city_name'], s)
                    ib = re.findall(rx_dict['value'], s)
                    icityr = icityr.append({'city': ia[0].encode('utf-8').decode('unicode-escape'), 'value': ib[0]},
                                         ignore_index=True)
                mic.append(city)
                miv.append(icityr)
                icond = 1
            else:
                icond = 0
                iloop = iloop + 1
        if iloop >= tries:
            ili.append([code, date])

    for key, value in zip(moc, mov):
        moveout[key] = value
    moveout = dict(zip(moc, mov))
    dout[d] = moveout
    for key, value in zip(mic, miv):
        movein[key] = value
    movein = dict(zip(mic, miv))
    din[d] = movein

#save
with open('mo1.txt', 'wb') as f:
    pickle.dump(dout, f)

with open('mi1.txt', 'wb') as f:
    pickle.dump(din, f)

