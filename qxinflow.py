# Import
import pandas as pd
import requests
import re
import datetime as dt
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

inflow = {}
moc = list()
mov = list()

rx_dict = {
    'date': re.compile(r'"(\d\d\d\d\d\d\d\d)"'),
    'value': re.compile(r':(\d*[.]?\d*)')
}

for i in range(0, cityframe.shape[0]):
    city = cityframe.iloc[i][0]
    code = cityframe.iloc[i][1]
    print(city, i)
    url = 'http://huiyan.baidu.com/migration/internalflowhistory.jsonp?dt=city&id=%s&date=20200214' % code
    try:
        cityrank = requests_retry_session().get(url)
    except Exception as x:
        print('It failed :(', x.__class__.__name__)
    else:
        print('It worked')
    temp = cityrank.text.split('"list":')
    if len(temp) > 1:
        temp = temp[1].split(',')
        if len(temp) > 1 :
            cityr = pd.DataFrame(columns=['date', 'value'])
            for s in temp:
                a = re.findall(rx_dict['date'], s)
                b = re.findall(rx_dict['value'], s)
                cityr = cityr.append({'date': a[0], 'value': b[0]},
                                     ignore_index=True)
            moc.append(city)
            mov.append(cityr)

for key, value in zip(moc, mov):
    inflow[key] = value
inflow = dict(zip(moc, mov))

with open('inflow.txt', 'wb') as f:
    pickle.dump(inflow, f)

