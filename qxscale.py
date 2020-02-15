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

moveout = {}
movein = {}
mic = list()
miv = list()
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
    ourl = 'http://huiyan.baidu.com/migration/historycurve.jsonp?dt=city&id=%s&type=move_out&startDate=20190112&endDate=20200207' % code
    iurl = 'http://huiyan.baidu.com/migration/historycurve.jsonp?dt=city&id=%s&type=move_in&startDate=20190112&endDate=20200207' % code
    try:
        ocityrank = requests_retry_session().get(ourl)
        icityrank = requests_retry_session().get(iurl)
    except Exception as x:
        print('It failed :(', x.__class__.__name__)
    else:
        print('It worked')
    otemp = ocityrank.text.split('"list":')
    if len(otemp) > 1:
        otemp = otemp[1].split(',')
        if len(otemp) > 1 :
            ocityr = pd.DataFrame(columns=['date', 'value'])
            for s in otemp:
                oa = re.findall(rx_dict['date'], s)
                ob = re.findall(rx_dict['value'], s)
                ocityr = ocityr.append({'date': oa[0], 'value': ob[0]},
                                     ignore_index=True)
            moc.append(city)
            mov.append(ocityr)
    itemp = icityrank.text.split('"list":')
    if len(itemp) > 1:
        itemp = itemp[1].split(',')
        if len(itemp) > 1:
            icityr = pd.DataFrame(columns=['date', 'value'])
            for s in itemp:
                ia = re.findall(rx_dict['date'], s)
                ib = re.findall(rx_dict['value'], s)
                icityr = icityr.append({'date': ia[0], 'value': ib[0]},
                                           ignore_index=True)
            mic.append(city)
            miv.append(icityr)

for key, value in zip(mic, miv):
    movein[key] = value
movein = dict(zip(mic, miv))

for key, value in zip(moc, mov):
    moveout[key] = value
moveout = dict(zip(moc, mov))

with open('inscale.txt', 'wb') as f:
    pickle.dump(movein, f)

with open('outscale.txt', 'wb') as f:
    pickle.dump(moveout, f)
