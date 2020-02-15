
# Import
import pandas as pd
import requests
import re
import datetime as dt
from datetime import datetime
import pickle
import copy
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

# Fixing
def Diff(li1,li2):
    return(list(set(li1) - set(li2)))
day1 = dt.date(2020, 1, 1)
dayend = list(din.keys())[-1]
dayend = datetime.strptime(dayend,'%Y%m%d')
period = pd.date_range(start=day1, end=dayend)
period = period.strftime('%Y%m%d')

tempc = list(cityframe['city'])
oli = list()
ili = list()

dinli = []
doutli = []
ddinli = []
ddoutli = []

for key in din.items():
    dinli.append(list(key[1]))
    doutli.append(list(key[1]))

for i in range(0,len(period)):
    ddinli.append(Diff(tempc,dinli[i]))
    ddoutli.append(Diff(tempc,doutli[i]))

ddinli2 = copy.deepcopy(ddinli)
ddoutli2 = copy.deepcopy(ddoutli)
tries = 50
for i in range(0,len(period)):
    date = period[i]
    rx_dict = {
        'city_name': re.compile(r'"city_name":"(?P<city_name>.*)","province_name"'),
        'value': re.compile(r'"value":(\d*[.]?\d*)'),
        'err_msg': re.compile(r'"errmsg":"(?P<err_msg>.*)",')
    }
    if len(ddoutli[i]) > 0:
        olist = ddoutli[i]
        for j in range(0, len(olist)):
            city = olist[j]
            code = cityframe[cityframe['city']==city]['code']
            ourl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_out&date=%s' % (code, date)
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
                err_msg = re.findall(rx_dict['err_msg'],otemp[0])
                if err_msg[0] == 'id is not valid':
                    ddoutli2[i].remove(city)
                    oloop = tries+1
                elif len(otemp) > 2:
                    ocityr = pd.DataFrame(columns=['city', 'value'])
                    for s in otemp:
                        oa = re.findall(rx_dict['city_name'], s)
                        ob = re.findall(rx_dict['value'], s)
                        ocityr = ocityr.append({'city': oa[0].encode('utf-8').decode('unicode-escape'), 'value': ob[0]},
                                             ignore_index=True)
                    dout[date][city] = ocityr
                    ocond = 1
                else:
                    ocond = 0
                    oloop = oloop+1
            if oloop >= tries:
                oli.append([date,city,err_msg])



    if len(ddinli[i]) > 0:
        ilist = ddinli[i]
        for j in range(0, len(ilist)):
            city = ilist[j]
            code = cityframe[cityframe['city']==city]['code']
            iurl = 'http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id=%s&type=move_in&date=%s' % (code, date)
            icond = 0
            iloop = 1
            while icond == 0 and iloop <= tries:
                print(city, i, date, "iloop=",iloop)
                try:
                    icityrank = requests_retry_session().get(iurl)
                except Exception as x:
                    print('It failed :(', x.__class__.__name__)
                else:
                    print('It worked')
                itemp = icityrank.text.split('},')
                err_msg = re.findall(rx_dict['err_msg'],otemp[0])
                if err_msg[0] == 'id is not valid':
                    ddinli2[i].remove(city)
                    iloop = tries+1
                elif len(itemp) > 2:
                    icityr = pd.DataFrame(columns=['city', 'value'])
                    for s in itemp:
                        ia = re.findall(rx_dict['city_name'], s)
                        ib = re.findall(rx_dict['value'], s)
                        icityr = icityr.append({'city': ia[0].encode('utf-8').decode('unicode-escape'), 'value': ib[0]},
                                             ignore_index=True)
                    din[date][city] = icityr
                    icond = 1
                else:
                    icond = 0
                    iloop = iloop + 1
                if iloop >= tries:
                    ili.append([date, city, err_msg])
#save
with open('mo.txt', 'wb') as f:
    pickle.dump(dout, f)

with open('mi.txt', 'wb') as f:
    pickle.dump(din, f)

#
ddinli = copy.deepcopy(ddinli2)
ddoutli = copy.deepcopy(ddoutli2)