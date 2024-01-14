#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2023, Devendra Singh "
__license__ = "GPL"
__version__ = "1.0.4"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# Script for webscrapping demonstration from https://in.investing.com/indices/s-p-cnx-nifty-historical-data?end_date
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons.

from bs4 import BeautifulSoup
import urllib3
import pandas as pd
import numpy as np
import itertools
import math

from datetime import datetime
from datetime import timedelta
from datetime import date
import time
import requests

#Preparing  Start Date and End Date for fetching data from investing.com
today  = date.today()
#yesterday = today - timedelta(days = 1)
#enddate = time.mktime(yesterday.timetuple())
enddate = time.mktime(today.timetuple())
enddate = int(enddate)

starting_day_of_current_year = datetime.now().date().replace(month=1, day=1)
starting_day_of_current_year
stdate=time.mktime(starting_day_of_current_year.timetuple())
stdate=int(stdate)

#BASE_URL ="https://in.investing.com/indices/s-p-cnx-nifty-historical-data?end_date={}&st_date={}".format(enddate,stdate)

BASE_URL="https://in.investing.com/indices/nifty-total-returns-historical-data?end_date=1705224489&st_date=1576348200"  
CSV_FILE_NAME= "C:\\investing.csv"

print(BASE_URL)
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
result = requests.get(BASE_URL)
if result.status_code >= 200 and result.status_code < 300:
        html = result.content
        soup = BeautifulSoup(html, "html5lib")
        date=[]
        for a in soup.findAll('td', attrs={'class':'col-rowDate'}):
            date_txt=a.find('span', attrs={'class':'text'})
            date.append(date_txt.text)

        #Loading Closing Prices
        close=[]
        for a in soup.findAll('td', attrs={'class':'col-last_close'}):
            close_txt=a.find('span', attrs={'class':'text'})
            close.append(close_txt.text)
        #Loading Open Prices
        open=[]
        for a in soup.findAll('td', attrs={'class':'col-last_open'}):
            open_txt=a.find('span', attrs={'class':'text'})
            open.append(open_txt.text)
            
        #Loading High Prices
        high=[]
        for a in soup.findAll('td', attrs={'class':'col-last_max'}):
            high_txt=a.find('span', attrs={'class':'text'})
            high.append(high_txt.text)

        #Loading Low Prices
        low=[]
        for a in soup.findAll('td', attrs={'class':'col-last_min'}):
            low_txt=a.find('span', attrs={'class':'text'})
            low.append(low_txt.text)
        ## Prepare DataFrame

        df_nifty = pd.DataFrame({'Date':date,'Open':open,'High':high,'Low':low,'Close':close}) 
        df_nifty.head()
        df_nifty['Date'] = df_nifty['Date'].str.replace(r",","")
        df_nifty['Date']=pd.to_datetime(df_nifty.Date , format = '%b %d %Y')
        df_nifty=df_nifty.drop_duplicates(subset="Date")
        #data = df_nifty.drop(['Date'], axis=1)
        data = df_nifty
        data['Close']=data['Close'].str.replace(r",","").astype('float')
        data['Open']=data['Open'].str.replace(r",","").astype('float')
        data['High']=data['High'].str.replace(r",","").astype('float')
        data['Low']=data['Low'].str.replace(r",","").astype('float')
        #data['Price']=data['Price'].astype('float')
        data=data.fillna(method="ffill")
        data.head()
        data=data.sort_values(['Date'], ascending=[True])
        data_log =    data[['Open', 'High', 'Low' , 'Close']].apply(np.log)
        #data_log =    data[['Open', 'High', 'Low' , 'Price']].apply(np.log)        
        data_log.head()
        print(data)
        data.to_csv(CSV_FILE_NAME)
        
