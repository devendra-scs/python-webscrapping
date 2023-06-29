#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2019, Devendra SIngh "
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# This is test python program for we scrapping, I have used this to fetch running data from TCS Open10k event 
# This will create .csv and .excel files after fetching data
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons. 
import requests
from bs4 import BeautifulSoup
import urllib3
import csv
import sqlite3
import base64
from db.dbutil import DatabaseUtil


#Change below values 
EVENT_NAME="SPIRIT OF WIPRO RUN BENGALURU 2019 "
EVENT_CITY="Bangalore"
EVENT_DATE="MAY 21, 2019"
EVENT_YEAR="2019"
START_BIB_NUMBER=10000
END_BIB_NUMBER=20000
BASE_URL ="https://www.timingindia.com/my-result-details/"
def parseAndWriteResponse(dbutil, event_id, soup, bibNumber):
    
    distance = soup.find_all("h3")
    distance_found = False
    for dist in distance:
        distance = dist.text.strip()
        distance_found = True
    
    if distance_found is False:
       return False
    
    if distance.find("10K") !=-1:
        distance ="10KM"
    elif distance.find("HALF") !=-1:
         distance ="21.1"
         
    table = soup.find("table",class_='table table-curved tborder')
    name =""
    Gender =""
    finishedTime=""
    rankOverall=""
    category=""
    categoryRank="" 
    distance=""
    splits={}
    pace=""
    
    for row in table.find_all("tr"):
        col = row.find_all("td")
        key = col[0].text.strip()
        val = col[1].text.strip()
        
        if key.find("Name") !=-1:
             name = val
        elif key.find("Gender")!=-1:
             Gender = val
        elif key.find("Category Rank")!=-1:
            categoryRank = val
        elif key.find("Category")!=-1:
            category= val
        elif key.find("Rank")!=-1:
            rankOverall= val
        elif key.find("Split")!=-1:
            splits[key]= val.split()[0]
             
            
        elif key.find("Net Time")!=-1:
             finishedTime = val
            
        # for cell in row.find_all("td"):
            # print(cell.text)
    #print("Name", name, " Gender:", Gender," Category:", category, " RankOverAll:", rankOverall," CategoryRank:", categoryRank, " FinishedTime:", finishedTime," Pace:", pace)
    runners_id = dbutil.insert_runners_details(name, Gender);
    dbutil.insert_row_in_db(event_id, runners_id, bibNumber, finishedTime, pace, rankOverall, category, categoryRank, distance )
    while key in splits:
        dbutil.Insert_splits_data(event_id, runners_id, bibNumber, key, splits[key])
   
    return True

def getURL(bibNumber):
    encodedpart = str(bibNumber)+":timing_r1909_sowben_10k:SPIRIT OF WIPRO RUN BENGALURU 2019"    
    url = BASE_URL+base64.b64encode(encodedpart.encode("ascii")).decode("ascii")
    return url
    
dbutil = DatabaseUtil()
bibNumber = START_BIB_NUMBER
#http = urllib3.PoolManager(cert_reqs='CERT_NONE')
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
count = 0
event_id = dbutil.insert_event_details(EVENT_NAME, EVENT_CITY, EVENT_DATE, EVENT_YEAR)

while( bibNumber < END_BIB_NUMBER):
    
    resultURL=getURL(bibNumber)
    #print(" Fetching details of BIB:", bibNumber)
    if bibNumber % 100 == 0:
        print(" Fetching details of BIB:", bibNumber)
        
    #result = http.request('GET', resultURL)
    result = requests.get(resultURL)    
    if result.status_code >= 200 and result.status_code < 300:
       html = result.content
       soup = BeautifulSoup(html, "html.parser")
       parseAndWriteResponse(dbutil, event_id, soup, bibNumber)
       
    bibNumber = bibNumber+1    
    
print("Completed successfully")   
