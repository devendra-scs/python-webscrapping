#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2023, Devendra Singh "
__license__ = "GPL"
__version__ = "1.0.4"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# Script for webscrapping demonstration from https://www.sportstimingsolutions.in
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons.

from bs4 import BeautifulSoup
import urllib3
from db.dbutil import DatabaseUtil

#<BEGIN Modify>
#Change below paramenter
EVENT_NAME="Adani Ahmedabad Marathon 2021"
EVENT_CITY="Ahmedabad"
EVENT_DATE="November , 2021"
EVENT_YEAR="2021"
START_BIB_NUMBER=1
END_BIB_NUMBER=50000
BASE_URL ='https://www.sportstimingsolutions.in/share.php?event_id=64824&bib='
#<END Modify>
    
def parseAndWriteResponse(event_id, soup, bibNumber, url):
    name=soup.find("h3",{"class":"txt-color img-padding"})
    
    if name is  None:
        return False
    #row=['BIB', 'Name', 'Finished Time', 'Chip Pace (min/km)', 'Rank Overall', "Category Rank", "Category"]
    name = name.text.strip()
    
    finishedTime= soup.find("td", {"class":"text-center neww-table he"}).text
    
    timers= soup.find_all("td", {"class":"text-center neww-table"})
    
    pace= timers[0].text
    rankOverall= timers[1].text
    genderRank =""
    if len(timers) >2:    
        genderRank = timers[2].text
    categoryRank=""
    if len(timers) >3:
        categoryRank= timers[3].text
    
    thlist= soup.find_all("th", {"class":"text-center"})
    category = thlist[len(thlist)-1].text
    #print("Category:", category)
    Gender =""
    if category.find('Female') != -1:
       Gender = "Women"
    else:
       Gender = "Men"
        
    distance=""
    splits={}
    pace=""
    
    #Write details into database
    runners_id = dbutil.insert_runners_details(name, Gender);
    #print(event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank )
    dbutil.insert_row_in_db(event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank, "10", url )
    table =soup.find_all("table", class_='table')
    # Parse split data
    #Interval	Gun Time	Chip Time	Chip Pace (min/km)	Speed
    for row in table:
        td = row.find_all('td')
        idx=0 
        for columns in td:
        # print("Col:",columns)
            val = columns.text.strip()
            if idx==0:
               distance = val
            elif idx == 1:
               gunTime = val
            elif idx == 2:
               chipTime = val
            elif idx == 3:
               pace = val
            elif idx == 4:
               speed = val

               dbutil.Insert_splits_data(event_id, runners_id, bibNumber, distance, gunTime)
               idx =0
               continue
            idx = idx+1
    return True

bibNumber = START_BIB_NUMBER
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
count = 0
#print("Inserting Event ID Details")
dbutil = DatabaseUtil()
print("Inserting Event Details")
event_id = dbutil.insert_event_details(EVENT_NAME, EVENT_CITY, EVENT_DATE, EVENT_YEAR,BASE_URL)

while( bibNumber < END_BIB_NUMBER):
    
    resultURL=BASE_URL+str(bibNumber)      
    print(" Fetching details of BIB:", resultURL)
    if bibNumber % 100 == 0:
        print(" Fetching details of BIB:", bibNumber)

    result = http.request('GET', resultURL)
    html = result.data
    if result.status == 200:
        soup = BeautifulSoup(html, "html5lib")
        parseAndWriteResponse(event_id, soup, bibNumber, resultURL)

    
    bibNumber = bibNumber+1
    
print("Completed successfully")
