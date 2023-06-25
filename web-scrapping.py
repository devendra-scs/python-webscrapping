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

from bs4 import BeautifulSoup
import urllib3
import csv
import sqlite3


#Change below values 
EVENT_NAME="TCS World 10K Bengaluru 2023"
EVENT_CITY="Bangalore"
EVENT_DATE="MAY 21, 2023"
EVENT_YEAR="2023"
START_BIB_NUMBER=1
END_BIB_NUMBER=80000
BASE_URL ='https://www.sportstimingsolutions.in/share.php?event_id=73813&bib='

def get_event_ID(conn, event_name):
    EventID=0
    sql="SELECT ID from EventDetails where EventName='"+EVENT_NAME+"'";
    cursor = conn.execute(sql);
    for row in cursor:
       EventID = row[0]
    return EventID;

def insert_event_details(conn):
    EventID=get_event_ID(conn, EVENT_NAME);
    
    #print("EventID:",EventID)
    if(EventID>0):
        #print("Event Already Found")
        return EventID
    
    sql="INSERT INTO EventDetails(EventName, EventCity, EventDate, EventURL, EventCity, EventYear) VALUES('"+EVENT_NAME+"','"+EVENT_CITY+"','"+EVENT_DATE+"','"+BASE_URL+"','"+EVENT_CITY+"','"+EVENT_YEAR+"')" 
    conn.execute(sql);
    conn.commit()
    EventID=get_event_ID(conn, EVENT_NAME);
    
    return EventID;

def get_runners_ID(conn, name, gender):
    runners_id=0
    sql="SELECT ID from RunnersDetails WHERE Name='"+name+"' AND Gender='"+gender+"'" 
    cursor = conn.execute(sql);
    for row in cursor:
       runners_id = row[0]
    return runners_id;
    
def insert_runners_details(conn, name, gender):
    runners_id=get_runners_ID(conn, name, gender);
        
    if(runners_id>0):        
        return runners_id
    sql="INSERT INTO RunnersDetails (Name, Gender) VALUES ( '"+name+"', '"+gender+"')";
    #print(sql)    
    conn.execute(sql);
    conn.commit()
    runners_id=get_runners_ID(conn, name,gender);
    
    return runners_id;
    
def get_record_id_from_event_data_table(conn, event_id, BIB):
    record_id=0
    sql="SELECT ID from EventData where EventId='"+str(event_id)+"' and BIB='"+str(BIB)+"'";
    cursor = conn.execute(sql)
    for row in cursor:
       record_id = row[0]
    return record_id;
    
#BIB,Finished Time,Pace (min/km),Rank Overall,Category Rank
def insert_row_in_db(conn, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance ):
    #Check if runners data for this event already present in database
    record_id = get_record_id_from_event_data_table(conn, event_id, BIB)
    
    if record_id >0:
       #print("Record already Present");
       return 0;

    sql = "INSERT INTO EventData ( BIB, RunnersID, EventID, FinishTime, GunTime, RankOverall, RankCategory, Distance,Category ) VALUES ('"+str(BIB)+ "','"+str(runners_id)+"','"+str(event_id)+"','"+ str(NetTime)+"','"+ str(GunTime)+"','"+ str(OverallRank)+"','"+ str(CategoryRank)+"','"+str(Distance)+"', '"+str(Category)+"')"
    #print("SQL",sql)
    conn.execute(sql);
    conn.commit()
    return 1;


def Get_splits_data(conn, event_id, runners_id, BIB, Distance):
    id=0
    sql="SELECT ID from SplitsDetails where EventID='"+str(event_id)+"' AND RunnersID='"+str(runners_id)+"' AND BIB='"+str(BIB)+"' AND Distance='"+str(Distance)+"'"
    #print("SQL:",sql)
    cursor = conn.execute(sql);
    for row in cursor:
       id = row[0]
    return id;
    

def Insert_splits_data(conn, event_id, runners_id, BIB, Distance, Time):
    split_id = Get_splits_data(conn, event_id, runners_id, BIB, Distance)
    if split_id >0:
        #print("Split Data already present")
        return True
    sql="INSERT INTO SplitsDetails(EventID, RunnersID, BIB, Distance, Time) VALUES('"+str(event_id)+"','"+str(runners_id)+"','"+str(BIB)+"','"+str(Distance)+"','"+str(Time)+"')"
    #print("Spilits Insert SQL:",sql)    
    conn.execute(sql);
    conn.commit()
    
    return True
    
def parseAndWriteResponse(conn, event_id, soup, bibNumber):
        
    name=soup.find("h3",{"class":"txt-color img-padding"})
    if name is  not None:
        #row=['BIB', 'Name', 'Finished Time', 'Chip Pace (min/km)', 'Rank Overall', "Category Rank", "Category"]
        name = name.text.strip()
        finishedTime= soup.find("td", {"class":"text-center neww-table he"})        
        timers= soup.find_all("td", {"class":"text-center neww-table"})                
        pace= timers[0].text        
        rankOverall= timers[1].text        
        categoryRank = timers[2].text              
        category = soup.find("h3", class_='txt-color text-center h3_race_name padding-left').text.strip()
        Gender =""
        if category.find('Men') != -1:
           Gender = "Men"
        else: 
            if category.find('Women') != -1:
               Gender = "Women"
            else:
                Gender = ""               
        
        #Write details into database
        print("Gender:", Gender)
        runners_id = insert_runners_details(conn, name, Gender);
        insert_row_in_db(conn, event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, "".format() )
    
        table =soup.find_all("table", class_='table text-center')
        # Parse split data
        #Interval	Gun Time	Chip Time	Chip Pace (min/km)	Speed
        for row in table:
            print("Found Row")
            td = row.find_all('td')
            #print(tr)            
            #print(td)
            idx=0 
            for columns in td:
            # print("Col:",columns)
                val = columns.text.strip()
                if idx==0:
                   distance = val                   
                else:
                    if idx == 1:
                        gunTime = val
                    else:
                        if idx == 2:
                            chipTime = val
                        else:
                            if idx == 3:
                                pace = val
                            else:
                                if idx == 4:
                                    speed = val
                                    #print("Distance:", distance," GunTime:", gunTime, " ChipTime:", chipTime, " Pace:", pace, " Speed:", speed)
                                    Insert_splits_data(conn, event_id, runners_id, bibNumber, distance, chipTime)
                                    idx =0
                                    continue;                    
                idx = idx+1                
    return

bibNumber = START_BIB_NUMBER
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
count = 0
conn = sqlite3.connect('data/RunningData.db')
#print("Inserting Event ID Details")
event_id = insert_event_details(conn);

while( bibNumber < END_BIB_NUMBER):
    
    resultURL=BASE_URL+str(bibNumber) 
    print(" Fetching details of BIB:", resultURL)
    if bibNumber % 100 == 0:
        print(" Fetching details of BIB:", bibNumber)
        
    result = http.request('GET', resultURL)
    html = result.data
    if result.status == 200:
        soup = BeautifulSoup(html, "html5lib")
        parseAndWriteResponse(conn, event_id, soup, bibNumber)
    bibNumber = bibNumber+1    
    
print("Completed successfully")   
conn.close()
