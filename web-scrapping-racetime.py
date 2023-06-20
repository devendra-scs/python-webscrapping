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
import json

EVENT_NAME="Shriram Properties Bengaluru Marathon 2022 October"
EVENT_CITY="Bangalore"
EVENT_DATE="16th October 2022"
#RACE_URL="https://appapi.racetime.in/result/details?raceID=20b2ab6a-4ca2-4312-9813-13584f53d8bc&event=MARATHON&bibNo="
RACE_URL="https://appapi.racetime.in/result/details?raceID=20b2ab6a-4ca2-4312-9813-13584f53d8bc&event="
EVENT_YEAR="2022"
START_BIB_NUMBER=1
END_BIB_NUMBER=500000

def get_event_ID(conn, event_name):
    EventID=0
    sql="SELECT ID from EventDetails where EventName='"+EVENT_NAME+"'";
    cursor = conn.execute(sql);
    for row in cursor:
       EventID = row[0]
    return EventID;

def insert_event_details(conn):
    EventID=get_event_ID(conn, EVENT_NAME);
    
    print("EventID:",EventID)
    if(EventID>0):
        print("Event Already Found")
        return EventID
    
    sql="INSERT INTO EventDetails(EventName, EventCity, EventDate, EventURL, EventCity, EventYear) VALUES('"+EVENT_NAME+"','"+EVENT_CITY+"','"+EVENT_DATE+"','"+RESULT_URL+"','"+EVENT_CITY+"','"+EVENT_YEAR+"')" 
    conn.execute(sql);
    conn.commit()
    EventID=get_event_ID(conn, EVENT_NAME);
    
    return EventID;

def get_runners_ID(conn, name, gender):
    runners_id=0
    sql="SELECT ID from RunnersDetails where Name='"+name+"' AND Gender='"+gender+"'" 
    cursor = conn.execute(sql);
    for row in cursor:
       runners_id = row[0]
    return runners_id;
    
def insert_runners_details(conn, name, gender):
    runners_id=get_runners_ID(conn, name, gender);
        
    if(runners_id>0):        
        return runners_id
    sql="INSERT INTO RunnersDetails (Name, Gender) VALUES ( '"+name+"', '"+gender+"')";
    print(sql)    
    conn.execute(sql);
    conn.commit()
    runners_id=get_runners_ID(conn, name,gender);
    
    return runners_id;
    
def get_record_id_from_event_data_table(conn, event_id, BIB):
    record_id=0
    sql="SELECT ID from EventData where EventId='"+str(event_id)+"' and BIB='"+BIB+"'";
    cursor = conn.execute(sql)
    for row in cursor:
       record_id = row[0]
    return record_id;
    
#BIB,Finished Time,Pace (min/km),Rank Overall,Category Rank
def insert_row_in_db(conn, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance ):
    #Check if runners data for this event already present in database
    record_id = get_record_id_from_event_data_table(conn, event_id, BIB)
    
    if record_id >0:
       print("Record already Present");
       return 0;

    sql = "INSERT INTO EventData ( BIB, RunnersID, EventID, FinishTime, GunTime, RankOverall, RankCategory, Distance,Category ) VALUES ('"+str(BIB)+ "','"+str(runners_id)+"','"+str(event_id)+"','"+ NetTime+"','"+ GunTime+"','"+ str(OverallRank)+"','"+ str(CategoryRank)+"','"+str(Distance)+"', '"+Category+"')"
    print("SQL",sql)
    conn.execute(sql);
    conn.commit()
    return 1;


def Get_splits_data(conn, event_id, runners_id, BIB, Distance):
    id=0
    sql="SELECT ID from SplitsDetails where EventID='"+str(event_id)+"' AND RunnersID='"+str(runners_id)+"' AND BIB='"+str(BIB)+"' AND Distance='"+str(Distance)+"'"
    print("SQL:",sql)
    cursor = conn.execute(sql);
    for row in cursor:
       id = row[0]
    return id;
    

def Insert_splits_data(conn, event_id, runners_id, BIB, Distance, Time):
    split_id = Get_splits_data(conn, event_id, runners_id, BIB, Distance)
    if split_id >0:
        print("Split Data already present")
        return True
    sql="INSERT INTO SplitsDetails(EventID, RunnersID, BIB, Distance, Time) VALUES('"+str(event_id)+"','"+str(runners_id)+"','"+str(BIB)+"','"+str(Distance)+"','"+str(Time)+"')"
    print("Spilits Insert SQL:",sql)    
    conn.execute(sql);
    conn.commit()


    return True

def parseAndWriteResponse(conn, event_id, json_data):
    BIB= json_data['bibNo']    
    Name = json_data['name']
    Category = json_data['category']
    Distance = json_data['distance']
    
    GunTime = json_data['gunTime']
    
    NetTime = json_data['netTime']

    OverallRank = json_data['overallRank']
    CategoryRank = json_data['categoryRank']
    
    Gender = json_data['gender']
    
    GenderRank = json_data['genderRank']

#Write details into database
    runners_id = insert_runners_details(conn, Name, Gender);
    insert_row_in_db(conn, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance )
    print("Inserting Splits Data")
    for item in json_data['laps']:
        Insert_splits_data(conn, event_id, runners_id, BIB, item['distance'], item['time'])
    return

conn = sqlite3.connect('data/RunningData.db')
print("Inserting Event ID Details")
event_id = insert_event_details(conn);
bibNumber = START_BIB_NUMBER

while( bibNumber < END_BIB_NUMBER):
    
    resultURL=""
    
    if bibNumber <1000:    
        resultURL= RACE_URL+"MARATHON+ELITE&bibNo="+str(bibNumber)
    else:
        if bibNumber <20000:
            #https://appapi.racetime.in/result/details?raceID=20b2ab6a-4ca2-4312-9813-13584f53d8bc&event=TIMED+10K&bibNo=14047 
            resultURL= RACE_URL+"TIMED+10K&bibNo="+str(bibNumber)
        else:
            if bibNumber <40000:
                #https://appapi.racetime.in/result/details?raceID=20b2ab6a-4ca2-4312-9813-13584f53d8bc&event=HALF+MARATHON&bibNo=26668
                resultURL= RACE_URL+"HALF+MARATHON&bibNo="+str(bibNumber)
            else :
                #https://appapi.racetime.in/result/details?raceID=20b2ab6a-4ca2-4312-9813-13584f53d8bc&event=MARATHON&bibNo=44103
                resultURL= RACE_URL+"MARATHON&bibNo="+str(bibNumber)
        
    print(resultURL)
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    result = http.request('GET', resultURL)
    
    if result.status == 200:
       json_data = result.data             
       result = json.loads(json_data)       
       parseAndWriteResponse(conn, event_id, result['data'])
    
    bibNumber =bibNumber+1

print("Completed successfully")