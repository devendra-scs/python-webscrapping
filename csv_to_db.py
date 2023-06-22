import csv
import sqlite3
#This script will push data from csv file to sqllite database
EVENT_NAME="TCS World 10K Bengaluru 2022"
EVENT_CITY="Bangalore"
EVENT_DATE="15-MAY-2022"
RESULT_URL="https://www.sportstimingsolutions.in/share.php?event_id=67028&bib="
EVENT_YEAR="2022"
FILE_PATH = 'tcs_2022.csv'

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

def get_runners_ID(conn, name):
    runners_id=0
    sql="SELECT ID from RunnersDetails where Name='"+name+"'";    
    cursor = conn.execute(sql);
    for row in cursor:
       runners_id = row[0]
    return runners_id;
    
def insert_runners_details(conn, name):
    runners_id=get_runners_ID(conn, name);
        
    if(runners_id>0):        
        return runners_id
    sql="INSERT INTO RunnersDetails ( Name) VALUES ( '"+name+"')";
    
    conn.execute(sql);
    conn.commit()
    runners_id=get_runners_ID(conn, name);
    
    return runners_id;
    
def get_record_id_from_event_data_table(conn, event_id, BIB):
    record_id=0
    sql="SELECT ID from EventData where EventId='"+str(event_id)+"' and BIB='"+BIB+"'";
    cursor = conn.execute(sql);    
    for row in cursor:
       record_id = row[0]       
    return record_id;
    
#BIB,Finished Time,Pace (min/km),Rank Overall,Category Rank
def insert_row_in_db(conn, row, event_id):
    runners_id = insert_runners_details(conn, row[1].strip());
    record_id = get_record_id_from_event_data_table(conn, event_id, str(row[0]));
    
    if record_id >0:
        return 0;
        
    sql = "INSERT INTO EventData ( BIB, RunnersID, EventID, FinishTime,Pace, RankOverall, RankCategory, Distance ) VALUES ('"+str(row[0])+ "','"+str(runners_id)+"','" +str(event_id)+"','"+ row[2]+"','"+ row[3]+"','"+ row[4]+"','"+ row[5]+"',"+"'10k')";
    conn.execute(sql);
    conn.commit()
    return 1;
    
def parse_csv_file_and_inset_into_db(file_path, conn, event_id):
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)  # Read the header row
        count =0
        for row in reader:
            insert_row_in_db(conn, row, event_id);
            count= count+1            
            if count >1000:
                print("Inserted upto BIB:", row[0]);
                count = 0

conn = sqlite3.connect('RunningData.db')
event_id = insert_event_details(conn);
parse_csv_file_and_inset_into_db(FILE_PATH, conn,event_id)
conn.close()
print("Successfully inserted");