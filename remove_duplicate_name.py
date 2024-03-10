import sqlite3
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="remove_duplicate.log",filemode='w')

def update_all_duplicate_ids(conn, duplicate_list):
    try:
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        old =0
        new =0
        for new_idx, old_idx in duplicate_list.items():
            query = "UPDATE EventData SET RunnersID="+str(old_idx)+" WHERE RunnersID="+str(new_idx)
            old =old_idx
            new = new_idx
            cursor.execute(query)
            conn.commit()
        cursor.close()
    except Exception as e:
        logging.error("An error occurred:", str(e)," Old:" + old," New:"+ new)
        # Rollback the changes if an exception occurred
        conn.rollback()
        cursor.close()
        assert False

def delete_from_splits(conn, idx):
    try:
        cursor = conn.cursor()
        query = "DELETE FROM SplitsDetails WHERE RunnersID="+str(idx)
        cursor.execute(query)
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error("An error occurred:"+idx)
        # Rollback the changes if an exception occurred
        conn.rollback()
        cursor.close()
        assert False


def update_all_duplicate_splits(conn, duplicate_list):
    try:
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        old =0
        new =0
        for new_idx, old_idx in duplicate_list.items():
            query = "UPDATE SplitsDetails SET RunnersID="+str(old_idx)+" WHERE RunnersID="+str(new_idx)
            old =old_idx
            new = new_idx
            try:
                cursor.execute(query)
            except Exception as e:
                delete_from_splits(conn, new_idx)
            conn.commit()
        cursor.close()
    except Exception as e:
        logging.error("An error occurred:", str(e)," Old:" + old," New:"+ new)

        # Rollback the changes if an exception occurred
        conn.rollback()
        cursor.close()
        assert False
        

def remove_all_duplicate_ids(conn, duplicate_list):
    try:
        cursor = conn.cursor()
        for new_idx, old_idx in duplicate_list.items():
            query3 = "DELETE FROM RunnersDetails WHERE ID="+str(new_idx)
            cursor.execute(query3)
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error("An error occurred:", str(e))
        # Rollback the changes if an exception occurred
        conn.rollback()
        cursor.close()
        assert False

def convert_names_to_upper_case(conn, name_list):
    try:
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        for name, idx in name_list.items():
            query = "UPDATE RunnersDetails SET name='"+name+"' WHERE ID="+str(idx)
            cursor.execute(query)
            conn.commit()
    except Exception as e:
        logging.error("An error occurred:", str(e))
        # Rollback the changes if an exception occurred
        conn.rollback()
        assert False

def update_finish_time(conn, invalid_list):
    try:
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        for idx, val in invalid_list.items():
            query = "UPDATE EventData SET FinishTime='"+val+"' WHERE ID="+str(idx)
            cursor.execute(query)
        conn.commit()
    except Exception as e:
        logging.error("An error occurred:", str(e))

        # Rollback the changes if an exception occurred
        conn.rollback()
        assert False
        

def update_invalid_finishtime(conn):
    query= "SELECT  ID, FinishTime FROM EventData where FinishTime like '<td class%'"
    print(query)
    cursor = conn.execute(query);
    invalid_list=dict()
    count = 0
    
    for row in cursor:
        key=str(row[0])
        val = str(row[1])
        soup = BeautifulSoup(val, 'html.parser')
        td_text = soup.td.get_text(strip=True)    
        invalid_list[key]=td_text
    cursor.close();
    update_finish_time(conn, invalid_list)
    logging.info("Invalid Count:"+str(len(invalid_list)))
    return

    

def cout_unique_names(conn):
    query= "SELECT  ID, name FROM RunnersDetails"
    cursor = conn.execute(query);
    name_dict = dict()
    duplicate_list=dict()
    count = 0
    for row in cursor:
        idx = row[0]
        name=row[1].upper()
        old_idx = name_dict.get(name, -1)
        if old_idx==-1:
           name_dict[name]=idx
        else:
            duplicate_list[idx]=old_idx

    cursor.close()
    logging.info("Updating duplicate ids")
    #update_all_duplicate_ids(conn, duplicate_list)
    logging.info("Removing duplicate splits")
    update_all_duplicate_splits(conn, duplicate_list)
    logging.info("Removing duplicate ids")    
    remove_all_duplicate_ids(conn, duplicate_list)
    convert_names_to_upper_case(conn, name_dict)
    logging.info("Total Number of Unique names:"+ str(len(name_dict)))
    logging.info("Total Number of Duplicate names:"+ str(len(duplicate_list)))
    return

conn = sqlite3.connect('data/RunningData.db')
update_invalid_finishtime(conn)
conn.close()

logging.info("succesfully converted")
