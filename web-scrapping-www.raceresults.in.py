#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2019, Devendra SIngh "
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# This is test python program for we scrapping, I have used this to fetch running data from http://www.raceresults.in/
# This will create .csv and .excel files after fetching data
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons. 

from bs4 import BeautifulSoup
import urllib3
import csv

# Create your dictionary class  
class my_dictionary(dict):  
  
    # __init__ function  
    def __init__(self):  
        self = dict()  
          
    # Function to add key:value  
    def add(self, key, value):  
        self[key] = value  

def writeCSVRow(row):
    csvWriter = csv.writer(csvFile)
    csvWriter.writerow(row)
    return

def parseAndWriteResponse(soup,row): 
    dict_obj = my_dictionary()  
    row.clear()
    details=soup.find_all("Engages")[0].find_all("E")
    results=soup.find_all("Resultats")[0].find_all("R")
    for item in details:
        row.clear()
        #print(item)
        row.insert(0,item['d']) #bib
        row.insert(1,item['n']) #name
        try: 
            row.insert(2,item['x']) #gender
            row.insert(3,item['p'])#race
            row.insert(4,item['rg']) #Mobile
        except:
            row.insert(2,'') #gender
            row.insert(3,'') #gender
            row.insert(5,'') #gender
        dict_obj.add(item['d'],list(row))              
    for item in results:
        #print(dict_obj)
        row = dict_obj.get(item['d'])
        print(item)
        #print(row)
        if row is  not None:
            row.insert(5,item['t']) #Time
            try:
                row.insert(6,item['m']) #Average Speed
                row.insert(7,item['re']) #Chip time
                row.insert(8,item['b'])#Time of Day
            except:
                row.insert(6,'') 
                row.insert(7,'')                 
                row.insert(8,'')
            dict_obj[item['d']]=list(row)       
    for row in dict_obj:
        writeCSVRow(dict_obj[row])       
    return

csvFile= open('freedom10k.csv', 'a') 

row=['BIB', 'Name', 'Gender', 'Race', 'Mobile', 'Time', 'Average Speed','Chip Time' ,'Time of Day']
writeCSVRow(row)
file = "freedom10k.xml"
handler = open(file).read()
soup = BeautifulSoup(handler,"xml")
parseAndWriteResponse(soup,row)


csvFile.close()

