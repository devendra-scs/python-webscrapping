import asyncio
import aiosqlite
import aiohttp
import logging
from lxml import html
import time
import random
import socket

# Configuration
DB_FILE_PATH = "data/RunningData.db"
EVENT_NAME = "Apollo Tyres New Delhi Marathon 2025"
EVENT_CITY = "Delhi"
EVENT_DATE = "February 23, 2025"
EVENT_YEAR = "2025"
START_BIB_NUMBER = 46000
END_BIB_NUMBER = 46006
BASE_URL = 'https://www.sportstimingsolutions.in/share.php?event_id=85094&bib='
CONCURRENT_REQUESTS = 10
MAX_RETRIES = 3

# Logging setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename='scraper.log', filemode='w')

async def init_db():
    logging.info("Initializing database...")
    async with aiosqlite.connect(DB_FILE_PATH) as db:
        await db.executescript('''
            CREATE TABLE IF NOT EXISTS EventDetails (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                EventName TEXT UNIQUE NOT NULL,
                EventCity TEXT,
                EventDate TEXT,
                EventYear TEXT,
                EventURL TEXT);

            CREATE TABLE IF NOT EXISTS RunnersDetails (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT UNIQUE COLLATE NOCASE,
                Gender TEXT);

            CREATE TABLE IF NOT EXISTS EventData (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                RunnersID INTEGER,
                EventID INTEGER,
                FinishTime TEXT,
                ChipTime TEXT,
                OverallRank TEXT,
                GenderRank TEXT,
                Category TEXT,
                Distance TEXT,
                BIB TEXT UNIQUE NOT NULL,
                PACE TEXT,
                GunTime TEXT,
                CategoryRank TEXT,
                ResultURL TEXT,
                FOREIGN KEY(RunnersID) REFERENCES RunnersDetails(ID),
                FOREIGN KEY(EventID) REFERENCES EventDetails(ID));
        
            CREATE TABLE IF NOT EXISTS SplitsDetails (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                EventID INTEGER,
                RunnersID INTEGER,
                Distance TEXT,
                Time TEXT,
                BIB TEXT,
                UNIQUE (EventID, RunnersID, Distance, BIB));
        
        ''')
        await db.commit()
    logging.info("Database initialized successfully.")

async def get_or_create_event(db):
    logging.debug("Fetching event details...")
    async with db.execute("""SELECT ID FROM EventDetails WHERE EventName=?""", (EVENT_NAME,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return row[0]
    logging.info(f"Event not found. Creating new event: {EVENT_NAME}")
    await db.execute("""INSERT INTO EventDetails (EventName, EventCity, EventDate, EventYear, EventURL) VALUES (?, ?, ?, ?, ?)""", 
                     (EVENT_NAME, EVENT_CITY, EVENT_DATE, EVENT_YEAR, BASE_URL))
    await db.commit()
    return await get_or_create_event(db)

async def fetch_data(session, bib):
    url = f"{BASE_URL}{bib}"
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    return bib, await response.text()
                logging.warning(f"Failed to fetch BIB {bib}, Status Code: {response.status}")
                return bib, None  # Don't retry if the response is not a connection error
        except (aiohttp.ClientError, socket.gaierror) as e:
            logging.error(f"Connection error fetching BIB {bib}, Attempt {retries + 1}: {e}")
        retries += 1
        await asyncio.sleep(2 ** retries + random.uniform(0, 1))  # Exponential backoff
    logging.error(f"Max retries reached for BIB {bib}, skipping...")
    return bib, None

async def parse_and_store(db, event_id, bib, content):
    try:
        tree = html.fromstring(content)
        name = tree.xpath('//h3[@class="txt-color img-padding"]/text()')
        if not name:
            return
        name = name[0].strip().upper()
        gender = "Women" if "Female" in name else "Men"

        await db.execute("INSERT OR IGNORE INTO RunnersDetails (Name, Gender) VALUES (?, ?)", (name, gender))
        await db.commit()
        async with db.execute("SELECT ID FROM RunnersDetails WHERE Name=?", (name,)) as cursor:
            runner_id = (await cursor.fetchone())[0]

        finished_time = tree.xpath('//td[@class="text-center neww-table he"]/text()')
        finished_time = finished_time[0].strip() if finished_time else ""
        category = tree.xpath('//th[@class="text-center"]/text()')[-1].strip() if tree.xpath('//th[@class="text-center"]') else ""
        
        distance = "21" if "Half Marathon" in category else "42" if "Marathon" in category else "10"

        await db.execute("""
            INSERT OR IGNORE INTO EventData (RunnersID, EventID, FinishTime, Category, Distance, BIB, ResultURL)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (runner_id, event_id, finished_time, category, distance, bib, f"{BASE_URL}{bib}"))
        await db.commit()
    except Exception as e:
        logging.error(f"Error parsing/storing data for BIB {bib}: {e}")

async def process_bibs():
    async with aiosqlite.connect(DB_FILE_PATH) as db:
        event_id = await get_or_create_event(db)
        async with aiohttp.ClientSession() as session:
            bibs = range(START_BIB_NUMBER, END_BIB_NUMBER)
            tasks = [fetch_data(session, bib) for bib in bibs]
            for future in asyncio.as_completed(tasks):
                bib, content = await future
                if content:
                    await parse_and_store(db, event_id, bib, content)
    logging.info("BIB processing completed.")

if __name__ == "__main__":
    logging.info("Script started.")
    asyncio.run(init_db())
    asyncio.run(process_bibs())
    logging.info("Script finished successfully.")
