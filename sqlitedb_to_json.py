import sqlite3
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="sqlitedbtojson.log",
    filemode="w"
)

# Constants
DB_FILE = "data/RunningData.db"
OUTPUT_JSON_FILE = "Consolidate-Report.json"

QUERY = """
SELECT BIB, RD.name, EVD.EventYear, Distance, FinishTime, Pace, OverallRank, 
       GenderRank, Category, CategoryRank, EVD.EventCity, EVD.EventName, ED.RESULTURL
FROM EventData ED
JOIN RunnersDetails RD ON ED.RunnersID = RD.ID
JOIN EventDetails EVD ON EVD.ID = ED.EventID
"""

def fetch_data(db_path):
    """Fetch data from SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row  # Allows fetching rows as dictionaries
            cursor = conn.execute(QUERY)
            data = [dict(row) for row in cursor.fetchall()]
            logging.info("Successfully fetched data from database.")
            return data
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return []

def save_json(data, output_file):
    """Save data to a JSON file."""
    try:
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        logging.info("Successfully saved data to JSON file.")
    except IOError as e:
        logging.error(f"File error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while saving JSON: {e}")

def main():
    """Main function to execute the script."""
    data = fetch_data(DB_FILE)
    if data:
        save_json(data, OUTPUT_JSON_FILE)
    else:
        logging.warning("No data found in the database.")

if __name__ == "__main__":
    main()
