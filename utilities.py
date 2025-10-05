from datetime import datetime
import sqlite3

_log_info = {
    "script_name" : None,
    "script_success" : 0,
    "record_count" : 0,
    "script_start_time" : None,
    "script_end_time" : None,
    "script_error_message" : "No Error Message"
}

def init_logging_table():
    # Function to create table if not existing. Also clears any successful logs that are more than a month old
    _db = sqlite3.connect('/usr/files/spotify_data/sp_data.db')
    _c = _db.cursor()

    _c.execute("""
    CREATE TABLE IF NOT EXISTS LOGGING_TABLE(
        script_name TEXT,
        script_success INTEGER, 
        record_count INTEGER, 
        script_start_time TEXT,
        script_end_time TEXT,
        script_error_message TEXT)
    """)

    _c.execute("""
    DELETE FROM LOGGING_TABLE WHERE  script_end_time < datetime('now','-1 months') AND script_success = 1;
    """)
    _db.commit()
    _db.close()

def get_start_info(script_name):
    # Store script name and the datetime this function is called (will be called on start of scripts)
    _log_info['script_name'] = script_name
    _log_info['script_start_time'] = datetime.now()

def get_finish_info(record_count):
    # Store the record count of insertions, and alter the success flag to show the script ran. Also get time of call to compare to start time
    _log_info['script_end_time'] = datetime.now()
    _log_info['script_success'] = 1
    _log_info['record_count'] = record_count

def get_error_message(error_message):
    _log_info['script_error_message'] = error_message

def write_log():
    # Write to logging table
    _db = sqlite3.connect('/usr/files/spotify_data/sp_data.db')
    _c = _db.cursor()
    _c.execute("""
        INSERT INTO LOGGING_TABLE (
            script_name,
            script_success,
            record_count,
            script_start_time,
            script_end_time,
            script_error_message)
        VALUES(
            :script_name,
            :script_success,
            :record_count,
            :script_start_time,
            :script_end_time,
            :script_error_message)    
    """,_log_info)
    _db.commit()
    _db.close()

def get_items(sp_connection, items_list):
    # Function to take the items list of an API call then iterate through next flags
    # Ensures that despite limits, the full entirety of data is retrieved
    items = items_list['items']

    while items_list['next']:
        items_list = sp_connection.next(items_list)
        items.extend(items_list['items'])
    
    return items

def chunk_list(list, length):
    # Function to seperate a list by the length parameter
    for i in range(0, len(list), length):
        yield list[i:i+length]

init_logging_table()