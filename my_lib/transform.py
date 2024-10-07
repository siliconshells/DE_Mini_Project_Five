"""
Transform the extracted data 
"""

import sqlite3
import csv
import os


# load the csv file and insert into a new sqlite3 database
def transform_n_load(
    local_dataset="air_quality.csv",
    database_name="air_quality.db",
    new_data_tables={ #add field types
        "air_quality": ["air_quality_id", "time_period", "start_date", "data_value"],
    },
    new_lookup_tables={ #add field types
        "indicator": ["indicator_id", "indicator_name", "measure", "measure_info"],
        "geo_data": ["geo_id", "geo_place_name", "geo_type_name"],
    },
    column_attributes = {
        "air_quality_id":"INTEGER PRIMARY KEY",
        "indicator_id":"INTEGER PRIMARY KEY",
        "indicator_name":"TEXT",
        "measure":"TEXT",
        "measure_info":"TEXT",
        "geo_type_name":"TEXT",
        "geo_id":"INTEGER PRIMARY KEY",
        "geo_place_name":"TEXT",
        "time_period":"TEXT",
        "start_date":"TEXT",
        "data_value":"REAL"
    },
    column_map = {
        "air_quality_id":0,
        "indicator_id":1,
        "indicator_name":2,
        "measure":3,
        "measure_info":4,
        "geo_type_name":5,
        "geo_id":6,
        "geo_place_name":7,
        "time_period":8,
        "start_date":9,
        "data_value":10
    }
):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # load the data from the csv
    reader = csv.reader(open("data/" + local_dataset, newline=""), delimiter=",")

    # skips the header of csv
    # next(payload)
    conn = sqlite3.connect("data/" + database_name)

    c = conn.cursor()
    for k, v in new_data_tables.items():
        c.execute(f"DROP TABLE IF EXISTS {k}")
        c.execute(f"CREATE TABLE {k} ({", ".join((f"{col} {column_attributes[col]}") for col in v)})")
    for k, v in new_lookup_tables.items():
        c.execute(f"DROP TABLE IF EXISTS {k}")
        c.execute(f"CREATE TABLE {k} ({", ".join((f"{col} {column_attributes[col]}") for col in v)})")
    
    # skip the first row
    next(reader)
    for row in reader:
        for k, v in new_data_tables.items():          
            c.execute(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")
        conn.commit()

        for k, v in new_lookup_tables.items():
            # print(c.execute(f"select count(*) from {k}").fetchone())
            # print(c.execute(f"select count({v[0]}) from {k} where {v[0]} = ({row[column_map[v[0]]]},)"))
            id = int(row[column_map[v[0]]])
            exec_str =  f"select count({v[0]}) from {k} where {v[0]} = ?"
            print(len(c.execute(exec_str, (id,)).fetchall()))
            result = c.execute(exec_str, (id,)).fetchall()
            if result:     
            #  print(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")    
                c.execute(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")
                conn.commit()
                # print(c.execute(f"select {v[0]} from {k} where {v[0]} = {row[column_map[v[0]]]}").fetchall())

        for k, v in new_data_tables.items():          
            c.execute(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")
        conn.commit()



    conn.close()
    return "GroceryDB.db"
