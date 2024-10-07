"""
Transform the extracted data 
"""

import sqlite3
import csv


# load the csv file and insert into a new sqlite3 database
def transform_n_load(
    local_dataset: str,
    database_name: str,
    new_data_tables : dict,
    new_lookup_tables: dict,
    column_attributes: dict,
    column_map: dict
):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # load the data from the csv
    reader = csv.reader(open("data/" + local_dataset, newline=""), delimiter=",")

    conn = sqlite3.connect("data/" + database_name)

    c = conn.cursor()
    for k, v in new_data_tables.items():
        c.execute(f"DROP TABLE IF EXISTS {k}")
        c.execute(f"CREATE TABLE {k} ({", ".join((f"{col} {column_attributes[col]}") for col in v)})")
    for k, v in new_lookup_tables.items():
        c.execute(f"DROP TABLE IF EXISTS {k}")
        c.execute(f"CREATE TABLE {k} ({(", ").join((f"{col} {column_attributes[col]}") for col in v)})")
    
    # skip the first row
    next(reader)
    for row in reader:
        for k, v in new_lookup_tables.items():
            exec_str =  f"select count({v[0]}) from {k} where {v[0]} = {int(row[column_map[v[0]]])}"
            result = c.execute(exec_str).fetchone()[0]
            if result == 0:     
                c.execute(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")
                conn.commit()

        for k, v in new_data_tables.items():          
            c.execute(f"INSERT INTO {k} ({', '.join(v)}) VALUES ('{"', '".join([(row[column_map[col]]) for col in v])}')")
        conn.commit()

    conn.close()
    return "Transform  and load Successful"
