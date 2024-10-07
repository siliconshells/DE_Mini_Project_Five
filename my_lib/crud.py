import sqlite3

def get_table_columns(database_name: str, table_name: str):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    columns = c.execute(f"SELECT name FROM pragma_table_info('{table_name}')").fetchall()
    return [(str(column).strip(')').strip('(').strip(',').strip("'")) for column in columns]


def get_primary_key(cursor, table_name):
    return cursor.execute(f"SELECT name FROM pragma_table_info('{table_name}') where pk").fetchone()[0]

def read_data(database_name: str, table_name: str, data_id: int):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    result = c.execute(f"select * from {table_name} where {get_primary_key(c, table_name)} = {data_id}").fetchall()
    c.close()
    conn.close()
    if result:
        return result
    else:
        return None
    

def read_all_data(database_name: str, table_name: str):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    result = c.execute(f"select * from {table_name}").fetchall()
    c.close()
    conn.close()
    if result:
        return result
    else:
        return None


def save_data(database_name: str, table_name: str, row: list):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    c.execute(f"INSERT INTO {table_name} ({', '.join(get_table_columns(database_name, table_name))}) VALUES ('{"', '".join(row)}')")
    conn.commit()
    c.close()
    conn.close()
    return "Save Successful"


def delete_data(database_name: str, table_name: str, data_id: int):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    c.execute(f"delete from {table_name} where {get_primary_key(c, table_name)} = {data_id}")
    conn.commit()
    c.close()
    conn.close()
    return "Delete Successful"


def update_data(database_name: str, table_name: str, things_to_update: dict, data_id: int):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    print(f"UPDATE {table_name} SET {', '.join([(k + "='" + v+"'") for (k,v) in things_to_update.items()])} WHERE {get_primary_key(c, table_name)} = {data_id}")
    c.execute(f"UPDATE {table_name} SET {', '.join([(k + "='" + v+"'") for (k,v) in things_to_update.items()])} WHERE {get_primary_key(c, table_name)} = {data_id}")
    conn.commit()
    c.close()
    conn.close()
    return "Update Successful"
