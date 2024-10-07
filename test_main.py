from my_lib.extract import extract
from my_lib.transform import transform_n_load
from my_lib.util import log_tests
import os
from my_lib.crud import (
    read_data,
    read_all_data,
    save_data,
    delete_data,
    update_data,
    get_table_columns,
)


# Test extract
def test_extract():
    log_tests("Extraction Test", header=True, new_log_file=True)
    log_tests("Removing existing CSV file exists")
    if os.path.exists("data/air_quality.csv"):
        os.remove("data/air_quality.csv")

    log_tests("Confirming that CSV file doesn't exists...")
    assert not os.path.exists("population_bar.png")
    log_tests("Test Successful")

    log_tests("Extracting data and saving...")
    extract("https://data.cityofnewyork.us/resource/c3uy-2p5r.csv", "air_quality.csv")

    log_tests("Testing if CSV file exists...")
    assert os.path.exists("data/air_quality.csv")
    log_tests("Test Successful", last_in_group=True)


# Test transform and load
def test_transform_and_load():
    transform_n_load(
        local_dataset="air_quality.csv",
        database_name="air_quality.db",
        new_data_tables={
            "air_quality": [
                "air_quality_id",
                "fn_indicator_id",
                "fn_geo_id",
                "time_period",
                "start_date",
                "data_value",
            ],
        },
        new_lookup_tables={
            "indicator": ["indicator_id", "indicator_name", "measure", "measure_info"],
            "geo_data": ["geo_id", "geo_place_name", "geo_type_name"],
        },
        column_attributes={
            "air_quality_id": "INTEGER PRIMARY KEY",
            "indicator_id": "INTEGER PRIMARY KEY",
            "indicator_name": "TEXT",
            "measure": "TEXT",
            "measure_info": "TEXT",
            "geo_type_name": "TEXT",
            "geo_id": "INTEGER PRIMARY KEY",
            "geo_place_name": "TEXT",
            "time_period": "TEXT",
            "start_date": "TEXT",
            "data_value": "REAL",
            "fn_indicator_id": "INTEGER",
            "fn_geo_id": "INTEGER",
        },
        column_map={
            "air_quality_id": 0,
            "indicator_id": 1,
            "indicator_name": 2,
            "measure": 3,
            "measure_info": 4,
            "geo_type_name": 5,
            "geo_id": 6,
            "geo_place_name": 7,
            "time_period": 8,
            "start_date": 9,
            "data_value": 10,
            "fn_geo_id": 6,
            "fn_indicator_id": 1,
        },
    )


# Test read data
def test_read_data():
    print(read_data("air_quality.db", "air_quality", 740885))


# Test read all data
def test_read_all_data():
    print(read_all_data("air_quality.db", "air_quality"))


# Test save data
def test_save_data():
    print(save_data("air_quality.db", "geo_data", ["1000", "Lancaster", "UFO"]))


# Test update data
def test_update_data():
    print(
        update_data(
            "air_quality.db", "geo_data", {"geo_place_name": "The Heights"}, 1000
        )
    )


# Test delete data
def test_delete_data():
    print(delete_data("air_quality.db", "geo_data", 1000))


# Test read all column names
def test_get_table_columns():
    print(get_table_columns("air_quality.db", "air_quality"))


if __name__ == "__main__":
    test_extract()
    transform_n_load()
    test_read_data()
    test_read_all_data()
    test_save_data()
    test_update_data()
    test_delete_data()
    test_get_table_columns()
