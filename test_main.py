from my_lib.extract import extract
from my_lib.transform import transform_n_load
from my_lib.crud import read_data, read_all_data, save_data, delete_data, update_data


if __name__ == "__main__":
    extract("https://data.cityofnewyork.us/resource/c3uy-2p5r.csv", "air_quality.csv")
    transform_n_load()
    # print(read_data("air_quality.db", "air_quality", 740885))
    # print(read_all_data("air_quality.db", "air_quality"))
    print(save_data("air_quality.db", "geo_data", ["1000", "Lancaster", "UFO"]))
    # print(delete_data("air_quality.db", "geo_data", 1000))
    print(
        update_data(
            "air_quality.db", "geo_data", {"geo_place_name": "The Heights"}, 1000
        )
    )
