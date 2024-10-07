import json
from my_lib.extract import extract
from my_lib.transform import transform_n_load


if __name__ == "__main__":
    extract("https://data.cityofnewyork.us/resource/c3uy-2p5r.csv", "air_quality.csv")
    transform_n_load()
