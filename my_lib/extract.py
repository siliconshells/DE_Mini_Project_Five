"""
Extract data from a url and save as a file
"""

import requests


def extract(
    url="https://data.cityofnewyork.us/resource/c3uy-2p5r.csv",
    file_name="air_quality.csv",
):
    """ "Extract a url to a file path"""
    file_path = "data/" + file_name
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
