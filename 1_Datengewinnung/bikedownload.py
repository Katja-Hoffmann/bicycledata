import csv
import datetime
import requests
import time
from pymongo import MongoClient
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError


def runner():
    def request_data(offset):
        api_token = "SECRETTOKEN"
        lat = "53.551086"
        lon = "9.993682"
        radius = "30000"
        limit = "100"
        providernetwork = "2"

        request_headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer SECRETTOKEN'
        }

        # Make the API call.
        try:
            with requests.get(
                    "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?" +
                    "lat=" + lat +
                    "&" +
                    "lon=" + lon +
                    "&" +
                    "radius=" + radius +
                    "&" +
                    "offset=" + str(offset) +
                    "&" +
                    "limit=" + limit +
                    "&" +
                    "providernetwork=" + providernetwork,
                    headers=request_headers,
                    stream=True) as r:
                if r.status_code == 200:
                    result = r.json()['items']
                else:
                    time.sleep(10)
                    result = request_data(offset)
        except (ChunkedEncodingError, ProtocolError, ValueError):
            time.sleep(10)
            result = request_data(offset)

        return result

    offset = 0
    result_list = request_data(offset)
    longlist = result_list

    numberofresults = len(result_list)

    while numberofresults > 0:
        print(offset)
        offset += numberofresults
        result_list = request_data(offset)
        time.sleep(sleeptimebetweenrequests)
        numberofresults = len(result_list)
        longlist = longlist + result_list
        print(result_list)
    print(offset)

    print(len(longlist))
    print("gonna die soon")

    mongodbconnectionstring = "mongodb://mongo:27017"
    client = MongoClient(mongodbconnectionstring)
    db = client.bikedata
    collection = db.bikedata

    post = {"date": datetime.datetime.utcnow(),
            "items": longlist}
    posts = db.posts
    post_id = posts.insert_one(post);
    print(post_id)
    print(datetime.datetime.utcnow())


while True:
    sleeptimebetweenrequests = 1
    sleeptimefail = 10

    runner()

    time.sleep(300)

raise SystemExit(0)

