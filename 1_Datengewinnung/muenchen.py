import datetime
import http
from http.client import IncompleteRead
from ssl import SSLError
import requests
import time



import urllib3
from pymongo import MongoClient
import requests.exceptions
from urllib3.exceptions import ProtocolError


def runner():
    def request_data(offset):
        api_token = "SECRETTOKEN"
        lat = "48.137154"
        lon = "11.576124"
        radius = "30000"
        limit = "100"
        providernetwork = "2"

        request_headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + api_token
        }

        # Make the HTTP API call.
        r = requests.get(
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
                stream=True)
        if r.status_code == 200:
            result = r.json()['items']
        else:
            raise ValueError
        return result

    offset = 0
    nomoreresults = False
    longlist = []
    test = False

    numberofresults = 0

    while nomoreresults is False:
        print(offset)
        try:
            result_list = request_data(offset)
        except SSLError:
            print("SSL")
            time.sleep(5)
            continue
        except ValueError:
            print("Value")
            time.sleep(5)
            continue
        except ProtocolError:
            print("Proto")
            time.sleep(5)
            continue
        except IncompleteRead:
            print("Incomplete")
            time.sleep(5)
            continue
        except requests.exceptions.ChunkedEncodingError:
            print("chunky")
            time.sleep(5)
            continue
        except requests.exceptions.ConnectionError:
            print("connerr")
            time.sleep(5)
            continue
        if test is True:
            test = False
            time.sleep(5)
            continue
        numberofresults = len(result_list)
        offset += numberofresults
        if numberofresults == 0:
            nomoreresults = True
        longlist = longlist + result_list
        print(result_list)
        time.sleep(sleeptimebetweenrequests)

    print(offset)

    print(len(longlist))

    mongodbconnectionstring = "mongodb://localhost:27017"
    client = MongoClient(mongodbconnectionstring)
    db = client.bikedata
    collection = db["muenchen_neu"]

    post = {"date": datetime.datetime.utcnow(),
            "items": longlist}
    post_id = collection.insert_one(post);
    print(post_id)
    print(datetime.datetime.utcnow())

    # with open('bikesdata.csv', mode='w') as file:
    #     writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #
    #     writer.writerow([datetime.datetime.utcnow(), longlist])


while True:
    sleeptimebetweenrequests = 1
    sleeptimefail = 5

    runner()
    #berlin
    #lat = "52.520008"
    #lon = "13.404954"
    #hh
    #lat = "53.551086"
    #lon = "9.993682"
    #muc
    #lat = "48.137154"
    #lon = "11.576124"

    time.sleep(300)

raise SystemExit(0)
