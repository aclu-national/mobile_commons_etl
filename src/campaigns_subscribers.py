"""API documentation can be found at: https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API"""

import requests
import pandas as pd
import xmltodict
import json
import os
import sys
import math
import datetime
import pytz
import dateparser
import asyncio
import aiohttp
import mobile_commons_etl as mc

from pandas.io.json import json_normalize
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

FULL_REBUILD_FLAG = os.getenv("FULL_REBUILD_FLAG")
MC_USER = os.getenv("MC_USERNAME")
MC_PWD = os.getenv("MC_PASSWORD")
SCHEMA = os.getenv("SCHEMA")
TABLE_PREFIX = os.getenv("TABLE_PREFIX")

URL = "https://secure.mcommons.com/api/"
ALL_ENDPOINTS = ["campaign_subscribers"]
INDEX_SET = {"campaigns": "campaign_id"}

RS_INCREMENTAL_KEYS = {"campaign_subscribers": "activated_at", "campaigns": None}
#RS_INCREMENTAL_KEYS is "activated_at" which is the timestamp we're using in campaign_subscribers to determine from when we should increment
#then it is passed into sql that queries the table within the func get_latest_record. Then the returned result is passed into the handler fetch_latest_timestamp
API_INCREMENTAL_KEYS = {"campaign_subscribers": "from", "campaigns": None}
#then within get_page API_INCREMENTAL_KEYS is set equal to the latest timestamp which is whats returned from fetch_latest_timestamp
#and it becomes the from parameter that is passed when making the api call in get_page
UP_TO = {"campaign_subscribers": "to"}

ENDPOINT_KEY = {
    1: {"campaigns": "campaigns", "campaign_subscribers": "subscriptions"},
    0: {"campaigns": "campaign", "campaign_subscribers": "sub"},
}

MIN_PAGES = 1
MAX_PAGES = 20000
LIMIT = 500
AUTH = aiohttp.BasicAuth(MC_USER, password=MC_PWD)

# Mobile Commons API allows up to 160 concurrent connections but they asked us to reduce to 80 for now
SEMAPHORE = asyncio.BoundedSemaphore(80)

retries = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
retry_adapter = HTTPAdapter(max_retries=retries)

http = requests.Session()
http.mount("https://secure.mcommons.com/api/", retry_adapter)
INCLUDE_OPT_IN_PATHS = 1

def main():

    for index in INDEX_SET.keys():

        full_build = True

        keywords = {
            "session": http,
            "user": MC_USER,
            "pw": MC_PWD,
            "base": URL,
            "endpoint_key": ENDPOINT_KEY,
            "api_incremental_key": API_INCREMENTAL_KEYS[index],
            "limit": LIMIT,
            "min_pages": MIN_PAGES,
            "max_pages": MAX_PAGES,
            "semaphore": SEMAPHORE,
            "schema": SCHEMA,
            "table_prefix": TABLE_PREFIX,
            "auth": AUTH,
            "db_incremental_key": RS_INCREMENTAL_KEYS[index],
            "include_opt_in_paths":INCLUDE_OPT_IN_PATHS,
        }

        tap = mc.mobile_commons_connection(index, full_build, **keywords)
        tap.fetch_latest_timestamp() #result: gets the latest timestamp in RS for this call // fetch_latest_timestamp calls on get_latest_record
        tap.page_count = tap.page_count_get(**keywords, page=MIN_PAGES) #makes a call to see how many pages there are and returns the value.

        print(
            "Kicking off extraction for endpoint {}...".format(str.upper(index)),
            flush=True,
            file=sys.stdout,
        )

        data = tap.ping_endpoint(**keywords)
        #if page count is greater than 500, it will find break up the total # of pages into chunks & make the calls a chunk at a time.
        #so if there are 1858 pages & it calculates that 4 chunks us ideal then the overall partition will look like [1, 620, 1239, 1859].
        #calls on get_page which makes the async calls & attempts retries if necessary
        template = pd.DataFrame(columns=tap.columns)
        df = pd.concat([template, data], sort=True, join="inner")

        print(
            "Loading data from endpoint {} into database...".format(
                str.upper(index), flush=True, file=sys.stdout
            )
        )

        tap.load(df, index)

        indices = set(data["id"])
        #indices = [str(ix) for ix in indices if str(ix) == "209901"]
        #and str(ix) != "210789"]
        index_results = []

        for i in indices:

            for ENDPOINT in ALL_ENDPOINTS:

                if str.lower(FULL_REBUILD_FLAG) == "true":

                    full_build = True

                else:

                    full_build = False

                extrakeys = {
                    "api_incremental_key": API_INCREMENTAL_KEYS[ENDPOINT], #from
                    "db_incremental_key": RS_INCREMENTAL_KEYS[ENDPOINT], #activated_at
                    "up_to" : UP_TO[ENDPOINT],
                    INDEX_SET[index]: i
                }

                keywords.update(extrakeys)
                subtap = mc.mobile_commons_connection(ENDPOINT, full_build, **keywords)
                subtap.index = INDEX_SET[index]
                subtap.fetch_latest_timestamp()

                print(
                    "Kicking off extraction for endpoint {} CAMPAIGN {}...".format(
                        str.upper(ENDPOINT), i
                    ),
                    flush=True,
                    file=sys.stdout,
                )

                if subtap.page_count_get(**keywords, page=MIN_PAGES) > 0:

                    print("Guessing page count...")

                    subtap.page_count = subtap.get_page_count(**keywords)

                    print(
                        "There are {} pages in the result set for endpoint {} and CAMPAIGN {}".format(
                            subtap.page_count, str.upper(ENDPOINT), i
                        )
                    )

                    data = subtap.ping_endpoint(**keywords)
                    template = pd.DataFrame(columns=subtap.columns)

                    if data is not None:

                        df = pd.concat([template, data], sort=True, join="inner")
                        df[INDEX_SET[index]] = str(i)
                        index_results.append(df)

                else:

                    print(
                        "No new results to load for endpoint {} CAMPAIGN {}".format(
                            str.upper(ENDPOINT), i
                        )
                    )
        if len(index_results) > 0:

            all_results = pd.concat(index_results, sort=True, join="inner")

            print(
                "Loading data from endpoint {} into database...".format(
                    str.upper(ENDPOINT), flush=True, file=sys.stdout
                )
            )

            subtap.load(all_results, ENDPOINT)

        else:

            print(
                "No new data from endpoint {}. ".format(
                    str.upper(ENDPOINT), flush=True, file=sys.stdout
                )
            )



if __name__ == "__main__":

    main()
