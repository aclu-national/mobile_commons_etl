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
ALL_ENDPOINTS = ["messages"]
INDEX_SET = {"campaigns": "campaign_id"}

RS_INCREMENTAL_KEYS = {
    "sent_messages": "sent_at",
    "campaigns": None,
    "messages": "received_at",
}

API_INCREMENTAL_KEYS = {
    "sent_messages": "start_time",
    "campaigns": None,
    "messages": "start_time",
}

UP_TO = {"messages": "end_time"}

ENDPOINT_KEY = {
    1: {"campaigns": "campaigns", "sent_messages": "messages", "messages": "messages"},
    0: {"campaigns": "campaign", "sent_messages": "message", "messages": "message"},
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
IGNORE_INDEX_FILTER = True

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
            "auth": AUTH,
            "schema": SCHEMA,
            "table_prefix": TABLE_PREFIX,
            "db_incremental_key": RS_INCREMENTAL_KEYS[index],
            "include_opt_in_paths":INCLUDE_OPT_IN_PATHS,
        }

        tap = mc.mobile_commons_connection(index, full_build, **keywords)
        tap.fetch_latest_timestamp()
        tap.page_count = tap.page_count_get(**keywords, page=MIN_PAGES)

        print(
            "Kicking off extraction for endpoint {}...".format(str.upper(index)),
            flush=True,
            file=sys.stdout,
        )

        data = tap.ping_endpoint(**keywords)
        indices = set(data["id"])
        #indices = [str(ix) for ix in indices if str(ix) == "212448"] #you can use this to filter out individual campaign ids when testing
        index_results = []
        original_timestamp = None

        for i in indices:

            for ENDPOINT in ALL_ENDPOINTS:

                if str.lower(FULL_REBUILD_FLAG) == "true":

                    full_build = True

                else:

                    full_build = False

                extrakeys = {
                    "api_incremental_key": API_INCREMENTAL_KEYS[ENDPOINT],
                    "db_incremental_key": RS_INCREMENTAL_KEYS[ENDPOINT],
                    "up_to" : UP_TO[ENDPOINT],
                    INDEX_SET[index]: i,
                }
                keywords.update(extrakeys)
                subtap = mc.mobile_commons_connection(ENDPOINT, full_build, **keywords)
                subtap.index = INDEX_SET[index]

                if original_timestamp is None:
                    original_timestamp = subtap.fetch_latest_timestamp(ignore_index_filter = IGNORE_INDEX_FILTER)
                    #passing the ignore_index_filter argument so that we can take
                    #the max of activated_at from the all the campaign subscriber
                    #records (not just for each campaign as it was doing before)
                else:
                    subtap.fetch_latest_timestamp(ignore_index_filter = IGNORE_INDEX_FILTER,passed_last_timestamp=original_timestamp)

                print(
                    "Kicking off extraction for endpoint {} CAMPAIGN {}...".format(
                        str.upper(ENDPOINT), i
                    ),
                    flush=True,
                    file=sys.stdout,
                )

                ### Page count in results is given for messages endpoints, no need to guess

                subtap.page_count = subtap.page_count_get(**keywords, page=MIN_PAGES)

                if subtap.page_count > 0:

                    print(
                        "There are {} pages in the result set for endpoint {} and CAMPAIGN {}".format(
                            subtap.page_count, str.upper(ENDPOINT), i
                        )
                    )

                    subtap.ping_endpoint(**keywords)

                else:

                    print(
                        "No new results to load for endpoint {} CAMPAIGN {}".format(
                            str.upper(ENDPOINT), i
                        )
                    )

if __name__ == "__main__":

    main()
