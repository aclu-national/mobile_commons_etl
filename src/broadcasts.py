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
ALL_ENDPOINTS = ["broadcasts"]

RS_INCREMENTAL_KEYS = {"broadcasts": "delivery_time"}
API_INCREMENTAL_KEYS = {"broadcasts": "start_time"}

ENDPOINT_KEY = {
    1: {"broadcasts": "broadcasts"},
    0: {"broadcasts": "broadcast"},
}

MIN_PAGES = 1
MAX_PAGES = 20000
LIMIT = 1000
AUTH = aiohttp.BasicAuth(MC_USER, password=MC_PWD)

# Mobile Commons API allows up to 160 concurrent connections but they asked us to reduce to 80 for now
SEMAPHORE = asyncio.BoundedSemaphore(80)

retries = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
retry_adapter = HTTPAdapter(max_retries=retries)

http = requests.Session()
http.mount("https://secure.mcommons.com/api/", retry_adapter)


def main():

    for ENDPOINT in ALL_ENDPOINTS:

        full_build = True

        keywords = {
            "session": http,
            "user": MC_USER,
            "pw": MC_PWD,
            "base": URL,
            "endpoint_key": ENDPOINT_KEY,
            "api_incremental_key": API_INCREMENTAL_KEYS[ENDPOINT],
            "limit": LIMIT,
            "min_pages": MIN_PAGES,
            "max_pages": MAX_PAGES,
            "semaphore": SEMAPHORE,
            "schema": SCHEMA,
            "table_prefix": TABLE_PREFIX,
            "auth": AUTH,
            "db_incremental_key": RS_INCREMENTAL_KEYS[ENDPOINT],
        }

        tap = mc.mobile_commons_connection(ENDPOINT, full_build, **keywords)
        tap.fetch_latest_timestamp()

        print(
            "Kicking off extraction for endpoint {}...".format(str.upper(ENDPOINT)),
            flush=True,
            file=sys.stdout,
        )

        tap.page_count = tap.page_count_get(**keywords, page=MIN_PAGES)

        if tap.page_count > 0:

            tap.ping_endpoint(**keywords)

if __name__ == "__main__":

    main()
