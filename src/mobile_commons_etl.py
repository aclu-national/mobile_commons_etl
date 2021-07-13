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
import numpy as np
import sqlalchemy
import mobile_commons_data as mcd
import pdb

from sqlalchemy import create_engine
from sqlalchemy import inspect

from pandas.io.json import json_normalize

from datetime import timedelta
from datetime import datetime

DB_DATABASE = os.getenv("DB_DATABASE")
DB_HOST = os.getenv("DB_HOST")
DB_CREDENTIAL_USERNAME = os.getenv("DB_CREDENTIAL_USERNAME")
DB_CREDENTIAL_PASSWORD = os.getenv("DB_CREDENTIAL_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

COLUMNS = mcd.columns()

class mobile_commons_connection:
    def __init__(self, endpoint, full_build, **kwargs):

        self.endpoint = endpoint
        self.full_build = full_build
        self.semaphore = kwargs.get("semaphore", None)
        self.auth = kwargs.get("auth", None)
        self.limit = kwargs.get("limit", None)
        self.base = kwargs.get("base", None)
        self.endpoint_key = kwargs.get("endpoint_key", None)
        self.columns = COLUMNS.columns[endpoint]
        self.page_count = kwargs.get("page_count", None)
        self.session = kwargs.get("session", None)
        self.user = kwargs.get("user", None)
        self.pw = kwargs.get("pw", None)
        self.base = kwargs.get("base", None)
        self.api_incremental_key = kwargs.get("api_incremental_key", None)
        self.min_pages = kwargs.get("min_pages", None)
        self.max_pages = kwargs.get("max_pages", None)
        self.last_timestamp = kwargs.get("last_timestamp", None)
        self.group_id = kwargs.get("group_id", None)
        self.campaign_id = kwargs.get("campaign_id", None)
        self.url_id = kwargs.get("url_id", None)
        self.index = None
        self.index_id = self.group_id or self.url_id or self.campaign_id
        self.db_incremental_key = kwargs.get("db_incremental_key", None)
        self.up_to = kwargs.get("up_to", None)
        self.include_opt_in_paths = kwargs.get("include_opt_in_paths", None)
        #used in campaigns.py & anything that calls the campaign endpoint
        self.schema = kwargs.get("schema", "public")
        self.table_prefix = kwargs.get("table_prefix", "")
        self.sql_engine = create_engine(
            "postgresql://"
            + DB_CREDENTIAL_USERNAME
            + ":"
            + DB_CREDENTIAL_PASSWORD
            + "@"
            + DB_HOST
            + ":"
            + DB_PORT
            + "/"
            + DB_DATABASE,
            pool_pre_ping=True
        )

    async def get_page(self, page, retries=5, **kwargs):
        """Base asynchronous request function"""

        if self.endpoint == "campaigns":
            params = {"page": page, "include_opt_in_paths": kwargs["include_opt_in_paths"]}
        else:
            params = {"page": page} # page here is page number

        if self.group_id is not None:
            params["group_id"] = self.group_id

        if self.campaign_id is not None:
            params["campaign_id"] = self.campaign_id

        if (self.api_incremental_key is not None) & (self.last_timestamp is not None) & (~self.full_build):
            params[self.api_incremental_key] = self.last_timestamp #from
            last_timestamp_datetime = datetime.strptime(self.last_timestamp , '%Y-%m-%d %H:%M:%S%z')
            up_to_date = last_timestamp_datetime + timedelta(days=30) #to
            params[self.up_to] = up_to_date.strftime('%Y-%m-%d %H:%M:%S%z')

        if self.url_id is not None:
            params["url_id"] = self.url_id

        if self.limit is not None:
            params["limit"] = self.limit

        url = f"{self.base}{self.endpoint}"
        print(f"Fetching page {page}")


        TIMEOUT = aiohttp.ClientTimeout(total=85*85)

        attempts = 1
        data = None

        while data is None or attempts <= retries:

            try:
                async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
                    async with self.semaphore, session.get(
                        url, params=params, auth=self.auth
                    ) as resp:
                        resp.raise_for_status()
                        print(f"{resp.url} status: {resp.status}")
                        data = await resp.text()
                        return data

            except aiohttp.ClientError:
                print(f"Retrying {page}...")
                attempts += 1
                await asyncio.sleep(1)

    def ping_endpoint(self, **kwargs):
        """Wrapper for asynchronous calls that then have results collated into a dataframe"""
        loop = asyncio.get_event_loop()

        # Chunks async calls into bundles if page count is greater than 200

        if self.page_count > 200:
            partition_size = int(
                math.ceil(
                    self.page_count ** (1 - math.log(200) / math.log(self.page_count))
                )
            )
            breaks = [
                int(n)
                for n in np.linspace(1, self.page_count + 1, partition_size).tolist()
            ]
        else:
            breaks = [1, self.page_count + 1]

        res = []

        for b in range(1, len(breaks)):

            temp = loop.run_until_complete(
                asyncio.gather(
                    *(
                        self.get_page(page, **kwargs)
                        for page in range(breaks[b - 1], breaks[b])
                    )
                )
            )

            df_agg = self.parse_temp_and_load(temp)
            return df_agg

    def parse_temp_and_load(self,res):
        endpoint_key_0 = self.endpoint_key[0][self.endpoint]
        endpoint_key_1 = self.endpoint_key[1][self.endpoint]
        res_list = []

        for r in res:
            try:
                if (
                    json.loads(json.dumps(xmltodict.parse(r))).get("response")
                    is not None
                ):
                    json_xml = json.loads(json.dumps(xmltodict.parse(r)))
                    page_result = json_normalize(
                        json_xml["response"][endpoint_key_1][endpoint_key_0],
                        max_level=0,
                    )
                    res_list.append(page_result)
            except:
                print("Improperly formatted XML response... skipping")
                continue


        df_agg = pd.concat(res_list, sort=True, join="outer")
        df_agg.columns = [
            c.replace(".", "_").replace("@", "").replace("@_", "").replace("_@", "")
            for c in df_agg.columns
        ]

        df_agg = df_agg.loc[:, df_agg.columns.isin(list(self.columns.keys()))]

        template = pd.DataFrame(columns=self.columns)
        df_agg_concat = pd.concat([template, df_agg], sort=True, join="outer")

        if self.endpoint in ["campaign_subscribers","sent_messages","messages","clicks","group_members"]:
            df_agg_concat[self.index] = str(self.index_id)

        if df_agg_concat is not None:
            print(
                "Loading data from endpoint {} into database...".format(
                    str.upper(self.endpoint), flush=True, file=sys.stdout
                )
            )
            self.load(df_agg_concat, self.endpoint)
        else:

            print("No new results to load for endpoint {}".format(str.upper(ENDPOINT)))
        return df_agg

    def get_latest_record(self, endpoint,ignore_index_filter=False):
        """Pulls the latest record from the database to use for incremental updates"""

        table = f"{self.schema}.{self.table_prefix}_{endpoint}"

        index_filter = """"""
        if self.index is not None and ignore_index_filter is False:
            index_filter = (
                """ where """ + self.index + """::integer = """ + str(self.index_id)
            )

        sql = (
            """select to_timestamp(max(case when """
            + self.db_incremental_key
            + """ = 'None' or """
            + self.db_incremental_key
            + """ = 'nan' then null else """
            + self.db_incremental_key
            + """::timestamp end),'YYYY-MM-DD HH24:MI:SS TZ') as latest_date from {}""".format(table)
            + index_filter
        )

        ins = inspect(self.sql_engine)

        #if the table DNE then set 2020-10-01 as the date to start from
        if (ins.has_table(f"{self.table_prefix}_{endpoint}",schema=self.schema) == False) & (self.endpoint in ["campaign_subscribers","sent_messages","messages","clicks"]):
            first_record_sql = "select to_timestamp('2020-10-01 00:00:00+00:00'::timestamp,'YYYY-MM-DD HH24:MI:SS TZ') as latest_date"
            date = pd.read_sql(first_record_sql, self.sql_engine)
            latest_date = self.parse_datetime(date,"latest_date")
            print(f"Grabbing records starting from {latest_date}.")

        #profiles has some profiles that were created in september
        elif (ins.has_table(f"{self.table_prefix}_{endpoint}",schema=self.schema) == False) & (self.endpoint in ["profiles","group_members"]):
            first_record_sql = "select to_timestamp('2020-09-01 00:00:00+00:00'::timestamp,'YYYY-MM-DD HH24:MI:SS TZ') as latest_date"
            date = pd.read_sql(first_record_sql, self.sql_engine)
            latest_date = self.parse_datetime(date,"latest_date")
            print(f"Grabbing records starting from {latest_date}.")

        #if the endpoint is campaign_subscribers and the table exists, then take the max of both activated_at and opted_out_at
        elif (ins.has_table(f"{self.table_prefix}_{endpoint}",schema=self.schema) == True) & (self.endpoint == "campaign_subscribers"):
            sql = (
                """select to_timestamp(max(case when """
                + self.db_incremental_key
                + """ = 'None' or """
                + self.db_incremental_key
                + """ = 'nan' then null when opted_out_at = 'None' or opted_out_at = 'nan' then null else date_add('s',1,greatest("""
                + self.db_incremental_key
                + """,opted_out_at)::timestamp) end),'YYYY-MM-DD HH24:MI:SS TZ') as latest_date from {}""".format(table)
                + index_filter
            )

            date = pd.read_sql(sql, self.sql_engine)
            latest_date = self.parse_datetime(date,"latest_date")
            print(f"Grabbing records starting from {latest_date}.")

        #take the max of the datetime column we're using to increment and add a second to the that datetime so we avoid dupes
        else:
            date_for_last_record = pd.read_sql(sql, self.sql_engine)

            if (date_for_last_record.shape[0] > 0) & (date_for_last_record["latest_date"][0] is not None):
                latest_date = self.parse_datetime(date_for_last_record,"latest_date")
                sql = (
                    """select to_timestamp(dateadd(s,1,'"""
                    + latest_date
                    + """'::timestamp),'YYYY-MM-DD HH24:MI:SS TZ') as latest_date"""
                )
                date = pd.read_sql(sql, self.sql_engine)
                latest_date = self.parse_datetime(date,"latest_date")
                print(f"Grabbing records starting from {latest_date}.")
                #latest_date here is already one second plus the activated_at timestamp of the last record uploaded in the previous upload.
                #if there was no prior upload then its the hardocded value of 2020-10-01 00:00:00+00:00
            else:
                latest_date = None

        return latest_date

    def parse_datetime(self,df,item_to_parse):
        date = str(df[item_to_parse][0])
        utc = pytz.timezone("UTC")
        dateparser.parse(date).astimezone(utc).isoformat()
        return date

    def fetch_latest_timestamp(self,ignore_index_filter=False,passed_last_timestamp=None):
        """Handler for pulling latest record if incremental build"""

        if passed_last_timestamp is not None:
            self.last_timestamp = passed_last_timestamp
            return self.last_timestamp

        if (not self.full_build) & (self.db_incremental_key is not None):

            print(
                "Getting latest record for endpoint {}...".format(
                    str.upper(self.endpoint)
                ),
                flush=True,
                file=sys.stdout,
            )
            self.last_timestamp = self.get_latest_record(self.endpoint,ignore_index_filter=ignore_index_filter)

        else:
            self.last_timestamp = None
            self.full_build = True

        return self.last_timestamp

    def page_count_get(self, page, **kwargs):
        """
        Returns metadata around whether a request has non-zero result set
        since endpoints are paginated but page count isn't always given
        """

        endpoint_key_0 = self.endpoint_key[0][self.endpoint]
        endpoint_key_1 = self.endpoint_key[1][self.endpoint]

        if self.endpoint == "campaigns":
            params = {"page": page, "include_opt_in_paths": kwargs["include_opt_in_paths"]}
        else:
            params = {"page": page} # page here is page number

        if self.limit is not None:
            params["limit"] = self.limit

        if (self.api_incremental_key is not None) & (self.last_timestamp is not None):
            params[self.api_incremental_key] = self.last_timestamp #from
            last_timestamp_datetime = datetime.strptime(self.last_timestamp , '%Y-%m-%d %H:%M:%S%z')
            up_to_date = last_timestamp_datetime + timedelta(days=30) #to
            params[self.up_to] = up_to_date.strftime('%Y-%m-%d %H:%M:%S%z')

        if self.group_id is not None:
            params["group_id"] = self.group_id

        if self.campaign_id is not None:
            params["campaign_id"] = self.campaign_id

        if self.url_id is not None:
            params["url_id"] = self.url_id

        resp = self.session.get(
            self.base + self.endpoint, auth=(self.user, self.pw), params=params
        )

        print(f"{resp.url}")
        formatted_response = json.loads(json.dumps(xmltodict.parse(resp.text)))["response"][endpoint_key_1]

        ### Sina: 7/20/20 only broadcasts, messages, & sent_messages endpoints have page_count field this at this point in time
        if formatted_response.get("@page_count") is not None:
            num_results = int(formatted_response.get("@page_count"))

        elif formatted_response.get("page_count") is not None:
            num_results = int(formatted_response.get("page_count"))

        elif formatted_response.get(endpoint_key_0) is not None:
            num_results = 1

        else:
            num_results = 0

        return num_results

    def get_page_count(self, **kwargs):
        """Binary search to find page count for an endpoint if page count data isn't available so we can parallelize downstream"""

        guess = math.floor((self.min_pages + self.max_pages) / 2)
        diff = self.max_pages - self.min_pages

        print(f"Page count guess: {guess}")
        kwargs["page"] = guess

        if (self.page_count_get(**kwargs) > 0) & (diff > 1):
            self.min_pages = guess
            return self.get_page_count(**kwargs)

        elif (self.page_count_get(**kwargs) == 0) & (diff > 1):
            self.max_pages = guess
            return self.get_page_count(**kwargs)

        else:
            print(f"Page count converged! Final count: {guess}")
            return guess

    def map_dtypes(self, value):

        if value == "int64":
            return sqlalchemy.types.BIGINT()
        elif value == "str":
            return sqlalchemy.types.NVARCHAR(length=65535)
        elif value == "float64":
            return sqlalchemy.types.Float(asdecimal=True)
        elif value == "datetime64[ns, <tz>]":
            return sqlalchemy.types.DateTime(timezone=True)
        elif value == "bool":
            return sqlalchemy.types.BOOLEAN()
        else:
            return sqlalchemy.types.NVARCHAR(length=65535)

    def load(self, df, endpoint):
        """Loads to database"""
        mapper = {k: self.map_dtypes(v) for k, v in self.columns.items()}
        x = set(df.columns)
        y = set(self.columns.keys())
        final_cols = {i: self.columns[i] for i in x.intersection(y)}
        df = df.astype(final_cols)
        df = df.replace({None: sqlalchemy.sql.null()})
        df = df.replace({'None': sqlalchemy.sql.null()})

        if self.full_build:

            df.to_sql(
                f"{self.table_prefix}_{endpoint}",
                self.sql_engine,
                schema=self.schema,
                if_exists="replace",
                index=False,
                dtype=mapper,
                method="multi",
                chunksize=300,
            )

        else:
            df.to_sql(
                f"{self.table_prefix}_{endpoint}",
                self.sql_engine,
                schema=self.schema,
                if_exists="append",
                index=False,
                dtype=mapper,
                method="multi",
                chunksize=300,
            )
