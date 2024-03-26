#
# Parent class ScraperBase
# with all features needed for a scraper for au.indeed.com
#
# ===============================================================


import smtplib
from contextlib import contextmanager
from datetime import datetime
from itertools import cycle

import pandas as pd
from psycopg2.pool import ThreadedConnectionPool

from utils.utils import download_free_proxies

# 'object' passing into class makes it a new-style class in modern python


class ScraperBase:
    """A base class for scraper for Indeed.com.au"""

    def __init__(self):
        self.NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.connect_db()
        proxies = self.load_proxies()
        headers = self.load_user_headers()
        self.proxy_pool = cycle(proxies)
        self.header_pool = cycle(headers)

    # +  -  -  - PROXIES AND HEADERS -  -  - +

    def load_proxies(self):
        """Load proxies from csv file and return a set of proxies"""
        proxies = set()
        try:
            df = pd.read_csv("./utils/proxy_files/proxies.csv")
            for i, r in df.iterrows():
                proxy = ":".join([r["IP Address"], str(r["Port"])[:-2]])
                proxies.add(proxy)
        except Exception as e:
            print(e)

        return proxies

    def load_user_headers(self):
        """Load headers from csv file and return a set of headers"""
        headers = set()
        df = pd.read_csv("./utils/proxy_files/user_agents.csv")
        for i, r in df.iterrows():
            headers.add(r["User agent"])
        return headers

    def get_proxies(self):
        """Get the next proxy in proxy pool"""
        proxy = next(self.proxy_pool)
        return {"http": proxy, "https": proxy}

    def get_headers(self):
        """Get the next header in header pool"""
        headers = next(self.header_pool)
        return {"User-Agent": headers}

    def reset_proxy_pool(self):
        """Download new proxies, save to csv and load csv"""
        download_free_proxies()
        proxies = self.load_proxies()
        self.proxy_pool = cycle(proxies)

    # +  -  -  - DATABASE -  -  - +

    def connect_db(self):
        """Connect to db on cloud"""
        self._connpool = ThreadedConnectionPool(
            1,
            2,
            database="bitko",
            user="steve",
            host="localhost",
            port="5432",
        )

    @contextmanager
    def cursor(self):
        """Get a cursor from the conn pool"""

        # Get available connection from pool
        conn = self._connpool.getconn()
        conn.autocommit = True
        try:
            # Return a generator cursor() created on the fly
            yield conn.cursor()
        finally:
            # Return the connection back to connection pool
            self._connpool.putconn(conn)

    def query_list(self, sql):
        with self.cursor() as cur:
            cur.execute(sql)
            results = [i for i in cur.fetchall()]
        return results

    def query_one(self, sql):
        with self.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
        return result

    def execute(self, sql):
        """Execute sql command"""
        with self.cursor() as cur:
            cur.execute(sql)

    # +  -  -  - QUERIES -  -  - +

    def creat_table_db(self, site):
        """
        Create table for job scraper
        # Columns:
            primary key, info, jd
        """

        sql = """
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE TABLE IF NOT EXISTS {}jobs
            (
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                info json,
                jd TEXT
            );
          """.format(
            site
        )

        with self.cursor() as cur:
            cur.execute(sql)

    def create_monitor_table(self):
        sql = """
            CREATE TABLE IF NOT EXISTS jobscrapers
            (
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                body json
            );
          """
        with self.cursor() as cur:
            cur.execute(sql)

    def to_table_db(self, new_info, site):
        """Add new information scraped to db table"""

        sql = """
            INSERT INTO {}jobs (info)
            VALUES ('{}');
          """

        with self.cursor() as cur:
            cur.execute(sql.format(site, new_info))

    def check_existed_jobid(self, jobid, site):
        """Query db to see if a job already existed"""

        sql = """
            SELECT info->>'jobid'
            FROM {}jobs
            WHERE info->>'jobid'='{}';
            """.format(
            site, jobid
        )

        result = self.query_one(sql)
        if result:
            return True
        return False

    def jd_to_db(self, jobid, content, site):
        """Save jd to db"""

        sql = """
            UPDATE {}jobs
            SET jd = '{}'
            WHERE (info ->> 'jobid') = '{}';
            """.format(
            site, content, jobid
        )

        with self.cursor() as cur:
            cur.execute(sql)

    def check_existed_jd(self, jobid, site):
        """Check if jd of jobid already scraped"""

        sql = """
            SELECT jd FROM {}jobs
            WHERE (info ->> 'jobid') = '{}'
            AND jd IS NOT NULL;
            """.format(
            site, jobid
        )
        result = self.query_one(sql)

        if result:
            return True
        return False

    def jobs_missing_jd(self, site):
        """Get jobid from main table where jd is missing"""

        sql = """
            SELECT info->>'jobid' from {}jobs
            WHERE
            jd is null;
            """.format(
            site
        )

        result = [i[0] for i in self.query_list(sql)]
        return result

    # +  -  -  - NOTIFY EXCEPTIONS -  -  - +
    # reference: http://naelshiab.com/tutorial-send-email-python/

    def notify_exception(self, error):
        """Notify through email when exceptions occur"""

        my_mail = "1199abcxyz@gmail.com"
        bot_mail = "melbdatahackers@gmail.com"
        pwd = "Melbourne123."

        server = smtplib.SMTP("smtp.gmail.com:587")
        server.ehlo()
        server.starttls()
        server.login(bot_mail, pwd)

        msg = str(error)
        server.sendmail(bot_mail, [my_mail], msg)
        server.quit()
