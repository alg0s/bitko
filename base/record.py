import ast
import json

from base.base import ScraperBase
from utils.RedisQueue import RedisQueue


class ScraperRecord(ScraperBase):
    """A handler records info for monitoring purpose"""

    def __init__(self, queue_id: str, site_name: str):
        print(">>>> Initializing ScraperRecord...")
        super().__init__()

        self.creat_table_db(site_name)
        self.create_monitor_table()

        # Specify redis queue being used
        self.rqueue = RedisQueue("{}".format(queue_id))

        # Variables keeping raw record
        self.raw_record = {
            "total_time_info": 0,
            "total_time_subcat": 0,
            "total_time_cat": 0,
            "total_time_insert": 0,
            "total_time_select": 0,
            "ssl_errors": 0,
            "proxy_errors": 0,
            "conn_errors": 0,
            "request_errors": 0,
            "other_errors": 0,
            "total_subcat": 0,
            "total_time_jd": 0,
        }

        # Variables keeping record of scraper
        self.record = {}

    # +  -  -  - DATABASE -  -  - +

    def get_total_jobs(self, site):
        """Get total jobs already scraped for 1 site"""

        sql = """
          SELECT COUNT(*) FROM {}jobs;
        """.format(
            site
        )

        result = str(self.query_one(sql)).strip("(,)")
        return result

    def get_null_jd(self, site):
        """Get number of jobs without jd"""

        sql = """
            SELECT COUNT(*) FROM {}jobs
            WHERE jd IS NULL;
        """.format(
            site
        )

        result = str(self.query_one(sql)).strip("(,)")
        return result

    def get_missing_jd(self, site):
        """Get number of jobs with jd = '<missing>'"""

        sql = """
            SELECT COUNT(*) FROM {}jobs
            WHERE jd = '<missing>';
        """.format(
            site
        )

        result = str(self.query_one(sql)).strip("(,)")
        return result

    def record_to_db(self):
        """Insert json record to db"""

        sql = """
            INSERT INTO jobscrapers (body)
            VALUES ('{}');
        """.format(
            json.dumps(self.record)
        )

        with self.cursor() as cur:
            cur.execute(sql)

    # +  -  -  - DATA PROCESSING -  -  - +

    def get_records(self, extra_record):
        """Get records from all scrapers' processes"""

        try:
            while True:
                data = self.rqueue.pop(timeout=10)
                if data:
                    data = ast.literal_eval(data.decode("utf-8"))
                    for key, value in data.items():
                        self.raw_record[key] += float(value)
                else:
                    break
            for key, value in extra_record.items():
                self.raw_record[key] = value
        except Exception as e:
            print(e)
        print("RAW RECORD: {}\n".format(self.raw_record))

    def calc_avg_info(self, jobs, time):
        """Calculate average time to scrape 1 info"""
        try:
            result = float(time) / float(jobs)
            return round(result, 5)
        except Exception as e:
            print("avg info {}".format(e))
            return 0.0

    def calc_avg_jd(self, jobs, time):
        """Calculate average time to scrape 1 jd"""
        try:
            result = float(time) / float(jobs)
            return round(result, 5)
        except Exception as e:
            print("avg jd {}".format(e))
            return 0.0

    def calc_avg_subcat(self, subcat, time):
        """Calculate average time to scrape 1 subcategory"""
        try:
            result = float(time) / float(subcat)
            return round(result, 5)
        except Exception as e:
            print("avg subcat {}".format(e))
            return 0.0

    def calc_avg_cat(self, cat, time):
        """Calculate average time to scrape 1 category"""
        try:
            result = float(time) / float(cat)
            return round(result, 5)
        except Exception as e:
            print("avg cat {}".format(e))
            return 0.0

    def calc_avg_insert(self, jobs, time):
        """Calculate average time to do 1 INSERT query"""
        try:
            result = float(time) / float(jobs)
            return round(result, 5)
        except Exception as e:
            print("avg insert {}".format(e))
            return 0.0

    def calc_avg_select(self, jobs, time):
        """Calculate average time to do 1 SELECT query"""
        try:
            result = float(time) / float(jobs)
            return round(result, 5)
        except Exception as e:
            print("avg select {}".format(e))
            return 0.0

    def prepare_records(self):
        """Calculate all neccessary values for records
        and update record
        """

        self.record["avg_time_info"] = self.calc_avg_info(
            self.raw_record["last_session_jobs"], self.raw_record["total_time_info"]
        )

        self.record["avg_time_jd"] = self.calc_avg_jd(
            self.raw_record["last_session_jobs"], self.raw_record["total_time_jd"]
        )

        self.record["avg_time_subcat"] = self.calc_avg_subcat(
            self.raw_record["total_subcat"], self.raw_record["total_time_subcat"]
        )

        self.record["avg_time_cat"] = self.calc_avg_cat(
            self.raw_record["total_cat"], self.raw_record["total_time_cat"]
        )

        self.record["avg_time_insert"] = self.calc_avg_insert(
            self.raw_record["last_session_jobs"], self.raw_record["total_time_insert"]
        )

        self.record["avg_time_select"] = self.calc_avg_select(
            self.raw_record["last_session_jobs"], self.raw_record["total_time_select"]
        )

        self.record["site"] = self.raw_record["site"]

        self.record["null_jd"] = self.raw_record["null_jd"]

        self.record["total_jobs"] = self.raw_record["total_jobs"]

        self.record["missing_jd"] = self.raw_record["missing_jd"]

        self.record["ssl_errors"] = self.raw_record["ssl_errors"]

        self.record["conn_errors"] = self.raw_record["conn_errors"]

        self.record["other_errors"] = self.raw_record["other_errors"]

        self.record["proxy_errors"] = self.raw_record["proxy_errors"]

        self.record["request_errors"] = self.raw_record["request_errors"]

        self.record["last_session_start"] = self.raw_record["session_start"]

        self.record["last_session_finish"] = self.raw_record["session_finish"]

        self.record["last_session_jobs"] = self.raw_record["last_session_jobs"]
