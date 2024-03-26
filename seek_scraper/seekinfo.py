# -*- coding: utf-8 -*-


#
# Scraper dealing with general information of job ad for seek.com.au
# Inherited from seekbase
#
# =====================================================================


import json
import logging
import logging.handlers as handlers
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from base.base import ScraperBase
from settings.seeksettings import SEEK_LINK, URL
from utils.RedisQueue import RedisQueue


class SeekJobInfoScraper(ScraperBase):
    """A handler scrapes jobs without full description"""

    def __init__(self):
        super().__init__()
        # Specify redis queue being used
        self.rqueue = RedisQueue("seek_queue")

        # Variables keeping record of scraper
        self.record = {
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
        }

        # Setting up logger
        # REFERENCE:
        # https://tutorialedge.net/python/python-logging-best-practices/
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

        logHandler = handlers.RotatingFileHandler(
            "./seek_scraper/logs/info_scrape_log.log", maxBytes=1500000, backupCount=0
        )

        formatter = logging.Formatter("%(levelname)s:%(message)s")
        logHandler.setFormatter(formatter)
        logHandler.setLevel(logging.DEBUG)

        if not self.log.handlers:
            self.log.addHandler(logHandler)

        self.key = [
            "jobTitle",
            "jobCompany",
            "jobLocation",
            "jobClassification",
            "jobSubClassification",
            "jobArea",
            "jobListingDate",
            "jobShortDescription",
        ]

    def get_records(self):
        """Return dictionary with key = areas being recorded"""
        return self.record

    def get_original_post_time(self, jobListingDate, days):
        """Calculate the original post time of a job by Seek"""

        posted = jobListingDate[:-4]
        posted_today = True
        if "d" in posted:
            gap = timedelta(days=int(posted[:-1]))
            if int(posted[:-1]) > int(float(days)):
                posted_today = False
        elif "h" in posted:
            gap = timedelta(hours=int(posted[:-1]))
        elif "m" in posted:
            gap = timedelta(minutes=int(posted[:-1]))
        elif "s" in posted:
            gap = timedelta(seconds=int(posted[:-1]))

        origin_time = (datetime.now() - gap).strftime("%Y-%m-%d %H:%M")
        return origin_time, posted_today

    def page_zero_result(self, soup):
        """Check if the page has any job"""
        zero = soup.find("div", attrs={"data-automation": "searchZeroResults"})
        if zero:
            return True
        return False

    def total_jobs_found(self, soup):
        total_jobs = soup.find("strong", attrs={"data-automation": "totalJobsCount"})
        if total_jobs:
            return total_jobs.get_text()
        return None

    def job_by_industry(self, industry, days=1):
        """Scrape all jobs of a given industry"""

        start_cat = time.time()

        msg = "Start scraping for: {}".format(industry.upper())
        print(msg)
        self.log.info(msg)

        posted_today = True
        jobs = []
        i = 1
        page_num = 1
        redirects = 0
        total_jobs = 0
        conn_failed = 0
        zero_results = 0
        proxy_failed = 0
        existed_jobid = 0
        request_failed = 0

        headers = self.get_headers()
        # proxies = self.get_proxies()
        finished = False
        while not finished:
            try:
                # + -- -- Request a webpage -- -- +
                if int(float(days)) == 1:
                    url = "{}{}?page={}&sortmode=ListedDate".format(
                        URL, SEEK_LINK[industry], page_num
                    )
                else:
                    url = "{}{}?page={}".format(URL, SEEK_LINK[industry], page_num)
                if page_num == 1:
                    allow_redirects = True
                else:
                    allow_redirects = False

                page = requests.get(
                    url, headers=headers, allow_redirects=allow_redirects
                )

                if 300 <= page.status_code < 400:
                    redirects += 1
                    self.log.debug("Being redirected: {}".format(page.status_code))
                    headers = self.get_headers()
                    proxies = self.get_proxies()
                    if redirects > 2:
                        self.reset_proxy_pool()
                    if redirects > 4:
                        finished = True
                        redirects = 0

                elif page.status_code == 200:
                    soup = BeautifulSoup(
                        page.content.decode("utf-8", "ignore"), "html.parser"
                    )

                    # check total jobs found:
                    if total_jobs == 0:
                        total_jobs_found = self.total_jobs_found(soup)
                        if total_jobs_found:
                            total_jobs = total_jobs_found

                    # check if the page has any results
                    # if it repeats 10 times, quit
                    if self.page_zero_result(soup):
                        self.log.info("Page has zero result: {}".format(url))
                        zero_results += 1
                        if zero_results >= 10:
                            finished = True
                    else:
                        zero_results = 0

                        # + -- -- Parse a page's content -- -- +

                        start_info = time.time()
                        job_articles = soup.find_all(
                            "article", attrs={"data-automation": "normalJob"}
                        )
                        for j in job_articles:
                            jobid = j["data-job-id"]

                            select_query_start = time.time()
                            # check if jobid already scraped
                            if self.check_existed_jobid(jobid, "seek"):
                                existed_jobid += 1
                                select_query_end = time.time()
                                self.record["total_time_select"] += (
                                    select_query_end - select_query_start
                                )

                                self.log.info(
                                    "-jobid already scraped: {} - #{}".format(
                                        jobid, existed_jobid
                                    )
                                )
                            else:

                                select_query_end = time.time()
                                self.record["total_time_select"] += (
                                    select_query_end - select_query_start
                                )

                                # + -- -- Extract post info -- -- +

                                info = {
                                    "jobid": jobid,
                                    "scraped_at": datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                }

                                for k in self.key:
                                    tag = j.find(attrs={"data-automation": k})
                                    if tag:

                                        # IMPORTANT
                                        info[k] = tag.text.replace(r"'", r"''")

                                        if k == "jobCompany" and tag.has_attr("href"):
                                            advertiserid = tag["href"][19:]

                                            if advertiserid.isdigit():
                                                info["advertiserid"] = advertiserid
                                            else:
                                                info["advertiserid"] = (
                                                    "<missing advertiserid>"
                                                )

                                        if k == "jobListingDate":
                                            info["posted_at"], posted_today = (
                                                self.get_original_post_time(
                                                    info[k], days
                                                )
                                            )
                                            if not posted_today:
                                                self.log.info(
                                                    "Finished scraping today's job"
                                                )
                                                break

                                # + -- -- Save to database -- -- +
                                if posted_today:
                                    try:
                                        insert_query_start = time.time()
                                        self.to_table_db(json.dumps(info), "seek")

                                        insert_query_end = time.time()
                                        self.record["total_time_insert"] += (
                                            insert_query_end - insert_query_start
                                        )

                                        self.rqueue.put(info["jobid"])
                                        info["saved_to_db"] = True
                                    except Exception as e:
                                        self.log.exception("db error: {}".format(e))
                                        info["saved_to_db"] = False
                                        info["exception"] = repr(e)

                                    jobs.append(info)
                                    self.log.info(
                                        "--- {}. {}: {} \n".format(
                                            i,
                                            info["jobid"],
                                            info["jobTitle"].encode("utf-8"),
                                        )
                                    )
                                    i += 1

                                    # Get total time scraped 1 job info
                                    end_info = time.time()
                                    self.record["total_time_info"] += (
                                        end_info - start_info
                                    )
                                else:
                                    finished = True

                    if existed_jobid >= 60:
                        finished = True
                    else:
                        page_num += 1
                else:
                    finished = True

            except requests.exceptions.SSLError as s:
                time.sleep(0.1)
                self.proxies = self.get_proxies()
                self.log.exception("-SLLError: {} \n".format(s))
                self.record["ssl_errors"] += 1

            except requests.exceptions.ProxyError as p:
                time.sleep(0.1)
                proxy_failed += 1
                self.log.exception("Proxy failed #{}: {}".format(proxy_failed, p))
                if proxy_failed >= 3:
                    proxy_failed = 0
                    self.reset_proxy_pool()
                headers = self.get_headers()
                proxies = self.get_proxies()
                self.record["proxy_errors"] += 1

            except requests.exceptions.ConnectionError as ce:
                conn_failed += 1
                self.log.exception("-Connection Error: {} \n".format(ce))
                time.sleep(0.1)
                if conn_failed >= 3:
                    conn_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["conn_errors"] += 1

            except requests.exceptions.RequestException as e:
                time.sleep(0.1)
                request_failed += 1
                self.log.exception("Request failed: {}".format(e))
                headers = self.get_headers()
                proxies = self.get_proxies()
                if request_failed >= 3:
                    request_failed = 0
                    finished = True
                self.record["request_errors"] += 1

            except KeyboardInterrupt:
                self.log.exception(
                    "Interrupted: wait saving data for {}...then try again".format(
                        industry
                    )
                )
                break

            except Exception as ex:
                print(ex)
                self.log.exception(ex)
                self.record["other_errors"] += 1

            end_cat = time.time()
            self.record["total_time_cat"] = end_cat - start_cat
            self.log.info("\nRecord: {}\n".format(self.record))
            self.rqueue.put(self.record)
