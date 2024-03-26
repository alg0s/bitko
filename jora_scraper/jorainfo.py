#
# Scraper dealing with general information of job ad for au.jora.com
# Inherited from jorabase
#
# =====================================================================


import json
import logging
import logging.handlers as handlers
import smtplib
import sys
import time
from datetime import datetime, timedelta

import psycopg2
import requests
from bs4 import BeautifulSoup

from base.base import ScraperBase
from settings.jorasettings import JORA_ATTRIBUTES, JORA_STATES, URL
from utils.RedisQueue import RedisQueue


class JoraJobInfoScraper(ScraperBase):
    """A handler scrapes general jobs info without full description"""

    def __init__(self):
        super().__init__()
        # Specify redis queue being used
        self.rqueue = RedisQueue("jora_queue")

        # Get proxy/header
        self.headers = self.get_headers()
        self.proxies = self.get_proxies()

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
            "./jora_scraper/logs/info_scrape_log.log", maxBytes=1500000, backupCount=0
        )

        formatter = logging.Formatter("%(levelname)s:%(message)s")
        logHandler.setFormatter(formatter)
        logHandler.setLevel(logging.DEBUG)

        if not self.log.handlers:
            self.log.addHandler(logHandler)

    def get_records(self):
        """Return dictionary with key = areas being recorded"""
        return self.record

    def get_original_post_time(self, jobListingDate, day_limit):
        """Calculate the original post time of a job"""

        time = jobListingDate.split()

        today_job = True

        # print(time)
        if time[0] == "about":
            # eg: about 4 hours ago
            if time[2] == "hour" or time[2] == "hours":
                delta = timedelta(hours=int(time[1]))
            # eg: about 1 month ago
            elif time[2] == "month":
                delta = timedelta(days=30)
                # today_job = False
            # eg: about 2 months ago
            elif time[2] == "months":
                delta = timedelta(days=int(time[1]) * 30)
                # today_job = False

        elif (time[1] == "day") or (time[1] == "days"):
            delta = timedelta(days=int(time[0]))
            if int(time[0]) > int(float(day_limit)):
                today_job = False

        elif (time[1] == "minutes") or (time[1] == "minute"):
            delta = timedelta(minutes=int(time[0]))

        elif (time[1] == "seconds") or (time[1] == "second"):
            delta = timedelta(seconds=int(time[0]))

        elif time[0] == "less":
            delta = timedelta(minutes=int(1))

        try:
            self.log.info("-- Time scraped raw: {} \n".format(jobListingDate))
            original_time = (datetime.now() - delta).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            # self.notify_exception(
            #     "-time exception at {}: {} \n".format(time, e))
            self.log.debug("-time exception at {}: {} \n".format(time, e))
            original_time = "<missing>"
        return original_time, today_job

    def get_subcategory_dict(self, url):
        """Return dictionary of subcategories within one category of job"""

        proxy_failed = 0
        loop_count = 0

        subcategory_dict = {}
        done = False
        while not done:
            try:
                loop_count += 1
                if loop_count >= 3:
                    time.sleep(0.1)
                    loop_count = 0
                # parse page content
                html_page = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(html_page.text, "html.parser")
                table = soup.find("div", class_="browse keyword")
                # store name of subcategory as key,
                # link to subcategory as value
                for element in table.find_all("li"):
                    # get name
                    subcategory_name = element.find("a").get_text()
                    # get link
                    subcategory_link = (
                        element.find("a")
                        .get("href")
                        .replace("-", "+")
                        .replace("+jobs?", "&")
                    )
                    # assign key, value
                    subcategory_dict[subcategory_name] = subcategory_link
                done = True
            except Exception as e:
                time.sleep(0.1)
                self.log.exception(
                    "-Exception when get subcategory dict: {} \n".format(e)
                )
                proxy_failed += 1
                if proxy_failed >= 3:
                    proxy_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                done = False
                self.record["other_errors"] += 1

        return subcategory_dict

    def scrape_job_info(self, article, day_limit):
        """Scrape information of ONE job article"""

        info = {}
        # get job id
        jobid = article.attrs["id"][2:]
        # to check job listing time is within given limit
        time_limit = True

        select_query_start = time.time()
        if self.check_existed_jobid(jobid, "jora"):
            self.log.debug("--jobid already scraped: {}  \n".format(jobid))
            select_query_end = time.time()
            self.record["total_time_select"] += select_query_end - select_query_start
            return {}, time_limit
        else:
            select_query_end = time.time()
            self.record["total_time_select"] += select_query_end - select_query_start
            info = {
                "jobid": jobid,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            # find information based on defined attributes
            for key, value in JORA_ATTRIBUTES.items():

                # scrape job title
                if value == "jobtitle":
                    result = article.a
                    if result:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        info[key] = result
                    else:
                        info[key] = "<missing>"

                # scrape job company
                elif value == "company":
                    result = article.find("span", class_="company")
                    if result:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        info[key] = result
                    else:
                        info[key] = "<missing>"

                # scrape job location
                elif value == "location":
                    result = article.find("span", class_="location")
                    info["jobState"] = "<missing>"
                    info["jobArea"] = "<missing>"
                    if result is None:
                        info[key] = "<missing>"
                    else:
                        result = result.get_text().strip(" \n")
                        info[key] = result.replace("'", "''")

                        new_value = result.split()

                        # separate location into state and area
                        if len(new_value) >= 2:
                            info["jobState"] = new_value[-1]
                            info["jobArea"] = " ".join(new_value[:-1]).replace(
                                "'", "''"
                            )

                            # check if location is the same as state's full name
                            for k, val in JORA_STATES.items():
                                if " ".join(new_value) == val:
                                    info["jobState"] = k
                                    info["jobArea"] = "<missing>"
                        elif len(new_value) == 1:
                            for k, val in JORA_STATES.items():
                                if new_value[0] == k or new_value[0] == val:
                                    info["jobState"] = k

                # scrape job salary
                elif value == "salary":
                    result = article.find("div", class_="salary")
                    if result:
                        result = result.get_text().strip(" \n").split()

                        # re-format salary unit, eg: a year -> yearly
                        period = ["hour", "day", "week", "month", "year"]
                        periodically = [
                            "hourly",
                            "daily",
                            "weekly",
                            "monthly",
                            "yearly",
                        ]
                        for idx in range(len(period)):
                            if len(result) == 3:
                                # eg: $24 an hour
                                if result[2] == period[idx]:
                                    info[key] = "{} {}".format(
                                        result[0], periodically[idx]
                                    )
                            if len(result) == 5:
                                # eg: $40,000 - $50,000 a year
                                if result[4] == period[idx]:
                                    info[key] = "{}-{} {}".format(
                                        result[0], result[2], periodically[idx]
                                    )
                    else:
                        info[key] = "<missing>"

                # scrape job listing date
                elif value == "date":
                    result = article.find("span", class_="date")
                    if result:
                        result = result.get_text().strip(" \n")
                        post_time, time_limit = self.get_original_post_time(
                            result, day_limit
                        )

                        if time_limit == False:
                            # if job listed is older than specified day_limit,
                            # stop scraping
                            self.log.info("-Finishing scraping today's jobs \n")
                            return {}, time_limit
                        info[key] = post_time
                    else:
                        info[key] = "<missing>"

                # scrape job short description
                elif value == "summary":
                    result = article.find("div", class_="summary")
                    if result is None:
                        info[key] = "<missing>"
                    else:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        result = result.strip(",...")
                        info[key] = "{}".format(result)
        return info, time_limit

    def get_job_div(self, url, headers, proxies):
        """Find div containing all job articles"""

        redirects = 0
        proxy_failed = 0

        done = False
        while not done:
            try:
                html_page = requests.get(url, headers=headers)
                if 300 <= html_page.status_code < 400:
                    redirects += 1
                    self.log.debug(
                        "-Being redirected: {} \n".format(html_page.status_code)
                    )

                    headers = self.get_headers()
                    proxies = self.get_proxies()

                    if redirects > 2:
                        self.reset_proxy_pool()
                    if redirects > 4:
                        done = True
                        redirects = 0

                elif html_page.status_code == 200:
                    soup = BeautifulSoup(html_page.text, "html.parser")
                    job_results = soup.find("ul", id="jobresults")
                    done = True

                else:
                    job_results = None
                    done = True

            except requests.exceptions.ProxyError as p:
                time.sleep(0.1)
                proxy_failed += 1
                self.log.exception(
                    "-Proxy failed for colresults #{}: {} \n".format(proxy_failed, p)
                )

                if proxy_failed >= 3:
                    self.reset_proxy_pool()
                    proxy_failed = 0
                done = False
                self.record["proxy_errors"] += 1

        return job_results

    def scrape_all_pages(self, job_url, category, subcategory, day_limit):
        """Loop pages and scrape ALL job articles then store to db"""

        msg = "---Start scraping for: {} \n".format(subcategory.upper())
        self.log.info(msg)
        # print(msg)

        i = 0
        page_num = 1
        proxy_failed = 0
        scraped_page = 0
        jobs_scraped = 0
        conn_failed = 0
        request_failed = 0

        # check if day_limit is 1, get url tail of latest job posts
        if int(float(day_limit)) == 1:  # convert string to int
            url_tail = "&st=date"
        else:
            url_tail = ""

        finished = False
        while not finished:
            try:
                existed_id = 0  # restart existed jobid in 1 page
                url_page = "{}/j?l=&p={}&q={}{}".format(
                    URL, page_num, job_url, url_tail
                )

                # get div containing jobs
                job_results = self.get_job_div(url_page, self.headers, self.proxies)

                # loop all job articles
                try:
                    article_list = job_results.find_all("li", class_="result")
                except:
                    break
                if article_list:
                    for article in article_list:

                        start_info = time.time()

                        scraped_data, daily_job = self.scrape_job_info(
                            article, day_limit
                        )

                        if scraped_data:
                            scraped_data["jobClassification"] = category
                            scraped_data["jobSubClassification"] = subcategory
                            jobs_scraped += 1

                            # +  -  -  - Put to Redis Queue -  -  - +
                            self.rqueue.put(scraped_data["jobid"])

                            insert_query_start = time.time()
                            # +  -  -  - Save to db -  -  - +
                            self.to_table_db(json.dumps(scraped_data), "jora")

                            insert_query_end = time.time()
                            self.record["total_time_insert"] += (
                                insert_query_end - insert_query_start
                            )

                            # Get total time scraped 1 job info
                            end_info = time.time()
                            self.record["total_time_info"] += end_info - start_info

                            self.log.info(
                                "--- {}. {}: {} \n".format(
                                    i, scraped_data["jobid"], scraped_data["jobTitle"]
                                )
                            )
                            i += 1
                        else:
                            existed_id += 1

                        if not daily_job:
                            finished = True
                            break

                        # if >3 jobid already scraped, skip page
                        if existed_id > 3:
                            scraped_page += 1
                            break
                else:
                    finished = True

                # if 2 page skipped => have reached last page, skip subcategory
                if scraped_page == 2:
                    finished = True
                    self.log.info(
                        "-Stopping at page {} for {} \n".format(page_num, subcategory)
                    )
                page_num += 1

            except requests.exceptions.SSLError as s:
                time.sleep(0.1)
                self.proxies = self.get_proxies()
                self.log.exception("-SLLError: {} \n".format(s))
                self.record["ssl_errors"] += 1

            except requests.exceptions.ProxyError as p:
                time.sleep(0.1)
                self.log.exception("-Proxy failed #{}: {} \n".format(proxy_failed, p))
                proxy_failed += 1
                if proxy_failed >= 3:
                    proxy_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["proxy_errors"] += 1

            except requests.exceptions.ConnectionError as ce:
                time.sleep(0.1)
                conn_failed += 1
                self.log.exception("-Connection Error: {} \n".format(ce))
                if conn_failed >= 3:
                    conn_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["conn_errors"] += 1

            except requests.exceptions.RequestException as re:
                time.sleep(0.1)
                request_failed += 1
                self.log.exception(
                    "-Request failed #{}: {} \n".format(request_failed, re)
                )
                if request_failed >= 3:
                    request_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["request_errors"] += 1

            except KeyboardInterrupt:
                self.log.exception(
                    "-Interrupted: wait saving data for {}...then try again \n".format(
                        subcategory
                    )
                )
                break

            except Exception as ex:
                print(ex)
                self.log.exception("-Unknown exception: {} \n".format(ex))
                self.record["other_errors"] += 1

    # sys.argv[1]
    def run(self, category, days=1):
        """Run scraper for a particular job category"""

        start_cat = time.time()

        message = "-Category: {} \n".format(category)
        self.log.info(message)
        print(message)

        # process category name for formatting url
        if len(category.split()) == 2:
            # if category name has 2 words, split for ease of use
            cat_name = category.split()
            category_url = "{}/findjobs/{}-{}".format(
                URL, cat_name[0].lower(), cat_name[1].lower()
            )
        else:
            category_url = "{}/findjobs/{}".format(URL, category.lower())

        # get subcategories
        subcategory_dict = self.get_subcategory_dict(category_url)
        self.record["total_subcat"] = len(subcategory_dict)

        # scrape subcategory
        for subcategory, url in subcategory_dict.items():
            start_subcat = time.time()

            self.scrape_all_pages(url, category, subcategory, days)

            end_subcat = time.time()
            self.record["total_time_subcat"] += end_subcat - start_subcat

        self.log.info("Finished scraping info at {} \n".format(self.NOW))
        end_cat = time.time()
        self.record["total_time_cat"] = end_cat - start_cat
        self.log.info("\nRecord: {}\n".format(self.record))
        self.rqueue.put(self.record)
