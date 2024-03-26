#
# Scraper dealing with general information of job ad for au.indeed.com
# Inherited from indeedbase
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
from settings import indeedsettings
from utils.RedisQueue import RedisQueue


class IndeedJobInfoScraper(ScraperBase):
    """A handler scrapes general jobs info without full description"""

    def __init__(self):
        super().__init__()
        # Specify redis queue being used
        self.rqueue = RedisQueue("indeed_queue")

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
            indeedsettings.LOG_FILE, maxBytes=1500000, backupCount=0
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
        """Calculate the original post time of a job by Seek

        # Arguments:
            jobListingDate: the date(string) got from job ad
            day_limit: limit how many days ago when job ads were posted

        # Returns:
            original_time: Date and time of job post
            today_job: Boolean value whether date was 1 day ago (true) or longer (false)
        """

        time = jobListingDate.split()

        today_job = True

        # if jobListingDate = "Just posted" or "Today"
        if (time[0] == "Just") or (time[0] == "Today"):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S"), today_job

        if (time[1] == "day") or (time[1] == "days"):
            if time[0] == "30+":
                delta = timedelta(days=30)
                if int(float(day_limit)) == 1:
                    today_job = False
            else:
                delta = timedelta(days=int(time[0]))
                if int(float(time[0])) > int(float(day_limit)):
                    today_job = False

        elif (time[1] == "hours") or (time[1] == "hour"):
            delta = timedelta(hours=int(time[0]))

        elif (time[1] == "minutes") or (time[1] == "minute"):
            delta = timedelta(minutes=int(time[0]))

        elif (time[1] == "seconds") or (time[1] == "second"):
            delta = timedelta(seconds=int(time[0]))

        original_time = (datetime.now() - delta).strftime("%Y-%m-%d %H:%M:%S")

        return original_time, today_job

    def column_results_div(self, url, headers, proxies):
        """Find html tag containing all job articles

        # Arguments:
            url: link being scraped
            headers, proxies: header and proxy initially set
        # Return:
            col_results: div containing all jobs

        """

        redirects = 0
        proxy_failed = 0

        done = False

        while not done:
            try:
                html_page = requests.get(url, headers=headers)
                # proxies=proxies)
                # Check status of web page
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
                    col_results = soup.find("td", id="resultsCol")
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

        return col_results

    def get_subcategory_dict(self, url):
        """Return dict of subcategories within one category of job"""

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
                # Parse page content
                print(">>> URL: ", url)
                html_page = requests.get(url, headers=self.headers)
                # proxies=self.proxies)
                soup = BeautifulSoup(html_page.text, "html.parser")
                table = soup.find("table", id="titles")

                # Store name of subcategory as key,
                # link to subcategory as value
                for element in table.find_all("p", attrs={"class": "job"}):
                    # Get name
                    subcategory_name = element.find("a").attrs["title"]
                    # Get link
                    subcategory_link = element.find("a").get("href")
                    # Assign key, value
                    subcategory_dict[subcategory_name] = subcategory_link
                done = True

            except Exception as e:
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
        """Scrape information of ONE job article

        # Arguments:
            article: single job article extracted from html tag
            day_limit: limitation for how many days ago was the job posted
        # Return:
            info: job's information in dictionary format
            time_limit: whether job was posted within day_limit

        """

        info = {}

        # Get job id
        jobid = article.get("data-jk")
        if not jobid:
            jobid = article.get("data-tk")

        # For checking job listing time is within given limit
        time_limit = True

        select_query_start = time.time()
        if self.check_existed_jobid(jobid, "indeed"):
            self.log.debug("--jobid already scraped: {}  \n".format(jobid))
            select_query_end = time.time()
            self.record["total_time_select"] += select_query_end - select_query_start
            # If already scraped, return empty dictionary
            return {}, time_limit

        else:
            select_query_end = time.time()
            self.record["total_time_select"] += select_query_end - select_query_start
            info = {
                "jobid": jobid,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Find information based on defined attributes
            for key, value in indeedsettings.INDEED_ATTRIBUTES.items():

                # scrape job title
                if value == "jobtitle":
                    result = article.find("a", class_="{}".format(value))
                    if result:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        info[key] = result
                    else:
                        result = article.find("a", class_="jobtitle turnstileLink")
                        if result:
                            result = result.get_text().strip(" \n").replace("'", "''")
                            info[key] = result
                        else:
                            result = article.find("a")

                            if result is None:
                                info[key] = "<missing>"
                            else:
                                result = (
                                    result.get_text().strip(" \n").replace("'", "''")
                                )
                                info[key] = result

                # scrape job company
                elif value == "company":
                    result = article.find(class_="{}".format(value))
                    if result is None:
                        info[key] = "<missing>"
                    else:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        info[key] = result

                # scrape job short description
                elif value == "summary":
                    result = article.find(class_="{}".format(value))
                    if result is None:
                        info[key] = "<missing>"
                    else:
                        result = result.get_text().strip(" \n").replace("'", "''")
                        result = result.strip(",...")
                        info[key] = "{}".format(result)

                # scrape job location
                elif value == "location":
                    result = article.find(class_="{}".format(value))
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

                            # scan STATES dictionary
                            for k, val in indeedsettings.INDEED_STATES.items():
                                if " ".join(new_value) == val:
                                    info["jobState"] = k
                                    info["jobArea"] = "<missing>"
                        elif len(new_value) == 1:
                            for k, val in indeedsettings.INDEED_STATES.items():
                                if new_value[0] == k or new_value[0] == val:
                                    info["jobState"] = k

                # scrape job listing date
                elif value == "date":
                    result = article.find("span", class_="{}".format(value))
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

                # scrape job sponsorship
                elif value == "sponsoredGray":
                    result = article.find("span", class_="{}".format(value))
                    if result:
                        # whether job ad is sponsored or not, and by who
                        info[key] = True
                        result = result.get_text().strip(" \n").split()
                        if len(result) >= 2:
                            info["sponsored_by"] = result[-1].replace("'", "''")
                        else:
                            info["sponsored_by"] = "<missing>"
                    else:
                        info[key] = False
                        info["sponsored_by"] = "<missing>"

                # scrape job salary
                elif value == "no-wrap":
                    result = article.find("span", class_="{}".format(value))
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

                # scrape job num of reviews
                elif value == "slNoUnderline":
                    result = article.find("span", class_="{}".format(value))
                    if result:
                        result = result.get_text().strip(" \n").split()
                        info[key] = result[0]
                    else:
                        info[key] = "<missing>"

        return info, time_limit

    def scrape_all_pages(self, job_url, category, subcategory, day_limit):
        """Loop pages and scrape ALL job articles then store to db

        # Arguments:
            job_url: url of a subcategory
            category: current scraping category
            subcategory: current scraping subcategory
            day_limit: limitation for how many days ago was the job posted

        """
        msg = "---Start scraping for: {} \n".format(subcategory.upper())
        self.log.info(msg)

        # jobs_list = []
        i = 0
        page_num = 0
        proxy_failed = 0
        scraped_page = 0
        conn_failed = 0
        request_failed = 0

        # Check if day_limit is 1, get url tail of latest job posts
        if int(float(day_limit)) == 1:  # convert string to int
            url_tail = "&sort=date&start="
        else:
            url_tail = "&start="

        finished = False
        while not finished:
            try:
                url_page = "{}{}{}{}".format(
                    indeedsettings.INFO_URL, job_url[:-18], url_tail, page_num
                )

                # Get tag containing jobs
                column_results = self.column_results_div(
                    url_page, self.headers, self.proxies
                )

                if column_results == {}:
                    break
                existed_id = 0  # existed jobid in 1 page

                # Loop all job articles
                for article in column_results.select(".row"):

                    start_info = time.time()
                    # Get job information
                    scraped_data, daily_job = self.scrape_job_info(article, day_limit)

                    # Check if it is today's job
                    if not daily_job:
                        finished = True
                        break

                    # Check if got any information
                    if scraped_data:
                        scraped_data["jobClassification"] = category
                        scraped_data["jobSubClassification"] = subcategory

                        # +  -  -  - Put to Redis Queue -  -  - +
                        self.rqueue.put(scraped_data["jobid"])

                        insert_query_start = time.time()
                        # +  -  -  - Save to db -  -  - +
                        self.to_table_db(json.dumps(scraped_data), "indeed")

                        insert_query_end = time.time()
                        self.record["total_time_insert"] += (
                            insert_query_end - insert_query_start
                        )

                        # Get total time scraped 1 job info
                        end_info = time.time()
                        self.record["total_time_info"] += end_info - start_info

                        self.log.info(
                            "--- {}. {}: {} \n".format(
                                i,
                                scraped_data["jobid"],
                                scraped_data["jobTitle"].encode("utf-8"),
                            )
                        )
                        i += 1
                    else:
                        # If 'scrape info' function returns {}, id already scraped
                        existed_id += 1

                    # if >4 jobid already scraped, skip page
                    if existed_id > 4:
                        scraped_page += 1
                        break

                # if 2 page skipped, skip subcategory
                if scraped_page == 2:
                    finished = True
                    self.log.info(
                        "-Stopping at page {} for {} \n".format(page_num, subcategory)
                    )
                page_num += 10

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

            except requests.exceptions.RequestException as re:
                request_failed += 1
                self.log.exception(
                    "-Request failed #{}: {} \n".format(request_failed, re)
                )
                time.sleep(0.1)
                msg = "-TOPIC: jobinfo - {} \n".format(re)
                if request_failed >= 3:
                    request_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["request_errors"] += 1

            except KeyboardInterrupt:
                self.log.debug(
                    "-Interrupted: wait saving data for {}...then try again \n".format(
                        subcategory
                    )
                )
                break

            except Exception as ex:
                print(ex)
                message = "job subcat {} : unknown exception {}".format(subcategory, ex)
                self.log.exception(message)
                self.record["other_errors"] += 1

    # sys.argv[1]
    def run(self, category, days=1):
        """Run scraper for a particular job category

        # Arguments:
            category: name of scraping category
            days: user argument specifying how many days ago should jobs be posted
        """

        start_cat = time.time()

        message = "-Category: {} \n".format(category)
        self.log.info(message)
        # print(message)

        # Process category name for formatting url
        if len(category.split()) == 2:

            # If category name has 2 words, split for ease of use
            cat_name = category.split()
            category_url = "{}/browsejobs/{}-{}".format(
                indeedsettings.INFO_URL, cat_name[0], cat_name[1]
            )

        else:
            category_url = "{}/browsejobs/{}".format(indeedsettings.INFO_URL, category)

        # Get subcategories
        subcategory_dict = self.get_subcategory_dict(category_url)
        self.record["total_subcat"] = len(subcategory_dict)

        # Scrape subcategory
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
