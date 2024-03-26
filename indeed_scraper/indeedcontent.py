# Scraper dealing with full description of job ad for au.indeed.com
# Inherited from indeedbase

import logging
import logging.handlers as handlers
import re
import time

import requests
from bs4 import BeautifulSoup

from base.base import ScraperBase
from settings import indeedsettings
from utils.RedisQueue import RedisQueue


class IndeedJobContentScraper(ScraperBase):
    """A scraper to look for full description of jobs"""

    def __init__(self):
        super().__init__()
        # Specify redis queue being used
        self.rqueue = RedisQueue("indeed_queue")

        # Get proxy/header
        self.proxies = self.get_proxies()
        self.headers = self.get_headers()

        # Total time for 1 jd
        self.record = {
            "total_time_jd": 0,
            "ssl_errors": 0,
            "proxy_errors": 0,
            "conn_errors": 0,
            "request_errors": 0,
            "other_errors": 0,
        }

        # Setting up logger
        # REFERENCE:
        # https://tutorialedge.net/python/python-logging-best-practices/
        self.log = logging.getLogger(__name__)

        self.log.setLevel(logging.DEBUG)

        logHandler = handlers.RotatingFileHandler(
            indeedsettings.LOG_FILE,
            maxBytes=1500000,
            backupCount=0,
        )

        formatter = logging.Formatter("%(levelname)s:%(message)s")
        logHandler.setFormatter(formatter)
        logHandler.setLevel(logging.DEBUG)

        if not self.log.handlers:
            self.log.addHandler(logHandler)

    def get_records(self):
        """Return dictionary with key = areas being recorded"""
        return self.record

    def scrape_job_content(self, jobid):
        """Scrape content of jobid"""

        start = time.time()
        existed_id = 0
        loop_count = 0
        proxy_failed = 0
        conn_failed = 0
        request_failed = 0
        redirect = 0

        # First, check if jobid is already scraped or not
        if self.check_existed_jd(jobid, "indeed"):
            existed_id += 1
            self.log.info(
                "---jobid already scraped: {} - #{} \n".format(jobid, existed_id)
            )
            return

        finished = False
        while not finished:
            try:
                jd_start = time.time()

                url = "{}{}".format(indeedsettings.CONTENT_URL, jobid)
                html_page = requests.get(url, headers=self.headers)
                finished = False
                status = html_page.status_code

                # Check status of web page
                if 303 <= status < 400:
                    self.reset_proxy_pool()
                    self.proxies = self.get_proxies()
                    self.headers = self.get_headers()
                    redirect += 1
                    if redirect > 3:
                        break
                elif status == 404:
                    self.log.debug("-404 error for: {} \n".format(jobid))
                    content = "<missing>"
                    self.jd_to_db(jobid, content, "indeed")
                    jd_end = time.time()
                    self.record["total_time_jd"] += jd_end - jd_start
                    finished = True
                elif status == 410 or status == 302:
                    self.log.debug("jd expired for {} \n".format(jobid))
                    content = "<missing>"
                    self.jd_to_db(jobid, content, "indeed")
                    jd_end = time.time()
                    self.record["total_time_jd"] += jd_end - jd_start
                    finished = True
                elif status == 200:
                    page_content = html_page.text
                    html_page.close()
                    soup = BeautifulSoup(
                        re.sub("<!--|-->", "", page_content), "html5lib"
                    )

                    # Find the block containing job description
                    jd = soup.find(
                        "div",
                        class_="jobsearch-JobComponent-description icl-u-xs-mt--md",
                    )
                    if not jd:
                        # if not found, try other tags
                        jd = soup.find("span", class_="summary")
                        if not jd:
                            jd = soup.find("span", id="job_summary")
                            if not jd:
                                jd = soup.find("div", class_="container")
                                if jd:
                                    content = "<missing>"
                                    self.jd_to_db(jobid, content, "indeed")
                                    break
                                else:
                                    loop_count += 1
                                    # If loop 3 times and still got nothing, break
                                    if loop_count == 3:
                                        self.log.debug(
                                            "-Cant to get jd jobid: {} \n".format(jobid)
                                        )
                                        finished = True
                                        break
                                    continue

                    if jd:
                        # Get the content
                        # content = jd.get_text(separator="\n\n",
                        #                       strip=True).replace("'", "''")
                        content = str(jd).replace("'", "''")
                        print(content)
                        self.jd_to_db(jobid, content, "indeed")
                        jd_end = time.time()
                        self.record["total_time_jd"] += jd_end - jd_start
                        finished = True
                        # print("-saved: {}".format(jobid))
                        self.log.info("-saved: {}".format(jobid))
                else:
                    finished = True

            except requests.exceptions.SSLError as s:
                time.sleep(0.1)
                self.proxies = self.get_proxies()
                self.log.exception("-SSL Error: {}".format(s))
                self.record["ssl_errors"]

            except requests.exceptions.ProxyError as p:
                proxy_failed += 1
                time.sleep(0.1)
                self.log.exception("-Proxy Error: {}".format(p))
                if proxy_failed >= 3:
                    proxy_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["proxy_errors"]

            except requests.exceptions.ConnectionError as ce:
                conn_failed += 1
                time.sleep(0.1)
                self.log.exception("-Connection Error: {} \n".format(ce))
                if conn_failed >= 3:
                    conn_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["conn_errors"]

            except requests.exceptions.RequestException as e:
                request_failed += 1
                time.sleep(0.1)
                self.log.exception("-Request failed: {} \n".format(e))
                if request_failed >= 3:
                    request_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["request_errors"]

            except KeyboardInterrupt:
                self.log.debug("-Keyboard Interrupted")
                break

            except Exception as ex:
                self.log.exception("-Unknown Exceptions: {} \n".format(ex))
                self.record["other_errors"]

        self.log.info(
            "Total time scraping jobid {} = {}".format(jobid, time.time() - start)
        )

    def scraper(self):
        """Run scraper"""

        num_record = 0
        empty = 0
        while True:
            # Pop jobid from Redis Queue
            jobid = self.rqueue.pop(timeout=10)

            # print("queue size: {}".format(self.rqueue.size()))

            if jobid:
                # If the 'jobid' length >=50 meaning a json record
                if len(jobid) >= 50:
                    num_record += 1
                    print(num_record)
                    if num_record >= 90:
                        self.rqueue.put(jobid)
                        # Put record into queue for latter use
                        self.rqueue.put(self.record)
                        break
                    else:
                        # Put back to queue for latter use
                        self.rqueue.put(jobid)
                else:
                    self.scrape_job_content(jobid.decode("utf-8"))
            else:
                empty += 1
                if empty >= 3:
                    break
