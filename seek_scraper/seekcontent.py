# -*- coding: utf-8 -*-


#
# Scraper dealing with full description of job ad for seek.com.au
# Inherited from seekbase
#
# =====================================================================


import logging
import logging.handlers as handlers
import time

import requests
from bs4 import BeautifulSoup

from base.base import ScraperBase
from utils.RedisQueue import RedisQueue


class SeekJobContentScraper(ScraperBase):
    """A scraper to look for full description of jobs"""

    def __init__(self):
        super().__init__()
        # Specify redis queue being used
        self.rqueue = RedisQueue("seek_queue")

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
            "./seek_scraper/logs/content_scrape_log.log",
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

    def is_job_expired(self, soup):
        content = soup.find("div", attrs={"data-automation": "expiredJobPage"})
        if content:
            return True
        return False

    def scrape_job_content(self, jobid):

        start = time.time()
        existed_id = 0
        proxy_failed = 0
        conn_failed = 0
        request_failed = 0
        redirect = 0

        if self.check_existed_jd(jobid, "seek"):
            existed_id += 1
            self.log.info(
                "---jobid already scraped: {} - #{} \n".format(jobid, existed_id)
            )
            return

        done = False
        while not done:
            try:
                url = "".join(["https://www.seek.com.au/job/", jobid])
                page = requests.get(url, headers=self.headers)
                done = False
                status = page.status_code

                jd_start = time.time()

                if 300 <= status < 400:
                    self.reset_proxy_pool()
                    self.proxies = self.get_proxies()
                    self.headers = self.get_headers()
                    redirect += 1
                    if redirect > 3:
                        break
                elif status == 404:
                    self.log.debug("-404 error for: {} \n".format(jobid))
                    jd = "<missing>"
                    self.jd_to_db(jobid, jd, "seek")
                    jd_end = time.time()
                    self.record["total_time_jd"] += jd_end - jd_start
                    print("saved <missing>: {}".format(jobid))
                    done = True
                elif status == 410:
                    self.log.info("jobid expired: {} \n".format(jobid))
                    jd = "<missing>"
                    self.jd_to_db(jobid, jd, "seek")
                    jd_end = time.time()
                    self.record["total_time_jd"] += jd_end - jd_start
                    print("saved 410 <missing>: {}".format(jobid))
                    done = True
                elif status == 200:
                    soup = BeautifulSoup(
                        page.content.decode("utf-8", "ignore"), "html.parser"
                    )
                    if not self.is_job_expired(soup):
                        content = soup.find("div", class_="templatetext")
                        if not content:
                            content = soup.find("div", class_="_2e4Pi2B")
                        if content:
                            # s = content.get_text().replace(r"'", r"''")
                            # jd = re.sub(r'\n\s*\n', r'\n\n',
                            #             s.strip(), flags=re.M)

                            jd = str(content).replace(r"'", r"''")
                            self.jd_to_db(jobid, jd, "seek")
                            jd_end = time.time()
                            self.record["total_time_jd"] += jd_end - jd_start

                            print("saved: {}".format(jobid))
                            self.log.info("saved: {}".format(jobid))

                    done = True
                else:
                    done = True
                self.log.info(
                    "Total time scraping 1 job = {}s".format(time.time() - start)
                )

            except requests.exceptions.SSLError as s:
                time.sleep(0.1)
                self.proxies = self.get_proxies()
                self.log.exception("-SLLError: {} \n".format(s))
                self.record["ssl_errors"]

            except requests.exceptions.ProxyError as p:
                time.sleep(0.1)
                proxy_failed += 1
                self.log.exception("-Proxy failed: {} \n".format(p))
                self.proxies = self.get_proxies()
                if proxy_failed >= 3:
                    proxy_failed = 0
                    self.reset_proxy_pool()
                    self.log.debug("-Re-downloading proxies \n")
                self.headers = self.get_headers()
                self.proxies = self.get_proxies()
                self.record["proxy_errors"]

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
                self.log.exception(
                    "-Keyboard Interrupted: wait putting jobid to exception table"
                )
                break

            except Exception as ex:
                self.log.exception("-Unknown Exceptions: {} \n".format(ex))
                self.record["other_errors"]

    def scraper(self):

        num_record = 0
        empty = 0
        while True:
            jobid = self.rqueue.pop(timeout=10)

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
