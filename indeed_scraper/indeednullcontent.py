#
# Scraper dealing with full description of job ad on indeed
# which were unsuccessfully scraped from the first time
#
# Inherited from indeedcontentscraper
#
# =====================================================================


import logging
import multiprocessing
from datetime import datetime

from indeedcontent import IndeedJobContentScraper


class IndeedJobMissingContent(IndeedJobContentScraper):
    """Scraper handling jobids whose content have not been scraped"""

    def __init__(self):
        super().__init__()


def get_jobids():
    """Get list of jobids with null jd"""

    s = IndeedJobMissingContent()
    jobids = s.jobs_missing_jd("indeed")

    print(len(jobids))
    return jobids


def main(jobid):
    """Init scraper and start scraping content"""

    content_scraper = IndeedJobMissingContent()
    content_scraper.scrape_job_content(jobid)


if __name__ == "__main__":
    p = multiprocessing.Pool(processes=10)
    p.map(main, get_jobids())
    p.close()
    p.join()
