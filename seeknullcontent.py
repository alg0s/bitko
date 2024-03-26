#
# Scraper dealing with full description of job ad on seek
# which were unsuccessfully scraped from the first time
#
# Inherited from seekcontentscraper
#
# =====================================================================

import logging
import multiprocessing
import time
from datetime import datetime

from seek_scraper.seekcontent import SeekJobContentScraper


class SeekJobMissingContent(SeekJobContentScraper):
    """Scraper handling jobids whose content have not been scraped"""

    def __init__(self):
        super().__init__()


def get_jobids():
    """Get list of jobids whose jd is null"""

    s = SeekJobMissingContent()
    jobids = s.jobs_missing_jd("seek")

    print(len(jobids))
    return jobids


def main(jobid):
    """Init scraper and run"""

    content_scraper = SeekJobMissingContent()
    content_scraper.scrape_job_content(jobid)


if __name__ == "__main__":
    p = multiprocessing.Pool(processes=10)
    p.map(main, get_jobids())
    p.close()
    p.join()
