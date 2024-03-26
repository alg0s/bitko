#
# Scraper dealing with full description of job ad on jora
# which were unsuccessfully scraped from the first time
#
# Inherited from joracontentscraper
#
# =====================================================================

import logging
import multiprocessing
import time
from datetime import datetime

from joracontent import JoraJobContentScraper


class JoraJobMissingContent(JoraJobContentScraper):
    """Scraper handling jobids whose content have not been scraped"""

    def __init__(self):
        super().__init__()


def get_jobids():
    """Get list of jobids missing jd"""

    s = JoraJobMissingContent()
    jobids = s.jobs_missing_jd("jora")

    print(len(jobids))
    return jobids


def main(jobid):
    """Init scraper and run"""

    content_scraper = JoraJobMissingContent()
    content_scraper.scrape_job_content(jobid)


if __name__ == "__main__":

    p = multiprocessing.Pool(processes=10)
    p.map(main, get_jobids())
    p.close()
    p.join()
