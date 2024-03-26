import multiprocessing
import sys
import time
from datetime import datetime

from base.record import ScraperRecord
from jora_scraper import joracontent, jorainfo
from settings.jorasettings import JORA_CATEGORIES, SERVICE_NAME

RECORD = {
    "total_jobs": 0,
    "last_session_jobs": 0,
    "null_jd": 0,
    "missing_jd": 0,
    "session_start": 0,
    "session_finish": 0,
    "total_cat": len(JORA_CATEGORIES),
    "site": "jora",
}


def run_content_scraper():
    s = joracontent.JoraJobContentScraper()
    s.scraper()


def run_info_scraper(category):
    scraper = jorainfo.JoraJobInfoScraper()
    scraper.run(category)


def process_records():
    """Calculate whatever fields are left in records
    then send results to database
    """
    global RECORD
    s = ScraperRecord("jora_queue")

    # +  -  -  - Query fields that dont need calculating -  -  - +
    final = s.get_total_jobs("jora").strip("(,)")
    RECORD["last_session_jobs"] = str(int(final) - int(RECORD["total_jobs"]))
    RECORD["total_jobs"] = final
    RECORD["null_jd"] = s.get_null_jd("jora").strip("(,)")
    RECORD["missing_jd"] = s.get_missing_jd("jora").strip("(,)")
    RECORD["session_finish"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # +  -  -  - Calculate other fields -  -  - +
    s.get_records(RECORD)
    s.prepare_records()

    # +  -  -  - Send record to db -  -  - +
    s.record_to_db()


def run():

    # if sys.argv[1] == 'info':
    """Create a pool of scraping-info processes"""
    p = multiprocessing.Pool(processes=4)
    p.map(run_info_scraper, JORA_CATEGORIES)
    p.close()
    p.join()

    # Sleep for a while then start content scraper
    time.sleep(3)

    # elif sys.argv[1] == 'content':
    """ Create a pool of workers to scrape """

    slaves = 5
    workers = []
    for i in range(slaves):
        w = multiprocessing.Process(target=run_content_scraper)
        w.start()
        workers.append(w)
        print("Start #", i)

    for w in workers:
        # Wait for all workers to finish
        w.join()


def main():
    """Run scrapers while recording the session time"""

    global RECORD
    # +  -  -  - Get starting time/jobs -  -  - +
    RECORD["session_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s = ScraperRecord("jora_queue", SERVICE_NAME)
    RECORD["total_jobs"] = s.get_total_jobs(SERVICE_NAME).strip("(,)")

    # +  -  -  - Run scraper -  -  - +
    run()

    # +  -  -  - Process records -  -  - +
    process_records()


if __name__ == "__main__":
    main()
