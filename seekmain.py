import multiprocessing
from datetime import datetime

from base.record import ScraperRecord
from seek_scraper import seekcontent, seekinfo
from settings.seeksettings import SEEK_CATEGORIES, SERVICE_NAME

RECORD = {
    "total_jobs": 0,
    "last_session_jobs": 0,
    "null_jd": 0,
    "missing_jd": 0,
    "session_start": 0,
    "session_finish": 0,
    "total_cat": len(SEEK_CATEGORIES),
    "site": "seek",
}


def run_content_scraper():
    s = seekcontent.SeekJobContentScraper()
    s.scraper()


def run_info_scraper(category):
    scraper = seekinfo.SeekJobInfoScraper()
    scraper.job_by_industry(category)


def process_records():
    """Calculate whatever fields are left in records
    then send results to database
    """
    global RECORD
    s = ScraperRecord("seek_queue")

    # +  -  -  - Query fields that dont need calculating -  -  - +
    final = s.get_total_jobs("seek").strip("(,)")
    RECORD["last_session_jobs"] = str(int(final) - int(RECORD["total_jobs"]))
    RECORD["total_jobs"] = final
    RECORD["null_jd"] = s.get_null_jd("seek").strip("(,)")
    RECORD["missing_jd"] = s.get_missing_jd("seek").strip("(,)")
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
    p.map(run_info_scraper, SEEK_CATEGORIES)
    p.close()
    p.join()

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
    s = ScraperRecord("seek_queue", SERVICE_NAME)
    RECORD["total_jobs"] = s.get_total_jobs(SERVICE_NAME).strip("(,)")

    # +  -  -  - Run scraper -  -  - +
    run()

    # +  -  -  - Process records -  -  - +
    process_records()


if __name__ == "__main__":
    main()
