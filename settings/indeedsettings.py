INFO_URL = "https://au.indeed.com"
CONTENT_URL = "https://au.indeed.com/viewjob?jk="
SERVICE_NAME = "indeed"
LOG_FILE = "./logs/info_scrape_log.log"

INDEED_ATTRIBUTES = {
    "jobTitle": "jobtitle",
    "jobCompany": "company",
    "jobLocation": "location",
    "jobShortDescription": "summary",
    "jobListingDate": "date",
    "sponsored": "sponsoredGray",
    "salary": "no-wrap",
    "numReviews": "slNoUnderline",
}

INDEED_STATES = {
    "NSW": "New South Wales",
    "QLD": "Queensland",
    "TAS": "Tasmania",
    "WA": "Western Australia",
    "NT": "Northern Territory",
    "SA": "South Australia",
    "VIC": "Victoria",
}


INDEED_CATEGORY = [
    "Other",
    "Accounting",
    "Finance",
    "Administrative",
    "Food Service",
    "Pharmaceutical",
    "Agriculture",
    "Healthcare",
    "Publishing",
    "Architecture",
    "Hospitality",
    "Real Estate",
    "Arts",
    "Human Resources",
    "Restaurant",
    "Banking",
    "IT",
    "Retail",
    "Business",
    "Law Enforcement",
    "Sales",
    "Computer",
    "Legal",
    "Scientific",
    "Construction",
    "Logistics",
    "Security",
    "Consulting",
    "Maintenance",
    "Social Care",
    "CustomerService",
    "Management",
    "Sport",
    "Education",
    "Manufacturing",
    "Training",
    "Energy",
    "Marketing",
    "Transportation",
    "Engineering",
    "Mechanical",
    "Travel",
    "Facilities",
    "Mining",
    "Volunteering",
]
