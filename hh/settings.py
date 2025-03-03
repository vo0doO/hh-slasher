import os

BOT_NAME = "hh"

HH_API_CLIENT_ID = os.environ["HH_API_CLIENT_ID"]
HH_API_CLIENT_SECRET = os.environ["HH_API_CLIENT_SECRET"]
HH_API_USER_AGENT = os.environ["HH_API_USER_AGENT"]

SPIDER_MODULES = ["hh.spiders"]
NEWSPIDER_MODULE = "hh.spiders"

CONCURRENT_REQUESTS = 3

COOKIES_ENABLED = False
COOKIES_DEBUG = False

DEFAULT_REQUEST_HEADERS = {
    "HH-User-Agent": HH_API_USER_AGENT,
    "Accept": "application/json",
}

DOWNLOAD_DELAY = 0.30
DOWNLOAD_TIMEOUT = 30

DOWNLOADER_MIDDLEWARES = {
    "hh.middlewares.TokenMiddleware": 543,
}

DUPEFILTER_DEBUG = True

FEED_EXPORTERS = {
    "csv": "hh.ext.TabbedCsvItemExporter",
}

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 60 * 60 * 8
HTTPCACHE_DIR = os.environ.get("HTTPCACHE_DIR", "httpcache")
HTTPCACHE_ALLOW_HTTP_CODES = (200,)
HTTPCACHE_POLICY = "hh.ext.WhitelistCodesCachePolicy"
HTTPCACHE_STORAGE = "hh.ext.OneMoreSubDirFilesystemCacheStorage"

LOG_FILE_APPEND = bool(os.environ.get("SCRAPYD"))

TELEGRAM_GROUP_ID = os.environ["TELEGRAM_GROUP_ID"]
TELEGRAM_THEME_ID = os.environ["TELEGRAM_THEME_ID"]
TELEGRAM_RECIPIENT = f"{TELEGRAM_GROUP_ID}"

REFERER_ENABLED = False
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html#std-setting-REFERRER_POLICY
REFERRER_POLICY = "no-referrer"

RETRY_HTTP_CODES = (403, 502, 400, 504)
RETRY_PRIORITY_ADJUST = -1000
RETRY_TIMES = 20

ROBOTSTXT_OBEY = False

TELNETCONSOLE_ENABLED = not bool(os.environ.get("SCRAPYD"))
TELNETCONSOLE_USERNAME = "scrapy"
TELNETCONSOLE_PASSWORD = os.environ.get("SCRAPY_TELNETCONSOLE_PASSWORD", "scrapy")

USER_AGENT = HH_API_USER_AGENT

FEED_EXPORT_FIELDS = [
    # "scraping_at",
    "published_at",
    "search_by",
    "vacancy_id",
    "vacancy_url",
    "vacancy_name",
    "vacancy_region",
    "vacancy_address",
    "vacancy_sallary",
    "vacancy_skills",
    "vacancy_description",
    # "contacts_name",
    # "contacts_email",
    # "contacts_phones",
    # "company_name",
    # "company_region",
    # "company_hh_url",
    # "company_site",
    # "company_industries",
]

FEED_EXPORT_FILE_PATH = "result/hh.csv"
FEEDS = {
    FEED_EXPORT_FILE_PATH: {
        "format": "csv",
        "overwrite": True,
    }
}

HH_API_TOKEN_FILE_PATH = "data/hh-api-token.json"
IDATICA_GOOGLE_SERVICE_ACCOUNT_CREDENTAILS_FILE_PATH = (
    "data/idatica-google-service-account-credentails.json"
)

GOOGLE_SPREADSHEET_KEY = ""
GOOGLE_WORKSHEET_NAME = "Data"
GOOGLE_WORKSHEET_META = {
    "head": 2,
    "update_date": {"rowIndex": 0, "columnIndex": 1, "cell": "B1"},
    "all_rows_count": {"rowIndex": None, "columnIndex": None, "cell": "D1"},
    "new_rows_count": {"rowIndex": None, "columnIndex": None, "cell": "F1"},
}

EXTENSIONS = {
    "spidermon.contrib.scrapy.extensions.Spidermon": 500,
}

SPIDERMON_ENABLED = True
SPIDERMON_TELEGRAM_SENDER_TOKEN = os.environ["TELEGRAM_SENDER_TOKEN"]
SPIDERMON_TELEGRAM_RECIPIENTS = [TELEGRAM_RECIPIENT]
SPIDERMON_SPIDER_CLOSE_MONITORS = ("hh.monitors.monitors.SpiderCloseMonitorSuite",)
