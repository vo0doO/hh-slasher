import datetime

import pytz
from itemloaders import ItemLoader

from hh.itemloader import VacancyLoader


def get_now():
    tz = pytz.timezone('Europe/Moscow')
    now = datetime.datetime.now(tz=tz).today()
    return now


def make_loader(**kwargs) -> ItemLoader:
    loader = VacancyLoader()
    loader.add_value("scraping_at", get_now())
    loader.add_value("published_at", kwargs)
    loader.add_value("search_by", kwargs)
    loader.add_value("vacancy_id", kwargs)
    loader.add_value("vacancy_url", kwargs)
    loader.add_value("vacancy_name", kwargs)
    loader.add_value("vacancy_region", kwargs)
    loader.add_value("vacancy_address", kwargs)
    loader.add_value("vacancy_skills", kwargs)
    loader.add_value("vacancy_salary", kwargs)
    loader.add_value("vacancy_description", kwargs)
    loader.add_value("contacts_name", kwargs)
    loader.add_value("contacts_email", kwargs)
    loader.add_value("contacts_phones", kwargs)
    loader.add_value("company_name", kwargs)
    loader.add_value("company_region", kwargs)
    loader.add_value("company_hh_url", kwargs)
    loader.add_value("company_site", kwargs)
    loader.add_value("company_industries", kwargs)
    return loader
