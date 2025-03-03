import datetime
import unicodedata

from itemloaders import ItemLoader
from itemloaders.processors import Join, MapCompose, SelectJmes, TakeFirst
from w3lib.html import remove_tags, replace_entities, unquote_markup

from hh.items import Vacancy


def extract_date(x: str) -> str:
    date = datetime.datetime.fromisoformat(x)
    return date.strftime("%Y-%m-%d")


def clean_unicode_string_from_symbols_category(x: str) -> str:
    return ''.join(c for c in x if unicodedata.category(c)[0] != 'C')


def extract_vacancy_address(address: dict) -> str | None:
    if not address:
        return None
    address_parts = []
    if city := address.get("city"):
        address_parts.append(city)
    if metro_stations := address.get("metro_stations"):
        for metro_station in metro_stations:
            address_parts.append(metro_station["station_name"])
    if street := address.get("street"):
        address_parts.append(street)
    if building := address.get("building"):
        address_parts.append(building)
    return ", ".join(address_parts)


def extract_phone(phone: dict) -> str | None:
    if not phone:
        return None
    formatted_phone = phone.get('formatted')
    if comment := phone.get("comment"):
        formatted_phone = f"{comment}[{formatted_phone}]"
    return formatted_phone


class VacancyLoader(ItemLoader):
    default_output_processor = TakeFirst()
    default_item_class = Vacancy

    scraping_at_in = MapCompose(str, extract_date)
    published_at_in = MapCompose(SelectJmes("vacancy.published_at"), extract_date)
    search_by_in = MapCompose(SelectJmes("text"))
    vacancy_id_in = MapCompose(SelectJmes("vacancy.id"))
    vacancy_url_in = MapCompose(SelectJmes("vacancy.alternate_url"))
    vacancy_name_in = MapCompose(SelectJmes("vacancy.name"))
    vacancy_region_in = MapCompose(SelectJmes("vacancy.area.name"))
    vacancy_address_in = MapCompose(
        SelectJmes("vacancy.address"),
        extract_vacancy_address,
    )
    vacancy_skills_in = MapCompose(
        SelectJmes("vacancy.key_skills[].name"),
    )
    vacancy_skills_out = Join(", ")
    vacancy_salary_in = MapCompose(
        SelectJmes("[vacancy.salary.from, vacancy.salary.to]"),
    )
    vacancy_skills_out = Join("-")
    vacancy_description_in = MapCompose(
        SelectJmes("vacancy.description"),
        remove_tags,
        replace_entities,
        unquote_markup,
        clean_unicode_string_from_symbols_category,
        str.strip,
    )
    contacts_name_in = MapCompose(SelectJmes("vacancy_of_listing.contacts.name"))
    contacts_email_in = MapCompose(SelectJmes("vacancy_of_listing.contacts.email"))
    contacts_phones_in = MapCompose(
        SelectJmes("vacancy_of_listing.contacts.phones"), extract_phone
    )
    contacts_phones_out = Join(", ")
    company_name_in = MapCompose(SelectJmes("vacancy.employer.name"))
    company_region_in = MapCompose(SelectJmes("employer.area.name"))
    company_hh_url_in = MapCompose(SelectJmes("vacancy.employer.alternate_url"))
    company_site_in = MapCompose(SelectJmes("employer.site_url"))
    company_industries_in = MapCompose(
        SelectJmes("employer.industries"),
        SelectJmes("name"),
    )
    company_industries_out = Join(", ")
