from scrapy.item import Field, Item


class Vacancy(Item):
    scraping_at = Field()
    published_at = Field()
    search_by = Field()
    vacancy_id = Field()
    vacancy_url = Field()
    vacancy_name = Field()
    vacancy_region = Field()
    vacancy_address = Field()
    vacancy_skills = Field()
    vacancy_salary = Field()
    vacancy_description = Field()
    contacts_name = Field()
    contacts_email = Field()
    contacts_phones = Field()
    company_name = Field()
    company_region = Field()
    company_hh_url = Field()
    company_site = Field()
    company_industries = Field()
