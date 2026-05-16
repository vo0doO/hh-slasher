from typing import Generator

import scrapy
from scrapy.http import HtmlResponse

from hh.utils import (
    cached_vacancies,
    employer_request,
    item,
    vacancies_request,
    vacancy_request,
)


class Spider(scrapy.Spider):
    name = "hh"
    handle_httpstatus_list = (200,)

    api_url = "https://api.hh.ru"

    seen_vacancies = set[str]()
    cached_vacancies = set[str]()

    # Параметры поиска
    countries = {
        "Россия": {
            "host": "hh.ru",
            "areas": [113],
        },
        # "Беларусь": {
        #     "host": "rabota.by",
        #     "areas": [16],
        # },
        # "Азербайджан": {
        #     "host": "hh1.az",
        #     "areas": [9],
        # },
        # "Узбекистан": {
        #     "host": "hh.uz",
        #     "areas": [97],
        # },
        # "Казахстан": {
        #     "host": "hh.kz",
        #     "areas": [40],
        # },
        # "Грузия": {
        #     "host": "headhunter.ge",
        #     "areas": [28],
        # },
        # "Кыргызстан": {
        #     "host": "headhunter.kg",
        #     "areas": [48],
        # },
    }

    def __init__(
        self,
        search_texts=None,
        excluded_text=None,
        search_field=[
            "name",
            "description",
            # "company_name",
        ],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if search_field is not None:
            if not isinstance(search_field, list):
                self.search_field = self._parse_list(search_field)
            else:
                self.search_field = search_field
        if search_texts is not None:
            self.search_texts = self._parse_list(search_texts)
        if excluded_text is not None:
            self.excluded_text = self._parse_list(excluded_text)

    @staticmethod
    def _parse_list(value) -> list[str]:
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return list(value)

    def start_requests(self) -> Generator:
        # self.cached_vacancies = cached_vacancies.get(self)

        for country in self.countries.values():
            for text in self.search_texts:
                yield vacancies_request.make_search_by_text(
                    self, country, text, excluded_text=self.excluded_text
                )

    def parse_vacancies_listing(self, response: HtmlResponse, **kwargs) -> Generator:
        data: dict = response.json()  # type: ignore
        vacancies = data["items"]  # type: ignore

        # Работа с вакансиями
        for vacancy in vacancies:
            # # Обработка кеша
            # if vacancy["id"] in self.cached_vacancies:
            #     self.crawler.stats.inc_value("duplicated/vanacy/cached")  # type: ignore # noqa
            #     continue

            # Обработка дубликатов
            if vacancy["id"] in self.seen_vacancies:
                self.crawler.stats.inc_value("duplicated/vanacy/seen")  # type: ignore # noqa
                continue
            self.seen_vacancies.add(vacancy["id"])

            # Запросы карточек вакансий
            yield vacancy_request.make(
                self, vacancy_of_listing=vacancy, text=kwargs["text"]
            )

        # Пагинация
        if data["page"] < data["pages"] - 1:
            yield vacancies_request.make_search_by_text(
                self,
                kwargs["country"],
                kwargs["text"],
                data["page"] + 1,
                kwargs['excluded_text'],
            )

    def parse_vacancy(self, response: HtmlResponse, **kwargs) -> Generator:
        kwargs["vacancy"] = response.json()

        # Багнутый работодатель(без карточки)
        if "id" not in kwargs["vacancy"]["employer"]:  # type: ignore
            self.crawler.stats.inc_value("Без карточки работодателя")  # type: ignore
            loader = item.make_loader(**kwargs)
            loader.load_item()
            yield loader.item
        # Нормальный работодатель
        else:
            yield employer_request.make(self, **kwargs)

    def parse_employer(self, response: HtmlResponse, **kwargs) -> Generator:
        kwargs["employer"] = response.json()
        loader = item.make_loader(**kwargs)
        loader.load_item()
        yield loader.item
