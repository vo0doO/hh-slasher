from urllib.parse import urlencode

from scrapy.http import Request


def make_search_by_text(self, country, text, page=0) -> Request:
    data = dict(country=country, text=text)
    api_path = "vacancies"
    params = {
        "text": text,
        "area": country["areas"],
        "page": page,
        "per_page": 100,
        "search_field": [
            "name",
            # "description",
            # "company_name",
        ],
        "premium": True,
        "order_by": "publication_time",
        "describe_arguments": True,
        "host": country["host"],
    }

    url = f"{self.api_url}/{api_path}?{urlencode(params, doseq=True)}"
    request = Request(url=url, callback=self.parse_vacancies_listing, cb_kwargs=data)
    return request
