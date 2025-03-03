from urllib.parse import urlencode

from scrapy.http import Request


def make(self, **kwargs) -> Request:
    request = Request(
        url=kwargs['vacancy_of_listing']['url'],
        callback=self.parse_vacancy,
        cb_kwargs=kwargs,
    )
    return request
