from scrapy.http import Request


def make(self, **kwargs) -> Request:
    request = Request(
        url=kwargs['vacancy']['employer']['url'],
        callback=self.parse_employer,
        cb_kwargs=kwargs,
        dont_filter=True,
    )
    return request
