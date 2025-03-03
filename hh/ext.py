import os

from scrapy.exporters import CsvItemExporter
from scrapy.extensions.httpcache import DummyPolicy, FilesystemCacheStorage
from scrapy.utils.request import fingerprint


class TabbedCsvItemExporter(CsvItemExporter):
    def __init__(self, *args, **kwargs):
        kwargs = {**kwargs, 'delimiter': '\t', 'encoding': 'utf8'}
        super().__init__(*args, **kwargs)


class OneMoreSubDirFilesystemCacheStorage(FilesystemCacheStorage):
    def _get_request_path(self, spider, request):
        key = fingerprint(request).hex()
        return os.path.join(
            self.cachedir,
            # add extra sub dir with name as project name has
            spider.settings['BOT_NAME'],
            spider.name,
            key[0:2],
            key,
        )


class WhitelistCodesCachePolicy(DummyPolicy):
    def __init__(self, settings):
        super().__init__(settings)
        self.allow_http_codes = [
            int(x) for x in settings.getlist('HTTPCACHE_ALLOW_HTTP_CODES')
        ]

    def should_cache_response(self, response, request):
        return response.status in self.allow_http_codes
