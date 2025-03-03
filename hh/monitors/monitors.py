from spidermon import Monitor, MonitorSuite, monitors
from spidermon.contrib.actions.telegram.notifiers import (
    SendTelegramMessageSpiderFinished,
)


class TelegramMessageSpiderFinished(SendTelegramMessageSpiderFinished):
    include_ok_messages = True


@monitors.name("Загрузчик")
class ResponseStatusCountMonitor(Monitor):
    min_response_status_200 = 1

    @monitors.name(f"Ответов со статусом 200 > {min_response_status_200}")
    def test_response_status_200(self):
        status_200_count = getattr(
            self.data.stats, "downloader/response_status_count/200", 0
        )
        return self.assertGreater(status_200_count, self.min_response_status_200)


@monitors.name("Ошибки HTTP")
class HttpErrorMonitor(Monitor):
    max_response_ignored_count = 1

    @monitors.name(f"Проигнорировано ответов < {max_response_ignored_count}")
    def test_response_ignored(self):
        response_ignored_count = getattr(
            self.data.stats, "httperror/response_ignored_count", 0
        )
        response_ignore_by_404_count = getattr(
            self.data.stats, "httperror/response_ignored_status_count/404", 0
        )
        response_ignored_count -= response_ignore_by_404_count
        return self.assertLess(response_ignored_count, self.max_response_ignored_count)


class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [ResponseStatusCountMonitor, HttpErrorMonitor]
    monitors_finished_actions = [TelegramMessageSpiderFinished]
